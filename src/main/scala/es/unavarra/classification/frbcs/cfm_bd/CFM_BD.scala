/*
 * Copyright (C) 2017 Mikel Elkano Ilintxeta and Mikel Galar Idoate
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package es.unavarra.classification.frbcs.cfm_bd

import java.io.{ObjectOutputStream, PrintWriter}

import es.unavarra.classification.frbcs.cfm_bd.kb._
import es.unavarra.classification.frbcs.cfm_bd.optimization._
import es.unavarra.classification.frbcs.cfm_bd.preprocessing.Scaler
import es.unavarra.classification.frbcs.cfm_bd.utils.ComparableByteArray
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashSet

object CFM_BD {

  def learn(paramsBroadcast: Broadcast[Parameters], dbBroadcast: Broadcast[DataBase], sparkContext: SparkContext, inputFile: RDD[String]): RuleBase = {

    println("\n-------------------------------\nBuilding the rule base...\n-------------------------------\n")
    val parameters = paramsBroadcast.value
    val dataBase = dbBroadcast.value
    val nExamples = inputFile.count()
    val tInit = System.currentTimeMillis()

    /*******************************************************************************************************************
    * CANDIDATE RULES GENERATION
    *******************************************************************************************************************/

    print("Extracting candidate itemsets...  ")
    val tCandidates = System.currentTimeMillis()

    // Create crisp partitions defined by the fuzzy sets with the highest membership degree for each example
    val crispPartitions = inputFile.map(example => createCrispPartition(paramsBroadcast.value, dbBroadcast.value, example)).cache()

    // Create all the candidate rules covering at least one example and compute their coverage
    val candidateRules = crispPartitions
      .reduceByKey((m1, m2) => m1 ++ m2.map {case (k, v) => k -> (v + m1.getOrElse(k, 0f))})
      .flatMap(m => createSubRules(paramsBroadcast.value.maxAtt, m._1.bytes, m._2))
      .reduceByKey((m1, m2) => (m1._1, m1._2 ++ m2._2.map {case (k, v) => k -> (v + m1._2.getOrElse(k, 0f))}))
      .map{case(id, (r, coverage)) => new CandidateFuzzyRule(id, r._1 zip r._2, coverage)}.cache()
    val totalCoverage = crispPartitions.map(p => p._2)
      .reduce((m1, m2) => m1 ++ m2.map{case (k, v) => k -> (v + m1.getOrElse(k, 0f))}).map(m => m._2).sum
    candidateRules.foreach(r => r.computeSupport(totalCoverage))
    crispPartitions.unpersist()
    println("Done (" + candidateRules.count() + " candidate itemsets).")

    // Get frequent itemsets based on the minimum support (the coverage considers class frequencies)
    print("Searching for frequent itemsets...  ")
    val frequentRules = candidateRules.filter(r => r.support > 0.025d / (dbBroadcast.value.classLabels.length * r.antecedents.length)).cache()
    println("Done (" + frequentRules.count() + " frequent itemsets).")
    candidateRules.unpersist()

    // Take the top 50% rules (at least)
    print("Selecting the most promising itemsets...  ")
    var topRules: RDD[CandidateFuzzyRule] = null
    var attIdx = 0
    if (parameters.costSensitive) {
      val confThreshold: Array[Array[Float]] = Array.ofDim[Float](dataBase.classLabels.length, parameters.maxAtt)
      var classIdx = 0
      while (classIdx < dataBase.classLabels.length) {
        attIdx = 0
        while (attIdx < parameters.maxAtt) {
          val classConfs = frequentRules.filter(r => r.mostCoveredClass == classIdx && r.antecedents.length == attIdx + 1)
            .map(r => r.confidences(r.mostCoveredClass)).sortBy(conf => conf, false).zipWithIndex().cache()
          val numConfs = classConfs.count()
          val medianIdx = Math.ceil(0.5 * numConfs).toLong // Median's index
          confThreshold(classIdx)(attIdx) = classConfs.filter(f => f._2 == medianIdx).first()._1.min(parameters.minCrispConfidence) // Minimum value between median's value and maxThreshold
          classConfs.unpersist()
          attIdx += 1
        }
        classIdx += 1
      }
      topRules = frequentRules.filter(r =>
        r.confidences(r.mostCoveredClass) >= confThreshold(r.mostCoveredClass)(r.antecedents.length-1)
      ).cache()
    }
    else {
      val confThreshold: Array[Float] = Array.ofDim[Float](parameters.maxAtt)
      while (attIdx < parameters.maxAtt) {
        val attConfs = frequentRules.filter(r => r.antecedents.length == attIdx + 1)
          .map(r => r.confidences(r.mostCoveredClass)).sortBy(conf => conf, false).zipWithIndex().cache()
        val numConfs = attConfs.count()
        val medianIdx = Math.ceil(0.5 * numConfs).toLong // Median's index
        confThreshold(attIdx) = attConfs.filter(f => f._2 == medianIdx).first()._1.min(parameters.minCrispConfidence) // Minimum value between median's value and maxThreshold
        attConfs.unpersist()
        attIdx += 1
      }
      topRules = frequentRules.filter(r =>
        r.confidences(r.mostCoveredClass) >= confThreshold(r.antecedents.length-1)
      ).cache()
    }

    // Candidate rules pruning
    val rulesOverlap = topRules.flatMap(r => {
      val itemsets: Array[((Int, Byte), Set[CandidateFuzzyRule])]
      = new Array[((Int, Byte), Set[CandidateFuzzyRule])](r.antecedents.length)
      var i = 0
      while (i < r.antecedents.length) {
        itemsets(i) = ((r.variables(i), r.antecedents(i)), Set(r))
        i += 1
      }
      itemsets
    }).reduceByKey((r1, r2) => r1 ++ r2)
    /******************************************************************************************************************/
    // OLD PRUNING
    /*val rulesToRemove: Broadcast[HashSet[Int]] = sparkContext.broadcast(
      HashSet() ++ rulesOverlap.flatMap{case (item, rules) => {
        var minRule: CandidateFuzzyRule = null
        var currRules: Set[CandidateFuzzyRule] = null
        var idsToRemove: HashSet[Int] = HashSet()
        var remove: Boolean = true
        while (remove) {
          remove = false
          currRules = rules.filter(r => !idsToRemove.contains(r.id))
          if (currRules.size > 0) {
            minRule = currRules.minBy(_.antecedents.length)
            currRules.foreach(r =>
              if (r.antecedents.length > minRule.antecedents.length
                && r.confidences(r.mostCoveredClass) <= minRule.confidences(minRule.mostCoveredClass)) {
                  remove = true
                  idsToRemove += r.id
              }
            )
          }
        }
        idsToRemove.toIterator
      }}.distinct().collect()
    )*/
    /******************************************************************************************************************/
    val rulesToRemove: Broadcast[HashSet[Int]] = sparkContext.broadcast(
      HashSet() ++ rulesOverlap.flatMap{case (item, rules) => {
        var minRule: CandidateFuzzyRule = null
        var currRules: Set[CandidateFuzzyRule] = null
        var idsToRemove: HashSet[Int] = HashSet()
        var remove: Boolean = true
        while (remove) {
          remove = false
          currRules = rules.filter(r => !idsToRemove.contains(r.id))
          if (currRules.size > 0) {
            minRule = currRules.minBy(_.antecedents.length)
            currRules.foreach(r => {
              var i = 0
              var subsetPresent = true
              while (i < minRule.antecedents.length && subsetPresent) {
                if (!r.variables.contains(minRule.variables(i))
                  || minRule.antecedents(i) != r.antecedents(r.variables.indexOf(minRule.variables(i))))
                  subsetPresent = false
                i += 1
              }
              if (subsetPresent && r.antecedents.length > minRule.antecedents.length
                && r.confidences(r.mostCoveredClass) <= minRule.confidences(minRule.mostCoveredClass)) {
                remove = true
                idsToRemove += r.id
              }
            })
          }
        }
        idsToRemove.toIterator
      }}.distinct().collect()
    )
    val topPrunedRules = topRules.filter(r => !rulesToRemove.value.contains(r.id)).collect().toList
    topRules.unpersist()
    frequentRules.unpersist()
    println("Done (" + topPrunedRules.size +" promising itemsets).")

    /*******************************************************************************************************************
    * COMPUTE RULE WEIGHTS, RESOLVE CONFLICTS, AND REMOVE REDUNDANT RULES
    *******************************************************************************************************************/

    val tWeights = System.currentTimeMillis()
    print("Computing rule weights and resolving conflicts...  ")

    // Generate candidate rule base with the candidate fuzzy rules
    val rbCandidate: CandidateRuleBase = new CandidateRuleBase(dataBase, topPrunedRules)
    val rbCandidateBroadcast = sparkContext.broadcast(rbCandidate)

    // Compute the matching of each example with the rule and reduce them using the sum (for the same rules and class = key)
    val matchPrecomp = inputFile
      .mapPartitions(examples => rbCandidateBroadcast.value.computeMatchingPartition(examples).iterator)
      .reduceByKey(
        (m1, m2) => for(classIndex  <- 0 until dbBroadcast.value.classLabels.length)
          yield (classIndex.toByte, m1(classIndex)._2 + m2(classIndex)._2)
      )
      .map{case (id, matchings) => (id, matchings.toMap)}

    /** REMOVE THIS: only for experiments **/
    /******************************************************************************************************************/
    // val datasetName = parameters.outputPath.split("/").last.split("-")(0)
    // Save candidate rule base and pre-computed matching degrees to disk
    /*matchPrecomp.cache.saveAsObjectFile("/experiments/CFM_BD/output/matchings/"+datasetName+"/"+datasetName+"-5-1/Matchings")
    rbCandidate.save("/experiments/CFM_BD/output/matchings/"+datasetName+"/"+datasetName+"-5-1/CandidateRB")*/
    // Load candidate rule base and pre-computed matching degrees from disk
    /*val matchPrecomp: RDD[(Int,Map[Byte,Float])] = sparkContext.objectFile(
      "/experiments/CFM_BD/output/matchings/"+datasetName+"/"+datasetName+"-5-1/Matchings", parameters.numPartitions
    ).cache()
    val rbCandidate = new CandidateRuleBase("/experiments/CFM_BD/output/matchings/"+datasetName+"/"+datasetName+"-5-1/CandidateRB")
    val rbCandidateBroadcast = sparkContext.broadcast(rbCandidate)*/
    /******************************************************************************************************************/

    // Compute the weight/support/confidence of each rule
    val weightSuppConf = matchPrecomp.map{case (id, matchings) => (id, rbCandidateBroadcast.value.rules(id).coverage.keySet, matchings)}
      .map {case (id, candidates, matchings) => (id,
        paramsBroadcast.value.weigthMethod match{
          case parameters.WEIGHT_PCF => candidates.map(c => (c, (matchings(c) - matchings.filterKeys(c != _).values.sum) / matchings.values.sum))
          case parameters.WEIGHT_CF => candidates.map(c => (c, matchings(c) / matchings.values.sum))
          case other =>
            System.err.println("\nERROR: Please, choose between Penalized Certainty Factor and Certainty Factor\n");
            println("\nERROR: Please, choose between Penalized Certainty Factor and Certainty Factor\n");
            System.exit(-1);
            null
        }, // weight
        candidates.map(c => (c, (matchings(c)) / nExamples.toFloat)), // support -> SHOULD BE: matchings.values.sum / nExamples.toFloat, instead of a Map
        candidates.map(c => (c, (matchings(c)) / matchings.values.sum)) // confidence
      )}.cache()
    matchPrecomp.unpersist()

    // Resolve conflicts by getting the class having the maximum weight
    val rulesNoConflicts = weightSuppConf.map {case (id, w, s, c) =>
      val wMax = w.maxBy(_._2)
      (id, wMax, (wMax._1, s.toMap.get(wMax._1).get), (wMax._1, c.toMap.get(wMax._1).get))
    }

    // Get the fuzzy rules having a minimum support/confidence (matching degrees are cost-sensitive)
    val fuzzyRules = rulesNoConflicts.filter {case (id, w, s, c) =>
      val rule = rbCandidateBroadcast.value.rules(id)
      val len = rule.antecedents.length
      ((w._2 > 0) && (s._2 >= 0.05 / (dbBroadcast.value.classLabels.length * len)) && (c._2 >= parameters.minFuzzyConfidence))
    }.map{case (id, w, s, c) => (id, w._1, w._2, c._2)}.cache()

    // Take top-rules
    val topFuzzyRules: ArrayBuffer[(Int, Byte, Float, Float)] = ArrayBuffer[(Int, Byte, Float, Float)]()
    attIdx = 0
    if (parameters.costSensitive) {
      var classIdx = 0
      while (classIdx < dataBase.classLabels.length) {
        attIdx = 0
        while (attIdx < parameters.maxAtt) {
          topFuzzyRules.appendAll(fuzzyRules.filter { case (id, ruleClass, weight, conf) =>
            ruleClass == classIdx && rbCandidateBroadcast.value.rules(id).antecedents.length == attIdx + 1
          }.sortBy(_._3, false).take(
            Math.ceil(2 * parameters.propInitRulesEvo * dataBase.numLinguisticLabels * dataBase.variables.length * parameters.propRuleLen(attIdx)).toInt
          ).toIterator)
          attIdx += 1
        }
        classIdx += 1
      }
    }
    else
      while (attIdx < parameters.maxAtt) {
        topFuzzyRules.appendAll(fuzzyRules.filter{case (id, ruleClass, weight, conf) =>
          rbCandidateBroadcast.value.rules(id).antecedents.length == attIdx + 1
        }.sortBy(_._3, false).take(
          Math.ceil(dataBase.classLabels.length * 2 * parameters.propInitRulesEvo * dataBase.numLinguisticLabels * dataBase.variables.length * parameters.propRuleLen(attIdx)).toInt
        ).toIterator)
        attIdx += 1
      }
    /******************************************************************************************************************/
    // OLD PRUNING
    /*val distTopFuzzyRules = sparkContext.parallelize(topFuzzyRules, parameters.numPartitions).cache()
    val fuzzyRulesOverlap = distTopFuzzyRules.flatMap{case (id, classIdx, weight, conf) => {
      val r = rbCandidateBroadcast.value.rules(id)
      val itemsets: Array[((Int, Byte), Set[(Int, Int, Double)])]
      = new Array[((Int, Byte), Set[(Int, Int, Double)])](r.antecedents.length)
      var i = 0
      while (i < r.antecedents.length) {
        itemsets(i) = ((r.variables(i), r.antecedents(i)), Set((id, r.antecedents.length, conf)))
        i += 1
      }
      itemsets
    }}.reduceByKey((r1, r2) => r1 ++ r2)
    val fuzzyRulesToRemove: Broadcast[HashSet[Int]] = sparkContext.broadcast(
      HashSet() ++ fuzzyRulesOverlap.flatMap{case (item, rules) => {
        var minRule: (Int, Int, Double) = null
        var minRuleLen: Int = 0
        var minRuleConf: Double = 1d
        var currRules: Set[(Int, Int, Double)] = null
        var idsToRemove: HashSet[Int] = HashSet()
        var remove: Boolean = true
        while (remove) {
          remove = false
          currRules = rules.filter{case (id, len, conf) => !idsToRemove.contains(id)}
          if (currRules.size > 0) {
            minRule = currRules.minBy(_._2)
            minRuleLen = minRule._2
            minRuleConf = minRule._3
            currRules.foreach{case (id, len, conf) =>
              if (len > minRuleLen && conf <= minRuleConf) {
                remove = true
                idsToRemove += id
              }
            }
          }
        }
        idsToRemove.toIterator
      }}.distinct().collect()
    )*/
    /******************************************************************************************************************/
    // Fuzzy rules pruning
    val distTopFuzzyRules = sparkContext.parallelize(topFuzzyRules, parameters.numPartitions).cache()
    val fuzzyRulesOverlap = distTopFuzzyRules.flatMap{case (id, classIdx, weight, conf) => {
      val r = rbCandidateBroadcast.value.rules(id)
      val itemsets: Array[((Int, Byte), Set[(FuzzyRule, Double)])]
      = new Array[((Int, Byte), Set[(FuzzyRule, Double)])](r.antecedents.length)
      var i = 0
      while (i < r.antecedents.length) {
        itemsets(i) = ((r.variables(i), r.antecedents(i)), Set((r, conf)))
        i += 1
      }
      itemsets
    }}.reduceByKey((r1, r2) => r1 ++ r2)
    val fuzzyRulesToRemove: Broadcast[HashSet[Int]] = sparkContext.broadcast(
      HashSet() ++ fuzzyRulesOverlap.flatMap{case (item, rules) => {
        var minRulePair: (FuzzyRule, Double) = null
        var minRule: FuzzyRule = null
        var minRuleConf: Double = 1d
        var currRules: Set[(FuzzyRule, Double)] = null
        var idsToRemove: HashSet[Int] = HashSet()
        var remove: Boolean = true
        while (remove) {
          remove = false
          currRules = rules.filter{case (r, conf) => !idsToRemove.contains(r.id)}
          if (currRules.size > 0) {
            minRulePair = currRules.minBy(_._1.antecedents.length)
            minRule = minRulePair._1
            minRuleConf = minRulePair._2
            currRules.foreach{case (r, conf) =>
              var i = 0
              var subsetPresent = true
              while (i < minRule.antecedents.length && subsetPresent) {
                if (!r.variables.contains(minRule.variables(i))
                  || minRule.antecedents(i) != r.antecedents(r.variables.indexOf(minRule.variables(i))))
                  subsetPresent = false
                i += 1
              }
              if (subsetPresent && r.antecedents.length > minRule.antecedents.length && conf <= minRuleConf) {
                remove = true
                idsToRemove += r.id
              }
            }
          }
        }
        idsToRemove.toIterator
      }}.distinct().collect()
    )
    val fuzzyRulesPruned = distTopFuzzyRules.filter{case (id, classIdx, weight, conf) =>
      !fuzzyRulesToRemove.value.contains(id)
    }

    /*******************************************************************************************************************
    * GENERATE THE FINAL RULE BASE
    *******************************************************************************************************************/

    // Create fuzzy rules
    val finalRules = fuzzyRulesPruned.collect().map{case (id, classIdx, weight, conf) =>
      new FuzzyRule(id, rbCandidate.rules(id).getAntecedentsLabels(), classIdx, weight)
    }
    fuzzyRules.unpersist()
    distTopFuzzyRules.unpersist()
    println("Done (" + finalRules.size + " rules).")
    weightSuppConf.unpersist()

    // Create the rule base
    var rbFinal: RuleBase = parameters.frm match {
      case parameters.FRM_WINNING_RULE => new RuleBaseWinningRule(dbBroadcast, finalRules, sparkContext)
      case parameters.FRM_ADDITIVE_COMBINATION => new RuleBaseAdditiveCombination(dbBroadcast, finalRules, sparkContext)
      case other =>
        System.err.println("\nERROR: Please, choose between Winning Rule and Additive Combination\n");
        println("\nERROR: Please, choose between Winning Rule and Additive Combination\n");
        System.exit(-1);
        null
    }

    /*******************************************************************************************************************
    * OPTIMIZE THE RULE BASE WITH CHC
    *******************************************************************************************************************/

    val tOptimize = System.currentTimeMillis()
    if (parameters.evolutionary) {
      println("\n-------------------------------\nOptimizing the rule base...\n-------------------------------\n")
      // Rules selection
      // val matchings = inputFile.mapPartitions(examples => rbFinal.computeMatchingExamples(examples).iterator).cache()
      // inputFile.unpersist() // Remove training set from memory and read it again to save memory. Sometimes throws RPC Timeout Exception
      /*val matchings = inputFile.map(example => rbFinal.computeMatchingExample(example)) // Pre-compute matching degrees
        .persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)*/ // Serialized cache to save memory
      var matchings = inputFile.map(example => rbFinal.computeMatchingExample(example)).cache() // Pre-compute matching degrees
      rbFinal = GlobalCHCRulesSelection.optimize(paramsBroadcast, sparkContext.broadcast(rbFinal), matchings, sparkContext)
      // 2-tuples
      // rbFinal = GlobalCHC2Tuples.optimize(parameters, rbFinal, sparkContext, inputFile, fitnessMeasure="geometric mean")
    }

    /*******************************************************************************************************************
    * OUTPUT
    *******************************************************************************************************************/

    val tFinal = System.currentTimeMillis()

    // Store data base and rule base
    rbFinal.save(parameters.outputPath + "/RB", true)
    val oos = new ObjectOutputStream(FileSystem.get(Parameters.getHadoopConf()).create(new Path(parameters.outputPath + "/DB"),true))
    oos.writeObject(dataBase.variables)
    oos.close()

    // Write execution time
    val titles = Array("Total execution time", "Candidate rules generation", "Rule weights computation", "Optimization")
    val times = Array(tFinal-tInit, tWeights-tCandidates, tOptimize-tWeights, tFinal-tOptimize)
    print("\n-------------------------------\nLearning process finished\n-------------------------------\n"
      +writeElapsedTime(parameters, times, titles, parameters.outputPath+"Time.txt"))

    // Store rule base stats
    val pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
      new Path(parameters.outputPath + "/RB_stats.txt"), true).getWrappedStream
    )
    // Totals
    var avgAtt: Float = 0f
    rbFinal.rules.foreach(r => avgAtt += r.antecedents.length)
    avgAtt /= rbFinal.rules.length
    println ("Final rule base size: "+rbFinal.rules.length)
    pw.write ("Final rule base size: "+rbFinal.rules.length+"\n")
    println("Average antecedents per rule: "+avgAtt)
    pw.write("Average antecedents per rule: "+avgAtt+"\n")
    // By class
    val classRules = Array.fill[(Integer, Array[FuzzyRule])](dataBase.classLabels.length)(null)
    var i: Int = 0
    while (i < dataBase.classLabels.length){
      classRules(i) = (i, rbFinal.rules.filter(f => f.classIndex==i))
      i += 1
    }
    classRules.foreach(c => {
      println("Class "+dataBase.classLabels(c._1)+" stats:")
      pw.println("Class "+dataBase.classLabels(c._1)+" stats:")
      println ("\tNumber of rules: "+classRules(c._1)._2.size)
      pw.println("\tNumber of rules: "+classRules(c._1)._2.size)
      println ("\tAvg. antecedents: "+(classRules(c._1)._2.map(f => f.antecedents.length).sum.toFloat / classRules(c._1)._2.size))
      pw.println("\tAvg. antecedents: "+(classRules(c._1)._2.map(f => f.antecedents.length).sum.toFloat / classRules(c._1)._2.size))
      println ("\tAvg. weight: "+(classRules(c._1)._2.map(f => f.weight).sum / classRules(c._1)._2.size))
      pw.println("\tAvg. weight: "+(classRules(c._1)._2.map(f => f.weight).sum / classRules(c._1)._2.size))
    })
    pw.close()

    // Return rule base
    rbFinal

  }

  def adjustFuzzySets(parameters: Parameters, dataBase: DataBase, inputFile: RDD[String]): Unit = {
    // For each fuzzy set, get the mean of the examples having a positive membership degree
    val stats = inputFile.mapPartitions(s => (fuzzySetStats(parameters, dataBase, s)))
      .reduce((x,y) => {
        var sum, count: Array[Array[Float]] = Array.fill(dataBase.variables.length){
          Array.fill(parameters.numLinguisticLabels){0f}}
        var i, j: Int = 0
        i = 0
        while (i < dataBase.variables.length){
          if (dataBase.variables(i).isInstanceOf[FuzzyVariable]){
            j = 0
            while (j < parameters.numLinguisticLabels) {
              sum(i)(j) = x._1(i)(j) + y._1(i)(j)
              count(i)(j) = x._2(i)(j) + y._2(i)(j)
              j += 1
            }
          }
          i += 1
        }
        (sum, count)
      })
    // Center the fuzzy sets with respect to the mean
    var fuzzyVar: FuzzyVariable = null
    var i, j: Int = 0
    i = 0
    while (i < dataBase.variables.length){
      if (dataBase.variables(i).isInstanceOf[FuzzyVariable]){
        fuzzyVar = dataBase.variables(i).asInstanceOf[FuzzyVariable]
        j = 0
        while (j < parameters.numLinguisticLabels) {
          fuzzyVar.fuzzySets(j).center(stats._1(i)(j) / stats._2(i)(j))
          j += 1
        }
      }
      i += 1
    }
  }

  def classify(parameters: Parameters, ruleBase: RuleBase, sparkContext: SparkContext, inputFile: RDD[String]): Float = {
    println("\n-------------------------------\nClassifying testing set...\n-------------------------------\n")
    // Classify dataset
    val confusionMatrix = ruleBase.classifyDataset(sparkContext, inputFile)
    // Output
    val stats = computeAccuracyStats(ruleBase.dataBase, confusionMatrix)
    saveConfusionMatrix(parameters, confusionMatrix, stats)
    println("Accuracy rate = " + stats(0))
    println("Geometric mean = " + stats(1))
    println("Avg. accuracy per class = " + stats(2))
    stats(0) // Return accuracy rate
  }

  /**
    * Returns an array of tuples containing:
    * 1) hash of the sub-rule of length [1, maxLength] composed of a subset of the given antecedents
    * 2) given coverage
    * @param maxLength maximum sub-rule length
    * @param allAntecedents linguistic labels that defines the partition
    * @param coverage map containing, for each class, the number of examples in the partition defined by the antecedents
    * @return array of tuples containing the hash of all the sub-rules that can be generated with the given antecedents and the given coverage (same reference)
    */
  def createSubRules(maxLength: Int, allAntecedents: Array[Byte], coverage: Map[Byte, Float]): Array[(Int, ((Array[Int], Array[Byte]), Map[Byte, Float]))] = {
    var len = 1
    var varIdx = 0
    val allVarsList = for (i <- 0 until allAntecedents.length) yield i
    val allVars = Set(allVarsList: _*)
    var subsetAnts: Array[Byte] = null
    var subsetVars: Array[Int] = null
    var subsetsVars: Iterator[Set[Int]] = null
    var subPartition: (Int, ((Array[Int], Array[Byte]), Map[Byte, Float])) = null
    val subPartitions = new ArrayBuffer[(Int, ((Array[Int], Array[Byte]), Map[Byte, Float]))]()
    // Create subsets of all possible lengths
    while (len <= maxLength){
      subsetsVars = allVars.subsets(len)
      // Compute the hash of each subset
      while (subsetsVars.hasNext) {
        subsetVars = subsetsVars.next().toArray.sorted
        subsetAnts = Array.fill(len)(-1)
        varIdx = 0
        while (varIdx < subsetVars.length) {
          subsetAnts(varIdx) = allAntecedents(subsetVars(varIdx))
          varIdx += 1
        }
        subPartition = ((subsetVars zip subsetAnts).mkString("").hashCode, ((subsetVars, subsetAnts), coverage))
        subPartitions.append(subPartition)
      }
      len += 1
    }
    subPartitions.toArray
  }

  /**
    * Returns a tuple containing an array of candidate rules and a tuple of:
    * 1) the linguistic labels with the highest membership degree for this example
    * 2) map of (class index => class cost)
    * @param parameters parameters
    * @param db data base
    * @param instanceStr example's values
    * @return a tuple containing an array of candidate rules and the linguistic labels with the highest membership degree for this example
    */
  def createCrispPartition(parameters: Parameters, db: DataBase,
                           instanceStr: String): (ComparableByteArray, Map[Byte, Float]) = {
    // Get the example's values
    val splitInstance = instanceStr.split(",").map(_.trim)
    val (inputValues, classLabel) = splitInstance.splitAt(splitInstance.length - 1)
    val classIndex = db.getClassIndex(classLabel(0))
    // Get the linguistic labels with the highest membership degree for all values
    val allAnts: Array[Byte] = Array.fill(db.variables.length){-1}
    var i = 0
    while (i < allAnts.length){
      allAnts(i) = db.variables(i).getLabelIndex(inputValues(i))
      i += 1
    }
    (new ComparableByteArray(allAnts), Map((classIndex -> db.classCost(classIndex))))
  }

  // Returns two matrices: one containing (for each fuzzy set) the sum of the values with a positive membership
  // and the other storing (for each fuzzy set) the number of values with a positive membership.
  // i-th row represents i-th variable and j-th column represents j-th fuzzy set (linguistic label) of i-th variable
  def fuzzySetStats(parameters: Parameters, dataBase: DataBase, examples: Iterator[String]): Iterator[(Array[Array[Float]], Array[Array[Float]])] = {

    var sum, count: Array[Array[Float]] = Array.fill(dataBase.variables.length){Array.fill(parameters.numLinguisticLabels){0f}}
    var exampleValues: Array[String] = null
    var fuzzyVar: FuzzyVariable = null
    var numValue: Float = 0f
    var i, j: Int = 0

    while (examples.hasNext) {
      exampleValues = examples.next().split(",").map(_.trim)
      i = 0
      while (i < dataBase.variables.length) {
        if (dataBase.variables(i).isInstanceOf[FuzzyVariable]){
          fuzzyVar = dataBase.variables(i).asInstanceOf[FuzzyVariable]
          numValue = exampleValues(i).toFloat
          j = 0
          while (j < parameters.numLinguisticLabels) {
            if (fuzzyVar.positiveMembership(j, numValue)) {
              sum(i)(j) += numValue
              count(i)(j) += 1
            }
            j += 1
          }
        }
        i += 1
      }
    }

    List((sum, count)).iterator

  }

  def saveHistogram(histogram: (Array[Double], Array[Long]), filename: String): Unit = {
    var pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
      new Path(filename),true).getWrappedStream
    )
    val buckets: Array[Double] = histogram._1
    val counts: Array[Long] = histogram._2
    pw.write(buckets.mkString(",")+"\n")
    pw.write(counts.mkString(",")+"\n")
    pw.close()
  }

  /**
    * Returns an array of doubles containing the accuracy rate, geometric mean, and average accuracy per class
    * @return an array of doubles containing the accuracy rate, geometric mean, and average accuracy per class
    */
  def computeAccuracyStats(dataBase: DataBase, confusionMatrix: Array[Array[Long]]): Array[Float] = {
    val nClasses = dataBase.classLabels.length
    // Compute stats
    val hits: Array[Long] = Array.fill(nClasses){0}
    val nExamples: Array[Long] = Array.fill(nClasses){0}
    val TPrates: Array[Float] = Array.fill(nClasses){0}
    var gm: Float = 1
    var avgacc: Float = 0
    var i: Int = 0
    while (i < nClasses) {
      hits(i) = confusionMatrix(i)(i)
      nExamples(i) = confusionMatrix(i).sum
      TPrates(i) = hits(i).toFloat / nExamples(i).toFloat
      gm *= TPrates(i)
      avgacc += TPrates(i)
      i += 1
    }
    avgacc /= nClasses.toFloat
    gm = math.pow(gm, 1f / nClasses).toFloat
    val acc: Float = hits.sum.toFloat / nExamples.sum.toFloat
    Array(acc, gm, avgacc)
  }

  /**
    * Save the confusion matrix and accuracy stats to disk
    * @param parameters parameters
    * @param confusionMatrix confusion matrix
    * @return
    */
  def saveConfusionMatrix(parameters: Parameters, confusionMatrix: Array[Array[Long]], stats: Array[Float]): Unit = {
    var pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
      new Path(parameters.outputPath + "/ConfusionMatrix.txt"),true).getWrappedStream
    )
    pw.write(confusionMatrix.map(_.mkString("\t")).mkString("\n"))
    pw.close()
    // Save stats
    pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
      new Path(parameters.outputPath + "/Accuracy.txt"),true).getWrappedStream
    )
    pw.write("Accuracy rate = " + stats(0) + "\n")
    pw.write("Geometric mean = " + stats(1) + "\n")
    pw.write("Avg. accuracy per class = " + stats(2) + "\n")
    pw.close()
  }

  def writeElapsedTime(parameters: Parameters, elapsedTimes: Array[Long], titles: Array[String], fileName: String): String = {
    val pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
      new Path(fileName),true).getWrappedStream
    )
    var str = ""
    var i: Int = 0
    while (i < elapsedTimes.length) {
      val h = elapsedTimes(i) / 3600000;
      val m = (elapsedTimes(i) % 3600000) / 60000;
      val s = ((elapsedTimes(i) % 3600000) % 60000) / 1000;
      str += f"${titles(i)} (hh:mm:ss): $h%02d:$m%02d:$s%02d (${elapsedTimes(i) / 1000} seconds)\n"
      i += 1
    }
    pw.write(str)
    pw.close()
    str
  }

}
