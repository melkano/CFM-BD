/*
 * Copyright (C) 2017 Isaac Triguero, Mikel Elkano Ilintxeta and Mikel Galar Idoate
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

package es.unavarra.classification.frbcs.cfm_bd.optimization

import java.io.PrintWriter

import es.unavarra.classification.frbcs.cfm_bd.kb.RuleBase
import es.unavarra.classification.frbcs.cfm_bd.{CFM_BD, Parameters}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, HashSet}

/**
 * @author Isaac Triguero, Mikel Galar, and Mikel Elkano Ilintxeta
 * 
 * This class implements a distributed CHC algorithm that uses all the data during the fitness evaluation.
 * In this case, we use CHC to select the most accurate rules
 */

object GlobalCHCRulesSelection {

  def optimize(parameters: Broadcast[Parameters], ruleBase: Broadcast[RuleBase], matchings: RDD[Array[Float]],
               sparkContext: SparkContext): RuleBase = {
    
    // Initialize variables
    val params = parameters.value
    var restartsNoImprove: Int = 0
    var bestUntilRestart = 0f
    var eval: Long = 0
    val numChromosomes = params.popSize
    val numRules = ruleBase.value.rules.length
    val dInit = Math.ceil(numRules / 4.0).toLong;
    var d = dInit
    var strProgress, strOut: String = ""

    // Create initial population (first chromosome selects all rules and uses non-adjusted fuzzy sets)
    val t0 = System.currentTimeMillis()
    var chromosomes: Array[SparseChromosome] = Array.fill(params.popSize){ new SparseChromosome(parameters.value, numRules) }
    val initChromosome = for (i <- 0 until numRules) yield i
    chromosomes(0) = new SparseChromosome(parameters.value, numRules, HashSet(initChromosome:_*), 0f)

    // Evaluate initial population
    strOut += "Evaluating initial population...  "
    if (params.verbose)
      print("Evaluating initial population...  ")
    evaluateChromosomes(sparkContext, parameters, ruleBase, matchings, chromosomes, params.fitnessMeasure)
    eval += chromosomes.length

    // Sort the population by fitness
    chromosomes = chromosomes.sorted
    strOut += "Done. " + chromosomes(0) + "\n\n"
    if (params.verbose)
      println("Done. " + chromosomes(0) + "\n")

    // Optimization loop
    while ((eval < params.maxEval) && (restartsNoImprove < params.maxRestartsNoImprove) && chromosomes(0).fitness < 1) {

      // Create new population by combining previous chromosomes
      var newChromosomes = combine(chromosomes, d, params)
      if (newChromosomes.length > 0) {
        evaluateChromosomes(sparkContext, parameters, ruleBase, matchings, newChromosomes, params.fitnessMeasure)
        newChromosomes = newChromosomes.sorted
        eval += newChromosomes.length
      }
      val worstParentFitness = chromosomes.last.fitness
      if ((newChromosomes.length == 0) || (newChromosomes(0).fitness <= worstParentFitness))
        d -= Math.ceil(dInit * 0.01d).max(1.0d).toLong
      else
        chromosomes = (chromosomes ++ newChromosomes).sorted.take(numChromosomes)

      // Restart (if required)
      if (d <= 0) {
        strOut += "\nRestarting...  "
        if (params.verbose)
          print("\nRestarting...  ")
        chromosomes.tail.foreach(_.diverge(chromosomes(0))) // tail takes all items except the first one
        evaluateChromosomes(sparkContext, parameters, ruleBase, matchings, chromosomes.tail, params.fitnessMeasure)
        eval += chromosomes.tail.length
        d = dInit
        if (bestUntilRestart == chromosomes(0).fitness)
          restartsNoImprove += 1
        else {
          restartsNoImprove = 0
          bestUntilRestart = chromosomes(0).fitness
        }
        strOut += "Done.\n\n"
        if (params.verbose)
          println("Done.\n")
      }

      // Print progress
      if (!newChromosomes.isEmpty) {
        strProgress = "Eval: " + eval + " (new chromosomes: " + newChromosomes.length + ", added chromosomes: " +
          newChromosomes.filter(nc => nc.fitness > worstParentFitness).length +", restart threshold: " + d + "). " +
          chromosomes(0) + ". Time elapsed: " + ((System.currentTimeMillis - t0) / 1000) + "s"
        strOut += strProgress + "\n"
        if (params.verbose)
          println(strProgress)
      }

    }

    if (params.verbose)
      println("\n")
    if (eval >= params.maxEval) {
      println("Maximum number of evaluations (" + params.maxEval + ") reached")
      strOut += "\nMaximum number of evaluations (" + params.maxEval + ") reached"
    }
    else if (restartsNoImprove >= params.maxRestartsNoImprove){
      println("Maximum number of restarts (" + params.maxRestartsNoImprove + ") reached")
      strOut += "\nMaximum number of restarts (" + params.maxRestartsNoImprove + ") reached"
    }
    else {
      println("Maximum fitness reached")
      strOut += "\nMaximum fitness reached"
    }

    // Save progress to disk
    val pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
      new Path(params.outputPath + "/Evolution.txt"), true).getWrappedStream
    )
    pw.write(strOut)
    pw.close()

    // Decode the rule base encoded by the best performing chromosome
    ruleBase.value.rules = ruleBase.value.rules.filter(r => chromosomes(0).genes.contains(r.id))
    ruleBase.value.rules.zipWithIndex.foreach{case (r, index) => r.id = index}
    ruleBase.value.broadcastRules(sparkContext)
    ruleBase.value

  }

  // Classify all examples using only selected rules
  // Returns an array of ((real, predicted), count)
  def classifyWithSelectedRules(parameters: Broadcast[Parameters], ruleBase: Broadcast[RuleBase], matchings: RDD[Array[Float]],
                                selectedRulesBroad: Broadcast[Array[HashSet[Int]]], index: Int): Array[Array[Long]] = {
    val confMat = matchings.mapPartitions(examplesMatching => {
      val selectedRules = selectedRulesBroad.value(index)
      var i, j: Int = 0
      val confMat: HashMap[(Int, Int), Long] = new HashMap[(Int, Int), Long]()
      while (i < ruleBase.value.dataBase.variables.length) {
        j = 0
        while (j < ruleBase.value.dataBase.variables.length) {
          confMat((i, j)) = 0l
          j += 1
        }
        i += 1
      }
      val defaultClass = ruleBase.value.dataBase.classFrequency.zipWithIndex.maxBy(_._1)._2
      var rulesMatching: Array[Float] = null
      var real, predicted: Int = -1
      if (parameters.value.frm == parameters.value.FRM_WINNING_RULE) {
        var maxMatching = 0f
        while (examplesMatching.hasNext) {
          rulesMatching = examplesMatching.next()
          maxMatching = 0f
          predicted = defaultClass
          i = 0
          while (i < rulesMatching.length - 1) { // last item is the class label
            if (selectedRules.contains(i) && rulesMatching(i) > maxMatching) {
              predicted = ruleBase.value.rules(i).classIndex.toInt
              maxMatching = rulesMatching(i)
            }
            i += 1
          }
          real = rulesMatching.last.toInt
          confMat((real, predicted)) = confMat((real, predicted)) + 1l
        }
      }
      else if (parameters.value.frm == parameters.value.FRM_ADDITIVE_COMBINATION) {
        val classMatching: Array[Float] = Array.fill[Float](ruleBase.value.dataBase.variables.length)(0f)
        while (examplesMatching.hasNext) {
          rulesMatching = examplesMatching.next()
          predicted = defaultClass
          i = 0
          while (i < rulesMatching.length - 1) { // last item is the class label
            if (selectedRules.contains(i))
              classMatching(ruleBase.value.rules(i).classIndex.toInt) += rulesMatching(i)
            i += 1
          }
          val winner = classMatching.zipWithIndex.maxBy(_._1)
          if (winner._1 > 0)
            predicted = winner._2
          real = rulesMatching.last.toInt
          confMat((real, predicted)) = confMat((real, predicted)) + 1l
        }
      }
      else{
        System.err.println("\nERROR: Please, choose between Winning Rule and Additive Combination\n");
        println("\nERROR: Please, choose between Winning Rule and Additive Combination\n");
        System.exit(-1)
      }
      confMat.toIterator
    }).reduceByKey((c1, c2) => c1 + c2).collect()
    // Transform the confusion matrix into an array
    val confusionMatrix: Array[Array[Long]] = Array.fill(ruleBase.value.dataBase.variables.length){
      Array.fill(ruleBase.value.dataBase.variables.length){0}}
    confMat.foreach {
      case ((real, predicted), count) =>
        confusionMatrix(real)(predicted) = count;
    }
    confusionMatrix
  }

  def evaluateChromosomes(sparkContext: SparkContext, parameters: Broadcast[Parameters], ruleBase: Broadcast[RuleBase],
                          matchings: RDD[Array[Float]], chromosomes: Array[SparseChromosome], measure: String) = {
    // Broadcast selected rules
    val selectedRules: Array[HashSet[Int]] = Array.fill[HashSet[Int]](chromosomes.length)(null)
    var i: Int = 0;
    while(i < chromosomes.length){
      selectedRules(i) = chromosomes(i).genes
      i += 1
    }
    val selectedRulesBroad = sparkContext.broadcast(selectedRules)
    // Evaluate all chromosomes
    i = 0
    while(i < chromosomes.length){
      // Classify with the selected rules
      //print("Evaluating chromosome...  ")
      //val t0aux = System.currentTimeMillis()
      var confusionMatrix = classifyWithSelectedRules(parameters, ruleBase, matchings, selectedRulesBroad, i)
      //val t1aux = System.currentTimeMillis()
      //println("Done ("+((t1aux-t0aux)/1000)+" seconds)")
      val results = CFM_BD.computeAccuracyStats(ruleBase.value.dataBase, confusionMatrix)
      val accuracy = measure.toLowerCase match {
        case "geometric mean" | "geometric_mean" | "geometric-mean" | "gm" => results(1)
        case "average accuracy" | "average_accuracy" | "average-accuracy" | "avgacc" => results(2)
        case other => results(0)
      }
      // Update chromosome's fitness
      val numInitialRules = ruleBase.value.rules.length
      chromosomes(i).fitness = accuracy - (numInitialRules / (numInitialRules - chromosomes(i).genes.size + 1).toFloat) * 0.15f
      // Next chromosome
      i += 1
    }
  }

  /**
    * Recombine
    * Takes chromosomes and d
    * flatMap after sliding with step 2, compute hamming, if hamming recombine -> fuse maps and remove half of the coincidents elements from each map
    * First of all make shuffle of the ordering of chromosomes
    */
  def combine(chromosomes: Array[SparseChromosome], d: Long, parameters:Parameters): Array[SparseChromosome] = {
    val newChromosomes = parameters.random.shuffle(chromosomes.toSeq).toArray
    val pairs = newChromosomes.sliding(2, 2).toArray
    val hamming = pairs.map{ pair => ( pair(0).genes.union(pair(1).genes) -- pair(0).genes.intersect(pair(1).genes)).size  };
    (hamming zip pairs).filter( _._1 / 2 > d ).flatMap{ case (h, pair) => {val newPair = {
      pair.map(c => new SparseChromosome(parameters, c.numGenes, c.genes, c.fitness))
    }; combine(newPair,parameters)} }
  }

  def combine(pair: Array[SparseChromosome], parameters:Parameters): Array[SparseChromosome] = { // Only two chromosomes are assumed
    val changes = parameters.random.shuffle( ( pair(0).genes.union(pair(1).genes) -- pair(0).genes.intersect(pair(1).genes) ).toSeq   ) // pair(0).body.diff(pair(1).body).toSeq) // these has to be change, half in each
    val change = changes.grouped( changes.size / 2 + changes.size % 2 ).next().toSet // Only the first group is used - cuales son distintos..
    val newPair = pair.map{ case chromosome =>       // TODO: comprobar si es true false o false true (pienso que lo primero)
      { val finalChange = change //-- (change -- chromosome.body).filter(x => if (parameters.rndGen.nextFloat() < parameters.prob0to1Rec) false else true )  // TODO: check with mikel prob0to1Rec replaces prob1to0Combine
       // println("finalChange size "+finalChange.size)
        chromosome.genes = chromosome.genes.union(finalChange) -- finalChange.intersect(chromosome.genes);  chromosome.crossed = true;
        chromosome} } //println ("chromosomeSIZE: "+ chromosome.body.size);
    // Return new pair
    newPair
  }

  /**
    * A sparse chromosome represents the selected rules
    * @param parameters Parameters
    * @param numGenes number of genes
    */
  case class SparseChromosome(val parameters: Parameters, val numGenes: Int) extends Ordered[SparseChromosome] {

    val rulesList = for (i <- 0 until numGenes; if parameters.random.nextFloat() > 0.5) yield i
    var genes = HashSet(rulesList:_*)
    var crossed: Boolean = true
    var valid: Boolean = true
    var fitness: Float = 0

    def this(parameters: Parameters, numGenes: Int, rules: HashSet[Int], fitness: Float) {
      this(parameters, numGenes)
      this.genes = rules
      this.fitness = fitness
      crossed = false
    }

    /**
      * Parameter parameters.bestGenesPct gives us the percentage of genes that needs to be removed from the best solution so far.
      */
    def diverge(bestChromosome: SparseChromosome): Unit = {
      var newRules = HashSet.empty[Int]
      var iterRules = bestChromosome.genes.iterator
      while(iterRules.hasNext){
        if (parameters.random.nextFloat() > parameters.bestGenesPct){
          newRules +=  iterRules.next()
        }else{
          // Add rules randomly.
          val rndRule = parameters.random.nextInt(numGenes)
          newRules += rndRule // it doesn't matter if this is repeated because is a HashSet (-> no repetitions)
          iterRules.next()
        }
      }
      genes = newRules
      crossed = true
    }

    // return 0 if the same, negative if this > that, positive if this < that
    override def compare(that: SparseChromosome): Int = {
      if (this.fitness < that.fitness) +1
      else if (this.fitness > that.fitness) -1
      else 0
    }

    def getSelectedRules(): HashSet[Int] = {
      genes
    }

    override def toString(): String = { "Fitness = " + fitness.toString }

  }

}
