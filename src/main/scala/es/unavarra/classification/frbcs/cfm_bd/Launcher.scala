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

import es.unavarra.classification.frbcs.cfm_bd.kb._
import es.unavarra.classification.frbcs.cfm_bd.utils.ComparableByteArray
import es.unavarra.classification.frbcs.cfm_bd.preprocessing.Scaler
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, HashSet}

object Launcher {

  val NUM_ARGS = 6

  val PREPROCESS_NONE: Byte = 0
  val PREPROCESS_LOG_SCALE: Byte = 1
  val PREPROCESS_UNIFORM: Byte = 2

  def main(args: Array[String]) {

    if ((args.length < NUM_ARGS) || (args.length > NUM_ARGS + 1)) {
      System.err.println("Usage: <hdfs://url:port> <config_file> <header_file> <output_path> [train_file] [test_file] [learn | classify]")
      System.exit(1)
    }

    // Create parameters
    val parameters = Parameters.getInstance()

    // Parse arguments
    parameters.hdfsLocation = args(0)
    val configFile: String = parameters.hdfsLocation + "/" + args(1)
    val headerFile: String = parameters.hdfsLocation + "/" + args(2)
    parameters.outputPath = parameters.hdfsLocation + "/" + args(3)
    val outputDir = new Path(parameters.outputPath)
    val fs = FileSystem.get(Parameters.getHadoopConf())
    if (fs.exists(outputDir))
      fs.delete(outputDir, true)

    // Parse optional arguments
    var mode = ""
    if (args.length == NUM_ARGS) {
      if ((args(args.length - 1) == "learn")) {
        parameters.trainInputPath = parameters.hdfsLocation + "/" + args(4)
        mode = "learn"
      }
      else if (args(args.length - 1) == "classify") {
        parameters.testInputPath = parameters.hdfsLocation + "/" + args(4)
        mode = "classify"
      }
      else {
        parameters.trainInputPath = parameters.hdfsLocation + "/" + args(4)
        parameters.testInputPath = parameters.hdfsLocation + "/" + args(5)
      }
    }
    else {
      parameters.trainInputPath = parameters.hdfsLocation + "/" + args(4)
      parameters.testInputPath = parameters.hdfsLocation + "/" + args(5)
      mode = args(args.length - 1)
    }

    // Read parameters and header file
    print("Reading config (" + configFile + ")...  ")
    parameters.readConfigFile(configFile)
    println("Done.")

    // Create Spark context
    // Logger.getLogger("org").setLevel(Level.OFF);
    // Logger.getLogger("akka").setLevel(Level.OFF);
    val conf = new SparkConf()
    //conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.eventLog.dir", parameters.outputPath)
    conf.set("spark.rdd.compress","true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Parameters], classOf[RuleBase], classOf[DataBase], classOf[RuleBaseWinningRule],
      classOf[RuleBaseAdditiveCombination], classOf[FuzzyRule], classOf[CandidateFuzzyRule], classOf[Variable],
      classOf[FuzzyVariable], classOf[NominalVariable], classOf[FuzzySet], classOf[ComparableByteArray]))
    conf.set("spark.default.parallelism", parameters.numPartitions.toString) // https://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
    //conf.set("spark.memory.fraction", "0.7") // WARNING: Should be avoided. Default value: 0.6
    //conf.set("spark.memory.storageFraction", "0.6") // WARNING: Should be avoided. Default value: 0.5
    print("Creating Spark context...  ")
    val sc: SparkContext = new SparkContext(conf)
    println("Done.")

    // Broadcast parameters
    print("Broadcasting config...  ")
    val paramsBroadcast = sc.broadcast(parameters)
    println("Done.")

    // Create data base
    val db: DataBase = DataBase.getInstance(parameters.numLinguisticLabels, parameters.costSensitive)
    print("Creating data base...  ")
    val t0 = System.currentTimeMillis()
    db.readHeaderFile(headerFile, variablesScale = DataBase.UNIFORM_SCALE)
    // translateFuzzySetsFromUniform(sc.textFile(parameters.trainInputPath, parameters.numPartitions).cache, db)
    println("Done (" + ((System.currentTimeMillis() - t0) / 1000) + " seconds).")
    /** REMOVE THIS: only for experiments **/
    /******************************************************************************************************************/
    /*
    db.variables.foreach(v => if (v.isInstanceOf[FuzzyVariable]) {
      val fuzzyVar = v.asInstanceOf[FuzzyVariable]
      println(v.name+": ("+fuzzyVar.lowerBound+", "+fuzzyVar.upperBound+")")
      fuzzyVar.fuzzySets.foreach(fs => println("\t"+fs.labelIndex+": "+fs.leftPoint+" | "+fs.midPoint+" | "+fs.rightPoint))
    })
    */
    /******************************************************************************************************************/

    // Learn and/or classify
    val preprocessMode: Byte = PREPROCESS_UNIFORM
    val classifier = CFM_BD
    mode.toLowerCase() match {
      case "learn" =>
        println("\n*******************************\nLearning...\n*******************************")
        print("\nLoading (" + parameters.trainInputPath + " => " + parameters.numPartitions + " partitions)...  ")
        val t0 = System.currentTimeMillis()
        var inputTrainFile = sc.textFile(parameters.trainInputPath, parameters.numPartitions)
        inputTrainFile = transformDataset(inputTrainFile, db, sc, preprocessMode).cache()
        println("Done (" + ((System.currentTimeMillis() - t0) / 1000) + " seconds).")
        classifier.learn(paramsBroadcast, sc.broadcast(db), sc, inputTrainFile)
      case "classify" =>
        println("\n*******************************\nTesting...\n*******************************")
        print("\nLoading (" + parameters.testInputPath + " => " + parameters.numPartitions + " partitions)...  ")
        val t0 = System.currentTimeMillis()
        var inputTestFile = sc.textFile(parameters.testInputPath, parameters.numPartitions)
        inputTestFile = transformDataset(inputTestFile, db, sc, preprocessMode).cache()
        println("Done (" + ((System.currentTimeMillis() - t0) / 1000) + " seconds).")
        val rb = RuleBase(parameters, parameters.outputPath + "/RB") // reads "RB"
        // val rbAC = new RuleBaseAdditiveCombination(sc.broadcast(db), rb.rules, sc) // WARNING!!! Remove this line
        classifier.classify(parameters, rb, sc, inputTestFile)
      case other =>
        println("\n*******************************\nLearning...\n*******************************")
        print("\nLoading (" + parameters.trainInputPath + " => " + parameters.numPartitions + " partitions)...  ")
        var t0 = System.currentTimeMillis()
        var inputTrainFile = sc.textFile(parameters.trainInputPath, parameters.numPartitions)
        var uniformTrainFile: (RDD[String], Array[Array[Double]]) = null
        if (preprocessMode == PREPROCESS_LOG_SCALE)
          inputTrainFile = Scaler.toLogScale(inputTrainFile.cache(), sc.broadcast(db), sc)
        else if (preprocessMode == PREPROCESS_UNIFORM) {
          uniformTrainFile = Scaler.toUniform(inputTrainFile.cache(), sc.broadcast(db), sc)
          inputTrainFile = uniformTrainFile._1
        }
        inputTrainFile.cache()
        println("Done (" + ((System.currentTimeMillis() - t0) / 1000) + " seconds).")
        /** REMOVE THIS: only for experiments **/
        /**************************************************************************************************************/
        // Save the histogram
        /*val t1 = System.currentTimeMillis()
        var varIdx = 0
        while (varIdx < db.variables.length) {
          if (db.variables(varIdx).isInstanceOf[FuzzyVariable]) {
            /*CFM_BD.saveHistogram(inputTrainFile.map(s => s.split(",")(varIdx).toDouble).histogram(10),
              parameters.outputPath + "/hist_"+db.variables(varIdx).name + ".txt")*/
            val stats = inputTrainFile.map(s => s.split(",")(varIdx).toDouble).stats()
            println("\n"+db.variables(varIdx).name+" (min | mean | max): "+stats.min+" | "+stats.mean+" | "+stats.max)
            println(db.variables(varIdx).name+":\n\t"+db.variables(varIdx).asInstanceOf[FuzzyVariable].fuzzySets.mkString("\n\t"))
          }
          varIdx += 1
        }*/
        /*varIdx = 0
        var break: Boolean = false
        while (!break) {
          if (db.variables(varIdx).isInstanceOf[FuzzyVariable]) {
            println("Total preprocessing time: " + ((System.currentTimeMillis() - t1) / 1000) + " seconds (" + uniformTrainFile._2(varIdx).size + " bins)")
            break = true
          }
          varIdx += 1
        }*/
        /**************************************************************************************************************/
        val rb = classifier.learn(paramsBroadcast, sc.broadcast(db), sc, inputTrainFile)
        println("\n*******************************\nTesting...\n*******************************")
        print("\nLoading (" + parameters.testInputPath + " => " + parameters.numPartitions + " partitions)...  ")
        t0 = System.currentTimeMillis()
        var inputTestFile = sc.textFile(parameters.testInputPath, parameters.numPartitions)
        if (preprocessMode == PREPROCESS_UNIFORM)
          inputTestFile = transformDataset(inputTestFile, db, sc, preprocessMode, uniformTrainFile._2).cache()
        println("Done (" + ((System.currentTimeMillis() - t0) / 1000) + " seconds).")
        // val rbAC = new RuleBaseAdditiveCombination(sc.broadcast(db), rb.rules, sc) // WARNING!!! Remove this line
        classifier.classify(parameters, rb, sc, inputTestFile)
    }

  }

  /**
    * Transforms the given dataset and updates (if specified) the database
    * @param dataset dataset to be preprocessed
    * @param dataBase database
    * @param sparkContext Spark context
    * @param mode type of transformation (PREPROCESS_LOG_SCALE, PREPROCESS_UNIFORM, or PREPROCESS_NONE)
    * @param quantiles quantiles' values
    * @return transformed dataset
    */
  def transformDataset(dataset: RDD[String], dataBase: DataBase, sparkContext: SparkContext, mode: Byte,
                       quantiles: Array[Array[Double]] = null): RDD[String] = {
    val dbBroadcast: Broadcast[DataBase] = sparkContext.broadcast(dataBase)
    if (mode == PREPROCESS_LOG_SCALE)
      Scaler.toLogScale(dataset.cache(), dbBroadcast, sparkContext)
    else if (mode == PREPROCESS_UNIFORM)
      Scaler.toUniform(dataset.cache(), dbBroadcast, sparkContext, quantiles)._1
    else
      dataset
  }

  /**
    * Translates fuzzy sets from a uniform distribution into the original scale
    * @param quantiles quantiles
    * @param dataBase data base
    */
  def translateFuzzySetsFromUniform(quantiles: Array[Array[Double]], dataBase: DataBase): Unit ={
    var varIdx = 0
    while (varIdx < dataBase.variables.length) {
      if (dataBase.variables(varIdx).isInstanceOf[FuzzyVariable]){
        val fuzzyVar = dataBase.variables(varIdx).asInstanceOf[FuzzyVariable]
        fuzzyVar.lowerBound = dataBase.variablesBounds(varIdx)._1
        fuzzyVar.upperBound = dataBase.variablesBounds(varIdx)._2
        fuzzyVar.fuzzySets.foreach(fs => {
          fs.leftPoint = Scaler.fromUniform(fs.leftPoint, varIdx, quantiles, dataBase)
          fs.midPoint = Scaler.fromUniform(fs.midPoint, varIdx, quantiles, dataBase)
          fs.rightPoint = Scaler.fromUniform(fs.rightPoint, varIdx, quantiles, dataBase)
        })
        fuzzyVar.mergePoints = fuzzyVar.mergePoints.map(p => Scaler.fromUniform(p, varIdx, quantiles, dataBase))
      }
      varIdx += 1
    }
  }

}
