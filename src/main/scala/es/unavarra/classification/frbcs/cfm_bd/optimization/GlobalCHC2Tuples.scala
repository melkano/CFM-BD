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

import es.unavarra.classification.frbcs.cfm_bd.kb.{FuzzySet, FuzzyVariable, RuleBase}
import es.unavarra.classification.frbcs.cfm_bd.{CFM_BD, Parameters}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet

/**
 * @author Isaac Triguero, Mikel Galar, and Mikel Elkano Ilintxeta
 * 
 * This class implements a distributed CHC algorithm that uses all the data during the fitness evaluation.
 * In this case, we use CHC to select the most accurate rules
 */

object GlobalCHC2Tuples {

  def optimize(parameters: Parameters, ruleBase: RuleBase, sparkContext: SparkContext, inputFile: RDD[String], measure: String = "accuracy"): RuleBase = {

    // Initialize variables
    var restartsNoImprove: Int = 0
    var bestUntilRestart = 0f
    var eval: Long = 0
    val numChromosomes = parameters.popSize
    val numRules = ruleBase.rules.length
    val numFuzzySets = ruleBase.dataBase.variables.length * parameters.numLinguisticLabels
    val dInit: Long = Math.ceil((numRules + numFuzzySets * parameters.bitsGene) / 4.0).toLong;
    var d = dInit
    var strProgress, strOut: String = ""

    // Create initial population (first chromosome selects all rules and uses non-adjusted fuzzy sets)
    val t0 = System.currentTimeMillis()
    var chromosomes: Array[SparseChromosome] = Array.fill(parameters.popSize){ new SparseChromosome(parameters, numRules, numFuzzySets) }
    val initChromosomeRules = for (i <- 0 until numRules) yield i
    chromosomes(0) = new SparseChromosome(parameters, numRules, numFuzzySets, HashSet(initChromosomeRules:_*),
      Array.fill(numFuzzySets){0.5f}, 0f)

    // Evaluate initial population
    strOut += "Evaluating initial population...  "
    if (parameters.verbose)
      print("Evaluating initial population...  ")
    evaluateChromosomes(sparkContext, ruleBase, inputFile, chromosomes, measure)
    eval += chromosomes.length

    // Sort the population by fitness
    chromosomes = chromosomes.sorted
    strOut += "Done. " + chromosomes(0) + "\n\n"
    if (parameters.verbose)
      println("Done. " + chromosomes(0) + "\n")

    // Optimization loop
    while ((eval < parameters.maxEval) && (restartsNoImprove < parameters.maxRestartsNoImprove) && chromosomes(0).fitness < 1) {

      // Create new population by combining previous chromosomes
      var newChromosomes = combine(chromosomes, d, parameters)
      if (newChromosomes.length > 0) {
        evaluateChromosomes(sparkContext, ruleBase, inputFile, newChromosomes, measure)
        newChromosomes = newChromosomes.sorted
        eval += newChromosomes.length
      }
      val worstParentFitness = chromosomes.last.fitness
      if ((newChromosomes.length == 0) || (newChromosomes(0).fitness <= worstParentFitness))
        d -= Math.ceil(dInit * 0.1d).toLong
      else
        chromosomes = (chromosomes ++ newChromosomes).sorted.take(numChromosomes)

      // Restart (if required)
      if (d <= 0) {
        strOut += "\nRestarting...  "
        if (parameters.verbose)
          print("\nRestarting...  ")
        chromosomes.tail.foreach(_.diverge(chromosomes(0))) // tail takes all items except the first one
        evaluateChromosomes(sparkContext, ruleBase, inputFile, chromosomes.tail, measure)
        eval += chromosomes.tail.length
        d = dInit
        if (bestUntilRestart == chromosomes(0).fitness)
          restartsNoImprove += 1
        else {
          restartsNoImprove = 0
          bestUntilRestart = chromosomes(0).fitness
        }
        strOut += "Done.\n\n"
        if (parameters.verbose)
          println("Done.\n")
      }

      // Print progress
      if (!newChromosomes.isEmpty) {
        strProgress = "Eval: " + eval + " (new chromosomes: " + newChromosomes.length + ", added chromosomes: " +
          newChromosomes.filter(nc => nc.fitness > worstParentFitness).length +", restart threshold: " + d + "). " +
          chromosomes(0) + ". Time elapsed: " + ((System.currentTimeMillis - t0) / 1000) + "s"
        strOut += strProgress + "\n"
        if (parameters.verbose)
          println(strProgress)
      }

    }

    if (parameters.verbose)
      println("\n")
    if (eval >= parameters.maxEval) {
      println("Maximum number of evaluations (" + parameters.maxEval + ") reached")
      strOut += "\nMaximum number of evaluations (" + parameters.maxEval + ") reached"
    }
    else if (restartsNoImprove >= parameters.maxRestartsNoImprove){
      println("Maximum number of restarts (" + parameters.maxRestartsNoImprove + ") reached")
      strOut += "\nMaximum number of restarts (" + parameters.maxRestartsNoImprove + ") reached"
    }
    else {
      println("Maximum fitness reached")
      strOut += "\nMaximum fitness reached"
    }

    // Save progress to disk
    val pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
      new Path(parameters.outputPath + "/Evolution.txt"), true).getWrappedStream
    )
    pw.write(strOut)
    pw.close()

    // Decode the data base and the rule base encoded by the best performing chromosome
    chromosomes(0).updateFuzzySets(ruleBase, sparkContext)
    ruleBase.rules = ruleBase.rules.filter(r => chromosomes(0).genesRules.contains(r.id))
    ruleBase.rules.zipWithIndex.foreach{case (r, index) => r.id = index}
    ruleBase.broadcastRules(sparkContext) // Update rules on all workers
    ruleBase

  }

  def evaluateChromosomes(sparkContext: SparkContext, ruleBase: RuleBase, inputFile: RDD[String],
                          chromosomes: Array[SparseChromosome], measure: String) = {
    var i: Int = 0;
    // Evaluate all chromosomes
    while(i < chromosomes.length){
      // print("Evaluating chromosome...  ")
      // val t0aux = System.currentTimeMillis()
      // Update fuzzy sets
      chromosomes(i).updateFuzzySets(ruleBase, sparkContext)
      // Classify with the selected rules
      val confusionMatrix = ruleBase.classifyDataset(sparkContext, inputFile, chromosomes(i).genesRules)
      val results = CFM_BD.computeAccuracyStats(ruleBase.dataBase, confusionMatrix)
      val accuracy = measure.toLowerCase match {
        case "geometric mean" | "geometric_mean" | "geometric-mean" | "gm" => results(1)
        case "average accuracy" | "average_accuracy" | "average-accuracy" | "avgacc" => results(2)
        case other => results(0)
      }
      // Update chromosome's fitness
      val numInitialRules = ruleBase.rules.length
      chromosomes(i).fitness = accuracy - (numInitialRules / (numInitialRules - chromosomes(i).genesRules.size + 1).toFloat) * 0.15f
      // val t1aux = System.currentTimeMillis()
      // println("Done ("+((t1aux-t0aux)/1000)+" seconds)")
      // Next chromosome
      i += 1
    }
  }

  /**
    * Recombine
    * Takes chromosomes and d
    * flatMap after sliding with step 2, compute hamming, if hamming recombine -> fuse maps and remove half of the coincident elements from each map
    * First of all make shuffle of the ordering of chromosomes
    */
  def combine(chromosomes: Array[SparseChromosome], d: Long, parameters:Parameters): Array[SparseChromosome] = {
    // Shuffle population
    val newChromosomes = parameters.random.shuffle(chromosomes.toSeq).toArray
    val pairs = newChromosomes.sliding(2, 2).toArray
    val hamming = pairs.map{ pair => (pair(0).genesRules.union(pair(1).genesRules)
      -- pair(0).genesRules.intersect(pair(1).genesRules)).size + pair(0).hammingFuzzySets(pair(1))};
    (hamming zip pairs).filter( _._1 / 2 > d ).flatMap{ case (h, pair) => {val newPair = {
      pair.map(c => new SparseChromosome(parameters, c.nGenesRules,c.nGenesFuzzySets, c.genesRules, c.genesFuzzySets, c.fitness))
    }; combine(newPair,parameters)} }
  }

  def combine(pair: Array[SparseChromosome], parameters:Parameters): Array[SparseChromosome] = { // Only two chromosomes are assumed
    // Rules
    val changes = parameters.random.shuffle( ( pair(0).genesRules.union(pair(1).genesRules)
      -- pair(0).genesRules.intersect(pair(1).genesRules) ).toSeq )
    val change = changes.grouped( changes.size / 2 + changes.size % 2 ).next().toSet
    val newPair = pair.map{ case chromosome =>
      { val finalChange = change
        chromosome.genesRules = chromosome.genesRules.union(finalChange) -- finalChange.intersect(chromosome.genesRules)
        chromosome.crossed = true
        chromosome
      }}
    // Fuzzy sets
    newPair(0).xPC_BLX(newPair(1), 1f)
    // Return new pair
    newPair
  }

  /**
    * A sparse chromosome represents the selected rules and the lateral shift of fuzzy sets
    * @param numGenesRules number of genes for rules
    * @param numGenesFuzzySets number of genes for fuzzy sets
    */
  case class SparseChromosome(val parameters: Parameters, val numGenesRules: Int, val numGenesFuzzySets: Int) extends Ordered[SparseChromosome] {

    val nGenesRules = numGenesRules
    val nGenesFuzzySets = numGenesFuzzySets
    val rulesList = for (i <- 0 until nGenesRules) yield parameters.random.nextInt(nGenesRules)
    var genesRules = HashSet(rulesList:_*)
    var genesFuzzySets: Array[Float] = Array.fill(nGenesFuzzySets){parameters.random.nextFloat()}
    var crossed: Boolean = true
    var valid: Boolean = true
    var fitness: Float = 0

    def this(parameters: Parameters, nGenesRules: Int, nGenesFuzzySets: Int, rules: HashSet[Int], fuzzySets: Array[Float], fitness: Float) {
      this(parameters, nGenesRules, nGenesFuzzySets)
      this.genesRules = rules
      this.genesFuzzySets = fuzzySets
      this.fitness = fitness
      crossed = false
    }

    def diverge(bestChromosome: SparseChromosome): Unit = {
      // Rules
      var newRules = HashSet.empty[Int]
      var iterRules = bestChromosome.genesRules.iterator
      while(iterRules.hasNext){
        if (parameters.random.nextFloat() > parameters.bestGenesPct){ // parameters.bestGenesPct gives us the percentage of genes that needs to be removed from the best solution
          newRules += iterRules.next()
        }else{
          // Add rules randomly
          val rndRule = parameters.random.nextInt(nGenesRules)
          newRules += rndRule
          iterRules.next()
        }
      }
      genesRules = newRules
      // Fuzzy sets
      var i: Int = 0
      while (i < genesFuzzySets.length) {
        genesFuzzySets(i) = parameters.random.nextFloat()
        i += 1
      }

      crossed = true
    }

    // return 0 if the same, negative if this > that, positive if this < that
    override def compare(that: SparseChromosome): Int = {
      if (this.fitness < that.fitness) +1
      else if (this.fitness > that.fitness) -1
      else 0
    }

    def getAdjustedFuzzySets(): Array[Float] = {
      genesFuzzySets
    }

    def getSelectedRules(): HashSet[Int] = {
      genesRules
    }

    def updateFuzzySets(ruleBase: RuleBase, sparkContext: SparkContext): Unit ={
      val db = ruleBase.dataBase
      val vars = db.variables
      val nLbls = parameters.numLinguisticLabels
      var fuzzySets, fuzzySetsIni: Array[FuzzySet] = null
      var displacement: Double = 0d
      var i: Int = 0
      var j: Int = 0
      var pos: Int = 0
      while (i < vars.length) {
        if (vars(i).isInstanceOf[FuzzyVariable]) {
          fuzzySets = vars(i).asInstanceOf[FuzzyVariable].fuzzySets
          fuzzySetsIni = vars(i).asInstanceOf[FuzzyVariable].fuzzySetsIni
          j = 0
          while (j < nLbls) {
            if (j == 0)
              displacement = (genesFuzzySets(pos) - 0.5d) * (fuzzySetsIni(j+1).midPoint - fuzzySetsIni(j).midPoint);
            else if (j == (nLbls-1))
              displacement = (genesFuzzySets(pos) - 0.5d) * (fuzzySetsIni(j).midPoint - fuzzySetsIni(j-1).midPoint);
            else {
              if ((genesFuzzySets(pos) - 0.5d) < 0f)
                displacement = (genesFuzzySets(pos) - 0.5d) * (fuzzySetsIni(j).midPoint - fuzzySetsIni(j-1).midPoint);
              else
                displacement = (genesFuzzySets(pos) - 0.5d) * (fuzzySetsIni(j+1).midPoint - fuzzySetsIni(j).midPoint);
            }
            fuzzySets(j).updatePoints(fuzzySetsIni(j).leftPoint + displacement,
              fuzzySetsIni(j).midPoint + displacement,
              fuzzySetsIni(j).rightPoint + displacement)
            pos += 1
            j += 1
          }
        }
        i += 1
      }
      // Update data base in all workers
      // ruleBase.broadcastDataBase(sparkContext)
    }

    def hammingFuzzySets(otherChromosome: SparseChromosome): Int = {

      var i, j, pos, length, count: Int = 0
      var n: Long = 0
      var last: Char = ' '
      var increase: Double = 0
      var string1, string2, stringAux: Array[Char] = null

      length = nGenesFuzzySets * parameters.bitsGene
      string1 = new Array[Char](length)
      string2 = new Array[Char](length)
      stringAux = new Array[Char](length)

      increase = 1.0 / (Math.pow(2.0, parameters.bitsGene.toDouble) - 1.0)

      pos = 0
      i = 0
      while (i < nGenesFuzzySets) {
        n = (genesFuzzySets(i) / increase + 0.5f).toInt
        j = parameters.bitsGene - 1
        while (j >= 0) {
          stringAux(j) = ('0' + (n & 1)).toChar
          n >>= 1
          j -= 1
        }
        last = '0'
        j = 0
        while (j < parameters.bitsGene) {
          if (stringAux(j) != last)  string1(pos) = ('0' + 1).toChar
          else  string1(pos) = ('0' + 0).toChar
          last = stringAux(j)
          j += 1
          pos += 1
        }
        i += 1
      }

      pos = 0
      i = 0
      while (i < nGenesFuzzySets) {
        n = (otherChromosome.genesFuzzySets(i) / increase + 0.5f).toInt
        j = parameters.bitsGene - 1
        while (j >= 0) {
          stringAux(j) = ('0' + (n & 1)).toChar
          n >>= 1
          j -= 1
        }
        last = '0'
        j = 0
        while (j < parameters.bitsGene) {
          if (stringAux(j) != last)  string2(pos) = ('0' + 1).toChar
          else  string2(pos) = ('0' + 0).toChar
          last = stringAux(j)
          j += 1
          pos += 1
        }
        i += 1
      }

      count = 0
      i = 0
      while (i < length) {
        if (string1(i) != string2(i))  count += 1
        i += 1
      }

      return  count

    }

    def xPC_BLX (chromosome: SparseChromosome, d: Float): Unit ={

      var I: Float = 0
      var A1: Float = 0
      var C1: Float = 0
      var i: Int = 0

      while (i < nGenesFuzzySets) {
        I = d * Math.abs(genesFuzzySets(i) - chromosome.genesFuzzySets(i));
        A1 = genesFuzzySets(i) - I; if (A1 < 0f) A1 = 0f;
        C1 = genesFuzzySets(i) + I; if (C1 > 1f) C1 = 1f;
        genesFuzzySets(i) = A1 + parameters.random.nextFloat() * (C1 - A1);
        A1 = chromosome.genesFuzzySets(i) - I; if (A1 < 0f) A1 = 0f;
        C1 = chromosome.genesFuzzySets(i) + I; if (C1 > 1f) C1 = 1f;
        chromosome.genesFuzzySets(i) = A1 + parameters.random.nextFloat() * (C1 - A1);
        i += 1
      }

    }

    override def toString(): String = { "Fitness = " + fitness.toString }

  }

}
