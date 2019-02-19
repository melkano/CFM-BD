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

package es.unavarra.classification.frbcs.cfm_bd.kb

import java.io._

import es.unavarra.classification.frbcs.cfm_bd.Parameters
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.annotation.switch
import scala.collection.mutable.{HashMap, HashSet}

abstract class RuleBase extends Serializable {

  var dataBase: DataBase = null
  var dbBroadcast: Broadcast[DataBase] = null
  var rules: Array[FuzzyRule] = null
  var rulesBroadcast: Broadcast[Array[FuzzyRule]] = null

  def this(dbBroadcast: Broadcast[DataBase], rules: Array[FuzzyRule], sparkContext: SparkContext) {
    this()
    this.dataBase = dbBroadcast.value
    this.dbBroadcast = dbBroadcast
    // Set the ID of each rule
    rules.zipWithIndex.foreach{case (r, index) => r.id = index}
    // Transform the rule base into an array
    this.rules = rules
    broadcastRules(sparkContext)
  }

  /**
    * Update rules in all workers
    */
  def broadcastRules(sparkContext: SparkContext): Unit = {
    if (rulesBroadcast != null){
      rulesBroadcast.unpersist()
      rulesBroadcast.destroy()
    }
    rulesBroadcast = sparkContext.broadcast(rules)
  }

  def classifyExample(example: Array[String], selectedRules: HashSet[Int] = null): (Int, Float)

  def classifyDataset(sparkContext: SparkContext, inputFile: RDD[String], selectedRules: HashSet[Int] = null): Array[Array[Long]] = {
    val confMat = inputFile.mapPartitions(examples => {
      // Initialize confusion matrix
      var i, j: Int = 0
      val confMat: HashMap[(Int, Int), Long] = new HashMap[(Int, Int), Long]()
      while (i < dbBroadcast.value.classLabels.length) {
        j = 0
        while (j < dbBroadcast.value.classLabels.length) {
          confMat((i, j)) = 0l
          j += 1
        }
        i += 1
      }
      // Classify all examples
      var example: Array[String] = null
      var label: Int = -1
      var prediction: (Int, Float) = null
      while (examples.hasNext){
        example = examples.next().split(",").map(_.trim)
        label = dbBroadcast.value.getClassIndex(example.last).toInt
        prediction = classifyExample(example, selectedRules)
        confMat((label, prediction._1.toInt)) += 1l
      }
      confMat.toIterator
    }).reduceByKey(_ + _).collect()
    // Transform the confusion matrix into an array
    val confusionMatrix: Array[Array[Long]] = Array.fill(dataBase.classLabels.length){Array.fill(dataBase.classLabels.length){0}}
    confMat.foreach {
      case ((real, predicted), count) =>
        confusionMatrix(real)(predicted) = count;
    }
    confusionMatrix
  }

  def precomputeMemberships(exampleValues: Array[String]): Array[Array[Float]] = {
    // pre-compute the matching with all labels
    dbBroadcast.value.variables.map(v => (v: @switch) match {
      case fuzzyVar: FuzzyVariable => fuzzyVar.fuzzySets.map(_.computeMembershipDegree(exampleValues(v.id).toDouble).toFloat)
      case nominalVar: NominalVariable => nominalVar.nominalValues.map(value => if (value == exampleValues(v.id)) 1f else 0f)
    })
  }

  // Returns the matching of the example with all rules
  // The last column is the class label index of the example
  def computeMatchingExample(example: String, selectedRules: HashSet[Int] = null): Array[Float] = {

    val n_rules: Int = rulesBroadcast.value.length
    var exampleValues: Array[String] = example.split(",").map(_.trim)
    var exampleLabel: Byte = dbBroadcast.value.getClassIndex(exampleValues.last)
    var memberships: Array[Array[Float]] = precomputeMemberships(exampleValues)

    // Compute the matching degree of the examples with all rules
    var matchings: Array[Float] = Array.fill[Float](n_rules+1)(0f)
    var i: Int = 0
    if (selectedRules == null)
      while (i < n_rules){
        matchings(i) += rulesBroadcast.value(i).computeMatching(memberships)
        i += 1
      }
    else
      while (i < n_rules){
        if (selectedRules.contains(rulesBroadcast.value(i).id))
          matchings(i) += rulesBroadcast.value(i).computeMatching(memberships)
        i += 1
      }
    matchings(n_rules) = exampleLabel

    matchings

  }

  def save(fileName: String, textFormat: Boolean): Unit = {
    if (textFormat) {
      val pw = new PrintWriter(FileSystem.get(Parameters.getHadoopConf()).create(
        new Path(fileName + "_text.txt"), true).getWrappedStream
      )
      pw.write(toString)
      pw.close()
    }
    val oos = new ObjectOutputStream(FileSystem.get(Parameters.getHadoopConf()).create(new Path(fileName),true))
    oos.writeObject(this)
    oos.close()
  }


  override def toString(): String = {
    "------------------\nNumber of rules = " + rules.length + "\n------------------\n\n" + rules.map(r => r.toString(dataBase)).mkString("\n")
  }

}

object RuleBase {

  def apply(parameters: Parameters, fileName: String): RuleBase = {
    val ois = new ObjectInputStream(FileSystem.get(Parameters.getHadoopConf()).open(new Path(fileName)))
    val rb = parameters.frm match {
      case parameters.FRM_WINNING_RULE => ois.readObject().asInstanceOf[RuleBaseWinningRule]
      case parameters.FRM_ADDITIVE_COMBINATION => ois.readObject().asInstanceOf[RuleBaseAdditiveCombination]
      case other =>
        System.err.println("\nERROR: Please, choose between Winning Rule and Additive Combination\n");
        println("\nERROR: Please, choose between Winning Rule and Additive Combination\n");
        System.exit(-1);
        null
    }
    ois.close()
    rb
  }

}

class RuleBaseAdditiveCombination(dbBroadcast2: Broadcast[DataBase], rules2: Array[FuzzyRule], sparkContext: SparkContext)
  extends RuleBase(dbBroadcast2, rules2, sparkContext) {

  override def classifyExample(example: Array[String], selectedRules: HashSet[Int] = null): (Int, Float) = {
    // Pre-compute membership degrees
    val precomputed = precomputeMemberships(example)
    // Compute the matching with all rules
    var i: Int = 0
    val nRules = rulesBroadcast.value.length
    val matching: Array[Float] = Array.fill[Float](dbBroadcast.value.classLabels.length)(0f)
    if (selectedRules == null)
      while (i < nRules){
        matching(rulesBroadcast.value(i).classIndex) += rulesBroadcast.value(i).computeMatching(precomputed)
        i += 1
      }
    else
      while (i < nRules){
        if (selectedRules.contains(rulesBroadcast.value(i).id))
          matching(rulesBroadcast.value(i).classIndex) += rulesBroadcast.value(i).computeMatching(precomputed)
        i += 1
      }
    // Get winning class
    val winClass = matching.zipWithIndex.maxBy(_._1)
    if (winClass._1 > 0)
      (winClass._2, winClass._1)
    else
      (dbBroadcast.value.classFrequency.zipWithIndex.maxBy(_._1)._2, winClass._1) // Default class = most frequent class
  }

}

class RuleBaseWinningRule(dbBroadcast2: Broadcast[DataBase], rules2: Array[FuzzyRule], sparkContext: SparkContext)
  extends RuleBase(dbBroadcast2, rules2, sparkContext) {

  override def classifyExample(example: Array[String], selectedRules: HashSet[Int] = null): (Int, Float) = {
    // Pre-compute membership degrees
    val precomputed = precomputeMemberships(example)
    // Compute the matching with all rules
    var i: Int = 0
    val nRules = rulesBroadcast.value.length
    var maxMatching, currMatching: Float = 0f
    var winClass: Int = dbBroadcast.value.classFrequency.zipWithIndex.maxBy(_._1)._2 // Default class = most frequent class
    if (selectedRules == null)
      while (i < nRules){
        currMatching = rulesBroadcast.value(i).computeMatching(precomputed)
        if (currMatching > maxMatching) {
          maxMatching = currMatching
          winClass = rulesBroadcast.value(i).classIndex
        }
        i += 1
      }
    else
      while (i < nRules){
        if (selectedRules.contains(rulesBroadcast.value(i).id)) {
          currMatching = rulesBroadcast.value(i).computeMatching(precomputed)
          if (currMatching > maxMatching) {
            maxMatching = currMatching
            winClass = rulesBroadcast.value(i).classIndex
          }
        }
        i += 1
      }
    // Return winning class
    (winClass, maxMatching)
  }

}
