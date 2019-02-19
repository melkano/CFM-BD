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

import java.io.{ObjectInputStream, ObjectOutputStream, Serializable}

import es.unavarra.classification.frbcs.cfm_bd.Parameters
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.annotation.switch
import scala.collection.immutable.IndexedSeq

class CandidateRuleBase extends Serializable {

  var db: DataBase = null
  var rules: Array[CandidateFuzzyRule] = null
  var rulesClasses: Array[Array[Byte]] = null // Classes of each rule

  def this (path:String){
    this()
    val ois = new ObjectInputStream(FileSystem.get(Parameters.getHadoopConf()).open(new Path(path)))
    val rb = ois.readObject().asInstanceOf[CandidateRuleBase]
    ois.close()
    this.db = rb.db
    this.rules = rb.rules
    this.rulesClasses = rb.rulesClasses
  }

  def this(db: DataBase, rules: List[CandidateFuzzyRule]) {
    this()
    // Initialize structures
    rulesClasses = new Array[Array[Byte]](rules.length)
    // Get the database
    this.db = db
    // Set the ID of each rule
    rules.zipWithIndex.foreach{case (r, index) => r.id = index}
    // Get rules' classes
    rules.foreach(r => {rulesClasses(r.id) = r.coverage.keySet.toArray})
    // Transform the rule base into an array
    this.rules = rules.toArray
  }

  def computeMatchingPartition(examples: Iterator[String]): List[(Int, IndexedSeq[(Byte, Float)])] = {

    var exampleValues: Array[String] = null
    var exampleLabel: Byte = -1
    var memberships: Array[Array[Float]] = null // Pre-computed membership degrees of a given example
    var aggMatchingDegrees: Array[Array[Float]] =
      Array.fill[Float](rulesClasses.length, db.classLabels.length)(0f) // Total matching degree of each class of each rule

    // Compute the matching degree of the examples with all rules
    var r: Int = 0
    val n_rules: Int = rules.length
    while (examples.hasNext) {
      // Get the example's values and class index
      exampleValues = examples.next().split(",").map(_.trim)
      exampleLabel = db.getClassIndex(exampleValues.last)
      // Pre-compute membership degrees
      memberships = precomputeMemberships(exampleValues)
      // Compute the matching degree with all the rules
      r = 0
      while (r < n_rules){
        aggMatchingDegrees(r)(exampleLabel) += rules(r).computeMatching(memberships) * db.classCost(exampleLabel)
        r += 1
      }
    }

    // Return the matching of each rule
    rules.toList.map(
      r => (r.id, for(classIndex  <- 0 until db.classLabels.length)
        yield (classIndex.toByte, aggMatchingDegrees(r.id)(classIndex)))
    )

  }

  def precomputeMemberships(exampleValues: Array[String]): Array[Array[Float]] = {
    // pre-compute all matchings with all labels
    db.variables.map(v => (v: @switch) match {
      // precomputed = numVariables x numLabels (may vary if nominals)
      case fuzzyVar: FuzzyVariable => fuzzyVar.fuzzySets.map(_.computeMembershipDegree(exampleValues(v.id).toFloat).toFloat)
      case nominalVar: NominalVariable => nominalVar.nominalValues.map(value => if (value == exampleValues(v.id)) 1f else 0f)
    })
  }

  def save(fileName: String): Unit = {
    val oos = new ObjectOutputStream(FileSystem.get(Parameters.getHadoopConf()).create(new Path(fileName),true))
    oos.writeObject(this)
    oos.close()
  }

}
