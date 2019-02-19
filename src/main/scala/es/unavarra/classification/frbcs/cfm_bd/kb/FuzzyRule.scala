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

import scala.collection.mutable.HashMap

class FuzzyRule(var id: Int,
                antecedentsLabels: Array[(Int, Byte)], // the first item is the variable and the second one is the linguistic label
                val classIndex: Byte,
                val weight: Float) extends Serializable {

  var antecedents: Array[Byte] = antecedentsLabels.map(a => a._2) // Index of each linguistic label in the antecedent part
  var variables: Array[Int] = antecedentsLabels.map(a => a._1) // Index of the variables involved in the antecedent part

  override def equals(o: Any) = o match {
    case that: FuzzyRule => that.antecedents.deep == antecedents.deep; that.variables.deep == variables.deep
    case _ => false
  }

  override def hashCode(): Int = (variables zip antecedents).mkString("").hashCode

  def computeMatching(membershipDegrees: Array[Array[Float]]): Float = {
    var matching: Float = 1f
    var i: Int = 0
    while (i < antecedents.length && matching > 0){
      matching *= membershipDegrees(variables(i))(antecedents(i))
      i += 1
    }
    matching * weight
  }

  def getAntecedentsLabels(): Array[(Int, Byte)] = variables zip antecedents

  def toString(db: DataBase): String = {
    "IF " + (variables zip antecedents).map {
      a => db.variables(a._1).name + " IS " + {
        db.variables(a._1) match {
          case FuzzyVariable(_, _, _, _, _) => "L_" + a._2
          case nominalVar: NominalVariable => nominalVar.getValues(a._2)
        }
      }
    }.mkString(" AND ") + " THEN CLASS " + db.classLabels(classIndex) + " WITH RW = " + weight
  }
}

class CandidateFuzzyRule(id: Int, antecedentsLabels: Array[(Int, Byte)], val coverage: Map[Byte, Float] = null)
  extends FuzzyRule(id, antecedentsLabels, -1, -1) {

  val confidences: HashMap[Byte, Float] = HashMap[Byte, Float]()
  coverage.foreach(f => confidences += (f._1 -> f._2 / coverage.values.sum))
  var support = -1f
  val mostCoveredClass: Byte = coverage.maxBy(_._2)._1

  override def computeMatching(membershipDegrees: Array[Array[Float]]): Float = {
    var matching: Float = 1f
    var i: Int = 0
    while (i < antecedents.length && matching > 0){
      matching *= membershipDegrees(variables(i))(antecedents(i))
      i += 1
    }
    matching
  }

  def computeSupport(totalCoverage: Float): Unit = {support = coverage.values.sum / totalCoverage}

  override def toString(db: DataBase): String = {

    id + ": IF " + (variables zip antecedents).map {
      a => db.variables(a._1).name + " IS " + {
        db.variables(a._1) match {
          case FuzzyVariable(_, _, _, _, _) => "L_" + a._2
          case nominalVar: NominalVariable => nominalVar.getValues(a._2)
        }
      }
    }.mkString(" AND ")

  }

}
