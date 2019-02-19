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

/**
  * Represents a fuzzy set
  * @param leftPoint Left point of the triangle
  * @param midPoint Mid point of the triangle
  * @param rightPoint Right point of the triangle
  * @param labelIndex Linguistic label associated with this fuzzy set
  */
case class FuzzySet(var leftPoint: Double, var midPoint: Double,
                    var rightPoint: Double, val labelIndex: Byte) {

  /**
    * Returns the membership degree of the input value to this fuzzy set
    * If the value is out of the range of this function or variable, then the value returned will be 0.
    * @param value input value
    * @return membership degree of the input value to this fuzzy set
    */
  def computeMembershipDegree(value: Double): Double = {
    // Between the left and mid points
    if ((leftPoint < value) && (value < midPoint))
      (value - leftPoint) / (midPoint - leftPoint)
    // Between the mid and right points
    else if ((midPoint < value) && (value < rightPoint))
      (rightPoint - value) / (rightPoint - midPoint)
    // The value equals the mid point or exceeds the variable's bounds when the boundary fuzzy sets cover all real numbers
    else if ((value == midPoint) || (value < leftPoint && leftPoint == midPoint) || (value > rightPoint && rightPoint == midPoint))
      1f
    // Out of the fuzzy set
    else
      0
  }

  def updatePoints(leftPoint: Double, midPoint: Double, rightPoint: Double): Unit = {
    this.leftPoint = leftPoint
    this.midPoint = midPoint
    this.rightPoint = rightPoint
  }

  def center(newMidPoint: Double): Unit = {
    val shift: Double = newMidPoint - this.midPoint
    this.leftPoint += shift
    this.midPoint += shift
    this.rightPoint += shift
  }

  override def toString: String = {
    "L_" + labelIndex + ": (" + leftPoint+", " + midPoint + ", " + rightPoint + ")"
  }

}

//def computeMembershipDegree(value: Double): Double = {
//if ((leftPoint <= value) && (value <= midPoint)) // Between the left and mid points
//if ((leftPoint == value) && (midPoint == value)) 1f
//else (value - leftPoint) / (midPoint - leftPoint)
//else if ((midPoint <= value) && (value <= rightPoint)) // Between the mid and right points
//if ((midPoint == value) && (rightPoint == value)) 1f
//else (rightPoint - value) / (rightPoint - midPoint)
////    else if (value == midPoint) // The value is the mid point
////      1f
//else // Out of the fuzzy set
//0
//}
