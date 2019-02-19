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
  * Represents a fuzzy variable of the problem, containing <i>l</i>
  * linguistic labels (fuzzy sets), being <i>l</i> the number of
  * linguistic labels specified by the user
  * @param name variable name
  * @param lowerBound Lower limit of the variable
  * @param upperBound Upper limit of the variable
  * @param numLinguisticLabels number of linguistic labels (fuzzy sets) that compose this variable
  */
case class FuzzyVariable(override val name: String,
                         override val id: Int,
                         var lowerBound: Double,
                         var upperBound: Double,
                         numLinguisticLabels: Byte) extends Variable(name, id) {

  val fuzzySets: Array[FuzzySet] = new Array(numLinguisticLabels) // Current fuzzy sets
  val fuzzySetsIni: Array[FuzzySet] = new Array(numLinguisticLabels) // Initial fuzzy sets
  var mergePoints: Array[Double] = new Array(numLinguisticLabels - 1) // Merge points of the current fuzzy sets

  /** *** Ruspini partitioning *** **/
  private var label: Int = 0
  while (label < numLinguisticLabels) {

    val halfBase = (upperBound - lowerBound) / (numLinguisticLabels - 1) // Compute the half of the triangle's base
    var leftPoint = lowerBound + halfBase * (label - 1)
    val midPoint = lowerBound + halfBase * label
    var rightPoint = lowerBound + halfBase * (label + 1)

    if (label == 0) leftPoint = midPoint
    else if (label == numLinguisticLabels - 1) rightPoint = midPoint

    fuzzySets(label) = new FuzzySet(leftPoint, midPoint, rightPoint, label.toByte)
    fuzzySetsIni(label) = new FuzzySet(leftPoint, midPoint, rightPoint, label.toByte)
    if (label > 0) mergePoints(label - 1) = midPoint - ((midPoint - fuzzySets(label - 1).midPoint) / 2f)

    label += 1

  }

  /**
    * Returns the variable label index corresponding to the input value, that is,
    * the one with the highest membership degree for the input value
    * @param inputValue input value
    * @return Variable label index corresponding to the input value
    */
  override def getLabelIndex(inputValue: String): Byte = {
    /** Since this function is only used in the learning stage, there is no need to compute
      * the membership degrees. Instead, the location of the input value with
      * respect to the point where two fuzzy sets merge is used
      */
    val index = mergePoints.indexWhere(_ > inputValue.toDouble)
    if (index == -1) mergePoints.length.toByte else index.toByte
  }

  //  /**
  //   * Returns the index of the fuzzy set corresponding to the input value, that is,
  //   * the one with the highest membership degree for the input value
  //   * @param inputValue input value
  //   * @return index of the fuzzy set (label) with the highest membership degree
  //   */
  //  def getLabel(inputValue: Double) : Byte = {
  //    /** Since this function is only used in the learning stage, there is no need to compute
  //      * the membership degrees. Instead, the location of the input value with
  //      * respect to the point where two fuzzy sets merge is used
  //      */
  //    mergePoints.find(_ >= value).getOrElse(mergePoints.length).toByte
  //  }


  override def normalize(value: String): Double = {
    System.err.println("This variable cannot be normalized"); System.exit(-1); -1
  }

  override def denormalize(value: Double): String = {
    System.err.println("This variable cannot be denormalized"); System.exit(-1); "-1"
  }

  def positiveMembership(fuzzySetIndex: Int, inputValue: Double): Boolean = {
    if ((inputValue < fuzzySets(fuzzySetIndex).leftPoint) || (inputValue > fuzzySets(fuzzySetIndex).rightPoint))
      false
    else
      true
  }

  override def toString(): String = {
    name+"\n"+"\t"
  }

}
