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
  * Represents a nominal variable of the problem
  * @param name variable name
  * @param nominalValues nominal values of this variable
  */
case class NominalVariable(override val name: String, override val id: Int, val nominalValues: Array[String]) extends Variable(name, id) {

  val numValues: Float = nominalValues.length

  /**
    * Returns the nominal value at the specified position
    * @param index position of nominal value
    * @return nominal value at the specified position
    */
  def getValues(index: Byte): String = {
    nominalValues(index)
  }

  override def normalize(value: String): Double = (getLabelIndex(value).toDouble / numValues)

  /**
    * Returns the variable label index corresponding to the nominal value
    * @param label input value
    * @return Variable label index corresponding to the input value
    */
  override def getLabelIndex(label: String): Byte = {
    nominalValues.indexOf(label).toByte
  }

  override def denormalize(value: Double): String = nominalValues((value * numValues).toInt)

  override def toString(): String = "NominalVariable(" + name + ",[" + nominalValues.mkString(",") + "])"

}
