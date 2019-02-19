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
  * Represents a variable of the problem
  * @param name variable name
  */
abstract class Variable(val name: String, val id: Int) extends Serializable {

  /**
    * It should return the variable label index corresponding to the input value
    * @param inputValue input value
    * @return Variable label index of the input value
    */
  def getLabelIndex(inputValue: String): Byte

  def normalize(value: String): Double

  def denormalize(value: Double): String
}
