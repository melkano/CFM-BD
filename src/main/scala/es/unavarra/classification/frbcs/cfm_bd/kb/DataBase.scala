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

import es.unavarra.classification.frbcs.cfm_bd.Parameters
import es.unavarra.classification.frbcs.cfm_bd.preprocessing.Scaler
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

case class DataBase (numLinguisticLabels: Byte, costSensitive: Boolean) {

  var classLabels: Array[String] = _
  var classNumExamples: Array[Long] = _
  var classCost: Array[Float] = _
  var classFrequency: Array[Double] = _
  var variablesBounds: Array[(Double, Double)] = Array[(Double, Double)]() // original lower and upper bounds of the variables for preprocessing
  var variables: Array[Variable] = null

  def getClassIndex(classLabel: String): Byte = classLabels.indexOf(classLabel).toByte

  def readHeaderFile(filePath: String, variablesScale: Byte = DataBase.ORIGINAL_SCALE): Unit = {
    var numVariables: Int = 0
    val fileContents = Source.fromInputStream(FileSystem.get(Parameters.getHadoopConf()).open(
      new Path(filePath)).getWrappedStream).getLines.toList
    var tmpVariables: List[Variable] = List()
    var outputVars, inputVars: Array[String] = Array()
    try {
      fileContents.foreach { str =>
        str match {
          case str if str.contains("@attribute") => tmpVariables = tmpVariables :+ variableFromHeaderStr(str, numVariables, variablesScale); numVariables += 1;
          case str if str.contains("@inputs") =>
            inputVars = str.substring("@inputs".length).replaceAll(" ", "").split(",")
          case str if str.contains("@outputs") =>
            outputVars = str.substring("@outputs".length).replaceAll(" ", "").split(",")
          case str if str.contains("@numInstancesByClass") =>
            classNumExamples = str.substring("@numInstancesByClass".length).replaceAll(" ", "").split(",").map(_.toLong)
          case _ =>
        }
      }
    } catch {
      case e: Exception =>
        System.err.println("\nERROR READING HEADER FILE: Error while parsing @ fields \n")
        System.err.println(e)
        System.exit(-1)
    }

    if (classNumExamples == null) {
      System.err.println("\nERROR READING HEADER FILE: The number of examples of each class is not specified\n")
      System.exit(-1)
    }

    if (outputVars.length > 1) {
      System.err.println("\nERROR READING HEADER FILE: This algorithm does not support multiple outputs\n")
      System.exit(-1)
    }

    // Remove output variable and use as output class (it has to be a nominal variable)
    classLabels = tmpVariables.find(_.name == outputVars(0)).get match {
      case nominalVar: NominalVariable => nominalVar.nominalValues
      case _ => System.err.println("\nERROR READING HEADER FILE: The output has to be a nominal variable\n")
        System.exit(-1);
        null
    }
    variables = tmpVariables.filterNot(_.name == outputVars(0)).toArray

    val total = classNumExamples.sum
    classFrequency = classNumExamples.map(_.toDouble / total)

    if (costSensitive) {
      val max = classNumExamples.max
      classCost = classNumExamples.map(max / _.toFloat) // #examples majority class / #examples class
    }
    else
      classCost = Array.fill(classLabels.length) {1f}

  }

  def variableFromHeaderStr(str: String, id: Int, scale: Byte = DataBase.ORIGINAL_SCALE): Variable = {
    val attributePattern = """(@attribute)[ ]+([\w\-/]+)[ ]*(integer|real)?[ ]*[\[{]([^{}\[\]]*)[\]}]""".r // \w, &+:;/()\.\-
    val regexp = attributePattern.findFirstMatchIn(str).get
    val attName = regexp.group(2)
    val attType = regexp.group(3)
    var attRangeOriginal = regexp.group(4).split(",").map(_.trim)
    attType match {
      case "real" => {
        var attRange: (Double, Double) = (attRangeOriginal(0).toDouble, attRangeOriginal(1).toDouble)
        variablesBounds = variablesBounds :+ (attRange._1, attRange._2)
        if (scale == DataBase.LOG_SCALE)
          attRange = Scaler.toLogScale(attRange._1, attRange._2)
        else if (scale == DataBase.UNIFORM_SCALE)
          attRange = (0d, 1d)
        new FuzzyVariable(attName, id, attRange._1, attRange._2, numLinguisticLabels)
      }
      case "integer" => {
        var attRange: (Double, Double) = (attRangeOriginal(0).toDouble, attRangeOriginal(1).toDouble)
        if ((attRange._2 - attRange._1 + 1) <= numLinguisticLabels) { // Generate nominal variable
          variablesBounds = variablesBounds :+ (Double.MinValue, Double.MinValue)
          new NominalVariable(attName, id, (attRange._1.toInt to attRange._2.toInt).toArray.map(_.toString))
        }
        else {
          variablesBounds = variablesBounds :+ (attRange._1, attRange._2)
          if (scale == DataBase.LOG_SCALE)
            attRange = Scaler.toLogScale(attRange._1, attRange._2)
          else if (scale == DataBase.UNIFORM_SCALE)
            attRange = (0d, 1d)
          new FuzzyVariable(attName, id, attRange._1, attRange._2, numLinguisticLabels)
        }
      }
      case _ => {
        variablesBounds = variablesBounds :+ (Double.MinValue, Double.MinValue)
        new NominalVariable(attName, id, attRangeOriginal)
      }
    }
  }

}

case object DataBase {

  val ORIGINAL_SCALE: Byte = 0
  val LOG_SCALE: Byte = 1
  val UNIFORM_SCALE: Byte = 2

  private var instance: DataBase = null

  def getInstance(numLinguisticLabels: Byte, costSensitive: Boolean): DataBase = {
    if (instance == null)
      instance = new DataBase(numLinguisticLabels, costSensitive)
    instance
  }

}
