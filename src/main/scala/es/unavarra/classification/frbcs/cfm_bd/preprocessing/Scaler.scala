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

package es.unavarra.classification.frbcs.cfm_bd.preprocessing

import es.unavarra.classification.frbcs.cfm_bd.kb.{DataBase, FuzzyVariable}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet
import scala.collection.Searching._

object Scaler {

  /**
    * Computes quantiles
    * @param dataset input dataset
    * @param dataBase data base
    * @param numBins number of bins
    * @param sparkContext Spark context
    * @return quantiles' values
    */
  def computeQuantiles (dataset: RDD[String], dataBase: DataBase, numBins: Int, sparkContext: SparkContext): Array[Array[Double]] = {
    val numSamples = dataset.count()
    val idx = for (i <- 1 to numBins) yield i
    val quantiles = Array(idx: _*).map(i => i / idx.length.toDouble)
    val computedQuantiles = new Array[Array[Double]](dataBase.variables.length)
    var varIdx = 0
    while (varIdx < dataBase.variables.length) {
      if (dataBase.variables(varIdx).isInstanceOf[FuzzyVariable]) {
        val sortedRdd = dataset.map(s => s.split(",")(varIdx).toDouble).sortBy(x => x, true).zipWithIndex()
        val quantilesIdx: HashSet[Long] = HashSet[Long]()
        var i = 0
        while (i < quantiles.length){
          quantilesIdx += Math.ceil(quantiles(i) * numSamples - 1).toLong.max(0L)
          i += 1
        }
        val quantilesBroad = sparkContext.broadcast(quantilesIdx)
        computedQuantiles(varIdx) = sortedRdd.filter(x => quantilesBroad.value.contains(x._2))
          .sortBy(x => x._2, true).map(x => x._1).collect()
      }
      varIdx += 1
    }
    computedQuantiles
  }

  /**
    * Recovers the value of the original distribution from the transformed uniform distribution
    * @param value value belonging to the uniform distribution
    * @param variableIndex index of the variable
    * @param quantiles quantiles used to transform the original distribution into a uniform distribution
    * @return value from the original distribution
    */
  def fromUniform (value: Double, variableIndex: Int, quantiles: Array[Array[Double]], dataBase: DataBase): Double = {
    if (value < 1) {
      if (value > 0){
        val quantileIdx = (value * quantiles(variableIndex).length).toInt - 1
        val x1 = quantileIdx match {
          case 0 => dataBase.variablesBounds(variableIndex)._1
          case other => quantiles(variableIndex)(quantileIdx - 1)
        }
        val x2 = quantiles(variableIndex)(quantileIdx)
        val y1 = quantileIdx match {
          case 0 => 0
          case other => quantileIdx / quantiles(variableIndex).length.toDouble
        }
        val y2 = (quantileIdx + 1) / quantiles(variableIndex).length.toDouble
        if ((x2 - x1) > 0) {
          val m = (y2 - y1) / (x2 - x1)
          ((value - y1) / m) + x1
        }
        else
          x1
      }
      else
        dataBase.variablesBounds(variableIndex)._1
    }
    else
      dataBase.variablesBounds(variableIndex)._2
  }

  /**
    * Transforms the dataset using a logarithmic scale
    * @param dataset dataset to be transformed
    * @param db data base
    * @param sparkContext Spark context
    * @return the dataset on a logarithmic scale
    */
  def toLogScale (dataset: RDD[String], db: Broadcast[DataBase], sparkContext: SparkContext): RDD[String] = {
    // Get the lower bound of each variable
    val minVal: Array[Double] = new Array[Double](db.value.variables.length)
    var i = 0
    while (i < minVal.length) {
      if (db.value.variables(i).isInstanceOf[FuzzyVariable])
        minVal(i) = db.value.variablesBounds(i)._1
      i += 1
    }
    val broadMinVal = sparkContext.broadcast(minVal)
    // Transform the dataset
    dataset.map(s => {
      val splitStr = s.split(",").map(_.trim)
      val (inputValues, classLabel) = splitStr.splitAt(splitStr.length - 1)
      val scaledValues = new Array[String](inputValues.length)
      var inputVal: Double = Double.MinValue
      val minValStr = Double.MinValue.toString
      var i = 0
      while (i < inputValues.length) {
        if (db.value.variables(i).isInstanceOf[FuzzyVariable]) {
          if (broadMinVal.value(i) < 0)
            inputVal = (inputValues(i).toDouble - broadMinVal.value(i) + 1).max(1)
          else
            inputVal = inputValues(i).toDouble
          if (inputVal > 0)
            scaledValues(i) = Math.log(inputVal).toString
          else
            scaledValues(i) = "0"
        }
        else
          scaledValues(i) = inputValues(i)
        i += 1
      }
      scaledValues.mkString(",") + "," + classLabel(0)
    })
  }

  /**
    * Returns the lower and upper bounds in logarithmic scale
    * @param lowerBound lower bound
    * @param upperBound upper bound
    * @return lower and upper bounds in logarithmic scale
    */
  def toLogScale (lowerBound: Double, upperBound: Double): (Double, Double) = {
    if (lowerBound > 0)
      (Math.log(lowerBound), Math.log(upperBound))
    else
      (0d, Math.log(upperBound - lowerBound + 1))
  }

  /**
    * Creates a new dataset where all variables follow a uniform distribution
    * https://en.wikipedia.org/wiki/Probability_integral_transform
    * @param dataset dataset to be transformed
    * @param db broadcast data base
    * @param sparkContext Spark context
    * @param quantiles array containing the values of the quantiles
    * @return a dataset where all variables follow a uniform distribution
    */
  def toUniform (dataset: RDD[String], db: Broadcast[DataBase], sparkContext: SparkContext, quantiles: Array[Array[Double]] = null): (RDD[String], Array[Array[Double]]) = {
    // Compute quantiles
    var quantilesBroad: Broadcast[Array[Array[Double]]] = quantiles match {
      case null => sparkContext.broadcast(computeQuantiles(dataset, db.value, 1000, sparkContext)) // 1,000 bins are usually enough
      case other => sparkContext.broadcast(quantiles)
    }
    // Transform the dataset
    (dataset.map(s => {
      val inputVals = s.split(",")
      val outputVals: Array[String] = new Array[String](inputVals.length)
      var i = 0
      while (i < inputVals.length - 1){
        if (db.value.variables(i).isInstanceOf[FuzzyVariable]) {
          val value = inputVals(i).toDouble
          // var quantileIdx = quantilesBroad.value(i).indexWhere(_ > value)
          val quantileIdx = quantilesBroad.value(i).search(value).insertionPoint
          if (quantileIdx < quantilesBroad.value(i).length) {
            val x1 = quantileIdx match {
              case 0 => db.value.variablesBounds(i)._1
              case other => quantilesBroad.value(i)(quantileIdx - 1)
            }
            val x2 = quantilesBroad.value(i)(quantileIdx)
            val y1 = quantileIdx match {
              case 0 => 0d
              case other => quantileIdx / quantilesBroad.value(i).length.toDouble
            }
            val y2 = (quantileIdx + 1) / quantilesBroad.value(i).length.toDouble
            if ((x2 - x1) > 0) {
              val m = (y2 - y1) / (x2 - x1)
              outputVals(i) = (m * (value - x1) + y1).toString
            }
            else
              outputVals(i) = y1.toString
          }
          else
            outputVals(i) = "1.0"
        }
        else
          outputVals(i) = inputVals(i)
        i += 1
      }
      outputVals(outputVals.length - 1) = inputVals(inputVals.length - 1)
      outputVals.mkString(",")
    }), quantilesBroad.value)
  }

}
