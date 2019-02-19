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

package es.unavarra.classification.frbcs.cfm_bd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source
import scala.util.Random

case class Parameters() {

  // CONSTANTS
  val BEST_GENES_PCT_FIELD: String = "best_genes_pct"
  val BITS_GENE_FIELD: String = "bits_gene"
  val COST_SENSITIVE_FIELD: String = "cost-sensitive"
  val EVOLUTIONARY_FIELD: String = "evolutionary"
  val FITNESS_MEASURE_FIELD: String = "fitness"
  val FRM_FIELD: String = "inference"
  val FRM_WINNING_RULE: Byte = 0
  val FRM_ADDITIVE_COMBINATION: Byte = 1
  val MAX_ATTRIBUTES_FIELD: String = "max_attributes"
  val MAX_EVAL_FIELD: String = "max_evaluations"
  val MAX_RESTARTS_NO_IMPROVE: String = "max_restarts_no_improve"
  val MIN_CRISP_CONFIDENCE_FIELD: String = "min_crisp_confidence"
  val MIN_FUZZY_CONFIDENCE_FIELD: String = "min_fuzzy_confidence"
  val NUM_LINGUISTIC_LABELS_FIELD: String = "num_linguistic_labels"
  val NUM_PARTITIONS_FIELD: String = "num_partitions"
  val POP_SIZE_FIELD: String = "population_size"
  val PROP_INIT_RULES_EVO_FIELD: String = "proportion_initial_rules_evolutionary"
  val PROP_RULE_LEN_FIELD: String = "proportions_rule_lengths"
  val VERBOSE_FIELD: String = "verbose"
  val WEIGHT_METHOD_FIELD: String = "weight_method"
  val WEIGHT_CF: Byte = 0
  val WEIGHT_PCF: Byte = 1

  // CFM-BD parameters
  var costSensitive: Boolean = true
  var evolutionary: Boolean = true
  var frm: Byte = -1
  var maxAtt: Int = -1
  var minCrispConfidence: Float = 1f
  var minFuzzyConfidence: Float = 1f
  var numLinguisticLabels: Byte = -1
  var propRuleLen: Array[Float] = null
  var weigthMethod: Byte = -1

  // CHC parameters
  var bestGenesPct: Float = 0.35f
  var bitsGene: Int = 30 // Only for 2-tuples
  var fitnessMeasure: String = "accuracy" // Measure to be optimized
  var maxEval: Int = 10000
  var maxRestartsNoImprove = 3
  var popSize: Int =  50
  var propInitRulesEvo = 1 // Take N times more rules for the evolutionary version

  // Paths
  var trainInputPath: String = _
  var testInputPath: String = _
  var outputPath: String = _

  // Hadoop
  var hdfsLocation: String = _
  var numPartitions: Int = 0

  // Environment
  val random: Random = new Random(123456789)
  var verbose = false

  def readConfigFile(filePath: String): Unit = {
    val fileContents = Source.fromInputStream(FileSystem.get(Parameters.getHadoopConf()).open(
      new Path(filePath)).getWrappedStream).getLines.toList
    fileContents.foreach { str =>
      val splitted = str.toLowerCase.split(" = ")
      splitted(0) match {
        case BEST_GENES_PCT_FIELD => bestGenesPct = splitted(1).toFloat
        case BITS_GENE_FIELD => bitsGene = splitted(1).toInt
        case COST_SENSITIVE_FIELD => costSensitive = splitted(1).toBoolean
        case EVOLUTIONARY_FIELD => evolutionary = splitted(1).toBoolean
        case FITNESS_MEASURE_FIELD => fitnessMeasure = splitted(1).toLowerCase().trim
        case FRM_FIELD => frm = splitted(1) match {
          case "wr" | "winningrule" | "winning_rule" => FRM_WINNING_RULE
          case _ => FRM_ADDITIVE_COMBINATION
        }
        case MAX_ATTRIBUTES_FIELD => maxAtt = splitted(1).toInt
        case MAX_EVAL_FIELD => maxEval = splitted(1).toInt
        case MAX_RESTARTS_NO_IMPROVE => maxRestartsNoImprove = splitted(1).toInt
        case MIN_CRISP_CONFIDENCE_FIELD => minCrispConfidence = splitted(1).toFloat
        case MIN_FUZZY_CONFIDENCE_FIELD => minFuzzyConfidence = splitted(1).toFloat
        case NUM_LINGUISTIC_LABELS_FIELD => numLinguisticLabels = splitted(1).toByte
        case NUM_PARTITIONS_FIELD => numPartitions = splitted(1).toInt
        case POP_SIZE_FIELD => popSize = splitted(1).toInt
        case PROP_INIT_RULES_EVO_FIELD => propInitRulesEvo = splitted(1).toInt
        case PROP_RULE_LEN_FIELD => propRuleLen = splitted(1).split(",").map(s => s.toFloat)
        case VERBOSE_FIELD => verbose = splitted(1).toBoolean
        case WEIGHT_METHOD_FIELD => weigthMethod = splitted(1) match {
          case "pcf" | "penalizedcertaintyfactor" | "penalized_certainty_factor" => WEIGHT_PCF
          case _ => WEIGHT_CF
        }
        case _ => System.err.println("Cannot parse input parameter in config file: " + str)
      }
    }
    if (propRuleLen.length != maxAtt){
      System.err.println("ERROR: the maximum number of attributes and the proportions of rule lengths must coincide")
      System.out.println("ERROR: the maximum number of attributes and the proportions of rule lengths must coincide")
    }
  }

}

case object Parameters {

  private var hadoopConf: Configuration = null // Throws an exception when serializing
  private var instance: Parameters = null

  def getInstance(): Parameters = {
    if (instance == null)
          instance = new Parameters()
    instance
  }

  def getHadoopConf(): Configuration = {
    if (hadoopConf == null)
      hadoopConf = new Configuration()
    hadoopConf
  }

}
