/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.classification

import java.util

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.JavaConverters._


class CRQSelection extends Transformer[CRQSelection]{

  import CRQSelection._

  // The data needed to transform based on the crq model
  //(class -> document id -> word -> word frequency -> P_i(w_t|d_i)
  //                                            -> log(P_j(w_t|d_i)) -> log(P(w_t)))
  var crqModel: Option[DataSet[(String, Int, String, Int, Double, Double, Double)]] = None

  def setSaveLocationCategories(value: String): CRQSelection = {
    parameters.add(saveLocationCategories, value)
    this
  }

}

object CRQSelection {

  // ====================================== Parameters =============================================

  /**
   * Save location for the data set storing the
   * correct categories from the transformed predcit data set and their document id.
   */
  case object saveLocationCategories extends Parameter[String] {
    override val defaultValue: Option[String] = None
  }

  // ==================================== Factory methods ==========================================

  def apply(): CRQSelection = {
    new CRQSelection()
  }

  // ====================================== Operations =============================================

  implicit val crqFit = new FitOperation[CRQSelection, (String, Int, String)] {
    /**
     * Creates the [[CRQSelection.crqModel]] needed for the transformation.
     * @param instance of [[CRQSelection]]
     * @param fitParameters to configure
     * @param input, the input data set to train the model
     */
    override def fit(instance: CRQSelection,
                     fitParameters: ParameterMap,
                     input: DataSet[(String, Int, String)]): Unit = {

      //Create a data set that stores all word related information
      // 1. Map: Split documents into its words and add a 1 to each tuple
      // 2. Group-Reduce: sum the 1s by class and document id
      val wordRelated: DataSet[(String, Int, String, Int)] = input.flatMap(new SingleWordSplitter)
        .groupBy(0, 1, 2)
        .sum(3) // (class -> document id -> word -> word frequency)

      wordRelated.writeAsCsv("/Users/jonathanhasenburg/Desktop/nbtemp/wordRelated",
        "\n", "\t", WriteMode.OVERWRITE)

      //Create a data set that stores all document related information
      // 1. Map: remove the word information
      // 2. Group-Reduce: sum the word counts in each document
      val docRelated: DataSet[(String, Int, Int)] = wordRelated
        .map(line => (line._1, line._2, line._4))
        .groupBy(0, 1).reduce((line1, line2)
      => (line1._1, line1._2, line1._3 + line2._3)) // (document id -> total word frequency)

      docRelated.writeAsCsv("/Users/jonathanhasenburg/Desktop/nbtemp/docRelated",
        "\n", "\t", WriteMode.OVERWRITE)

      //Calculate P_i(w_t|d_i) data set
      // 1. Map: calculate P_i(w_t|d_i)
      val piSet: DataSet[(String, String, Int, Double, Int)] = wordRelated
        .map(new RichMapFunction[(String, Int, String, Int),
        (String, String, Int, Double, Int)] {

        var broadcastMap: mutable.Map[Int, Double] = mutable.Map[Int, Double]()

        override def open(config: Configuration): Unit = {
          val collection = getRuntimeContext
            .getBroadcastVariable[(String, Int, Int)]("docRelated").asScala
          for (record <- collection) {
            broadcastMap.put(record._2, record._3.toDouble)
          }
        }

        override def map(value: (String, Int, String, Int)): (String, String, Int, Double, Int) = {
          (value._3, value._1, value._2, value._4.toDouble / broadcastMap(value._2), value._4)
        }
      }).withBroadcastSet(docRelated, "docRelated")
      // (word -> class -> document id ->  P_i(w_t|d_i) -> word frequency)

      piSet.writeAsCsv("/Users/jonathanhasenburg/Desktop/nbtemp/piSet",
        "\n", "\t", WriteMode.OVERWRITE)

      //Calculate the total number of words of all documents of class c_j
       //1. Map: remove document id
       //2. Group-Reduce: sum the frequencies
      val sumWordFreqForClass: DataSet[(String, Double)] = docRelated
        .map(line => (line._1, line._3.toDouble))
        .groupBy(0).sum(1)

      sumWordFreqForClass.writeAsCsv("/Users/jonathanhasenburg/Desktop/nbtemp/sumWordFreqForClass",
        "\n", "\t", WriteMode.OVERWRITE)

      //Calculate the total number of words of all document for all classes
       //1. Map: remove class
       //2. Group-Reduce: sum the frequencies
      val sumWordFreqAllClasses: DataSet[(Double)] = sumWordFreqForClass
        .map(line => line._2).reduce((val1, val2) => val1 + val2)

      sumWordFreqAllClasses.writeAsText("/Users/jonathanhasenburg/Desktop/nbtemp/" +
        "sumWordFreqAllClasses", WriteMode.OVERWRITE)

      /* This is wrong, because multiplikation with single values, not whole term
      //Calculate sum of all P_i(w_t|d_i) for all documents of class c_j
       // 1. Map: Remove word, documentID and word frequency
       // 2. Group-Reduce: sum the P_i for all documents of a class
      val sumPiForClass: DataSet[(String, Double)] = piSet
        .map(line => (line._1, line._4))
        .groupBy(0).sum(1) // class -> sum P_i(w_t|d_i) for all documents of class

      sumPiForClass.writeAsCsv("/Users/jonathanhasenburg/Desktop/nbtemp/sumPiForClass",
        "\n", "\t", WriteMode.OVERWRITE)

      //Calculate sum of all P_i(w_t|d_i) for all documents for all classes
       // 1. Map: Remove class
       // 2. Reduce: Add all values together
      val sumPiAllClasses: DataSet[(Double)] = sumPiForClass
        .map(line => (line._2))
        .reduce((val1, val2) => val1 + val2)

      sumPiAllClasses.writeAsText("/Users/jonathanhasenburg/Desktop/nbtemp/sumPiAllClasses",
        WriteMode.OVERWRITE)
        */
/*
      //Create the crqModel by calculating P_j(w_t|d_i) and P(w_t)
      val crqModel: DataSet[(String, String, Int, Double, Double, Double)] = piSet
        .map(new RichMapFunction[(String, String, Int, Double,  Int),
        (String, Int, String, Double,  Double, Double)] {

        var docRelatedMap: mutable.Map[Int, Double] = mutable.Map[Int, Double]()
        var sumWordFreqForClassMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
        var sumWordFreqAllClassesSet: util.List[Double] = null

        override def open(config: Configuration): Unit = {

          sumWordFreqAllClassesSet = getRuntimeContext
            .getBroadcastVariable[Double]("sumWordFreqAllClasses")
          if (sumWordFreqAllClassesSet.size() != 1) {
            throw new RuntimeException("The sumWordFreqAllClassesSet data set has the " +
              "wrong size! This should not happen, it is. " + sumWordFreqAllClassesSet.size())
          }

          val collection = getRuntimeContext
            .getBroadcastVariable[(String, Double)]("sumWordFreqForClass").asScala
          for (record <- collection) {
            sumWordFreqForClassMap.put(record._1, record._2.toDouble)
          }

          val collection2 = getRuntimeContext
            .getBroadcastVariable[(String, Int, Int)]("docRelated").asScala
          for (record <- collection2) {
            docRelatedMap.put(record._2, record._3.toDouble)
          }
        }

        override def map(value: (String, Int, String, Int, Double)):
          (String, Int, String, Double, Double, Double) = {
          (value._1, value._2, value._3, value._4,
            value._4 * docRelatedMap(value._2) / sumWordFreqForClassMap(value._1),
            value._4 * docRelatedMap(value._2) / sumWordFreqAllClassesSet.get(0))
        }
      }).withBroadcastSet(sumPiForClass, "sumPiForClass")
        .withBroadcastSet(sumPiAllClasses, "sumPiAllClasses")
        .withBroadcastSet(docRelated, "docRelated")
        .withBroadcastSet(sumWordFreqForClass, "sumWordFreqForClass")
        .withBroadcastSet(sumWordFreqAllClasses, "sumWordFreqAllClasses")
      .distinct() // (class -> document ID -> word -> P_i(w_t|d_i) -> P_j(w_t|d_i) -> P(w_t)


      crqModel.writeAsCsv("/Users/jonathanhasenburg/Desktop/nbtemp/crqModel",
        "\n", "\t", WriteMode.OVERWRITE)
*/
    }
  }

  implicit val crqTransform = new TransformDataSetOperation[CRQSelection,
    (String, Int, String), (String, String)] {
    /**
     * Uses the [[CRQSelection.crqModel]] to transform an input data set into a data set in the
     * format that is required by the [[MultinomialNaiveBayes]] to fit.
     * @param instance of [[CRQSelection]]
     * @param transformParameters to configure
     * @param input, the input data set that is transformed
     * @return
     */
    override def transformDataSet(instance: CRQSelection,
                                  transformParameters: ParameterMap,
                                  input: DataSet[(String, Int, String)]):
                                    DataSet[(String, String)] = {

      //Create a data set that splits the documents in their words
      // 1. Map: Split documents into its words and add a 1 to each tuple
      // 2. Group-Reduce: sum the 1s by class and document id
      val words: DataSet[(String, Int, String, Int)] = input.flatMap(new SingleWordSplitter)
        .groupBy(0, 1, 2)
        .sum(3) // (class -> document id -> word -> word frequency)

      //Calculate CRQ Score for every word
       // 1. Map: Calculate P(w_t)
      //val crq = words.
      null

    }
  }

  implicit val crqTransformNoClassLabel = new TransformDataSetOperation[CRQSelection,
    (Int, String), (Int, String)] {
    /**
     * This is a data set without class label information, so no transformation is applied
     * @param instance of [[CRQSelection]]
     * @param transformParameters to configure
     * @param input, the input data set
     * @return the input data set
     */
    override def transformDataSet(instance: CRQSelection,
                                  transformParameters: ParameterMap,
                                  input: DataSet[(Int, String)]): DataSet[(Int, String)] = {
      input
    }
  }



  /*
* ************************************************************************************************
* *******************************************Function Classes*************************************
* ************************************************************************************************
 */

  /**
   * Transforms a (String, Int, String) tuple into a (String, Int, String, Int)) tuple.
   * The third element from the input gets split into its words, for each word a tuple is collected
   * with the Int 1.
   */
  class SingleWordSplitter() extends FlatMapFunction[(String, Int, String),
    (String, Int, String, Int)] {
    override def flatMap(value: (String, Int, String),
                         out: Collector[(String, Int, String, Int)]): Unit = {
      for (token: String <- value._3.split(" ")) {
        out.collect((value._1, value._2, token, 1))
      }
    }
  }

}
