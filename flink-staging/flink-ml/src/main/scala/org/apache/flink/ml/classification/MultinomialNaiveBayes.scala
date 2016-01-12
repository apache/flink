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

import java.{lang, util}

import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml._
import org.apache.flink.ml.common.{ParameterMap, Parameter}
import org.apache.flink.ml.pipeline.{PredictDataSetOperation, FitOperation, Predictor}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

/**
  * While building the model different approaches need to be compared.
  * For that purpose the fitParameters are used. Each "possibility" that might enhance the
  * performance and each improvement proposed by other authors that might increase the accuracy can
  * be activated separately by using the following list of parameters:
  *
  * Possibility 1 (P1): way of calculating document count
  *  P1 = 0 -> use .count() to get count of all documents
  *  P1 = 1 -> use a reducer and a mapper to create a broadcast data set containing the count of
  *    all documents
  *
  * Possibility 2 (P2): all words in class (order of operators)
  *    If p2 = 1 improves the speed, many other calculations must switch their operators, too.
  *  P2 = 0 -> first the reducer, than the mapper
  *  P2 = 1 -> first the mapper, than the reducer
  *
  * Possibility 3 (P3): way of calculating pwc
  *  P2 = 0 -> join singleWordsInClass and allWordsInClass to wordsInClass data set
  *  P3 = 1 -> work on singleWordsInClass data set and broadcast allWordsInClass data set
  *
  * Schneider/Rennie 1 (SR1): ignore/reduce word frequency impact
  *  SR1 = 0 -> word frequency information is not ignored
  *  SR1 = 1 -> word frequency information is ignored (Schneiders approach)
  *  SR1 = 2 -> word frequency information is reduced (Rennies approach)
  *
  * Schneider 1 (S1): ignore P(c_j) in cMAP formula
  *  S1 = 0 -> normal cMAP formula
  *  S2 = 1 -> cMAP without P(c_j)
  *
  * Rennie 1 (R1): transform document frequency
  *  R1 = 0 -> normal formula
  *  R1 = 1 -> apply inverse document frequecy
  * Note: if R1 = 1 and SR1 = 2, both approaches get applied.
  *
  * It is recommended to set P1, P2 and P3 to 1 for all data sets except massive ones to achieve the
  * highestperformance.
  * The improvements proposed by other authors are not always benefitial, so small test should be
  * performed. For a detailed evaluation see "Design, Implementation and Evaluation of Naive Bayes
  * Classification for the Apache Flink Big Data Analytics Platform" by Jonathan Hasenburg.
  *
  * The Schneider improvements are gathered from:
  *   Schneider, K.-M. (2005). Techniques for Improving the Performance of Naive Bayes for Text
  *   Classification. In Computational Linguistics and Intelligent Text Processing, pages 682–693.
  *   Springer.
  *
  * The Rennie improvements are gathered from:
  *   Rennie, J. D., Shih, L., Teevan, J., Karger, D. R., et al. (2003). Tackling the Poor
  *   Assumptions of Naive Bayes Text Classifiers. In ICML, volume 3, pages 616–623.Washington DC).
 */
class MultinomialNaiveBayes extends Predictor[MultinomialNaiveBayes] {

  import MultinomialNaiveBayes._

  //The model, that stores all needed information that are related to one specific word
  //(class name -> word -> log P(w|c))
  var wordRelatedModelData: Option[DataSet[(String, String, Double)]] = None

  //The model, that stores all needed information that are related to one specifc class
  //(class name -> p(c) -> log p(w|c) not in class)
  var classRelatedModelData: Option[DataSet[(String, Double, Double)]] = None

  //A data set that stores additional needed information for some of the improvements
  //(word -> log number of documents in all classes / word frequency in all classes
  var improvementData: Option[DataSet[(String, Double)]] = None

  // ============================== Parameter configuration ========================================

  /**
   * Activate/Deactivate Possibilty 1
   * @param value, 0 or 1
   * @return itself
   */
  def setP1(value: Int): MultinomialNaiveBayes = {
    parameters.add(P1, value)
    this
  }

  /**
   * Activate/Deactivate Possibilty 2
   * @param value, 0 or 1
   * @return itself
   */
  def setP2(value: Int): MultinomialNaiveBayes = {
    parameters.add(P2, value)
    this
  }

  /**
   * Activate/Deactivate Possibilty 3
   * @param value, 0 or 1
   * @return itself
   */
  def setP3(value: Int): MultinomialNaiveBayes = {
    parameters.add(P3, value)
    this
  }

  /**
   * Activate/Deactivate Schneider/Rennie 1
   * @param value, 0, 1 or 2
   * @return itself
   */
  def setSR1(value: Int): MultinomialNaiveBayes = {
    parameters.add(SR1, value)
    this
  }

  /**
   * Activate/Deactivate Schneider 1
   * @param value, 0 or 1
   * @return itself
   */
  def setS1(value: Int): MultinomialNaiveBayes = {
    parameters.add(S1, value)
    this
  }

  /**
   * Activate/Deactivate Rennie 1
   * @param value, 0 or 1
   * @return itself
   */
  def setR1(value: Int): MultinomialNaiveBayes = {
    parameters.add(R1, value)
    this
  }

  // =============================================== Methods =======================================

  /**
   * Save already existing model data created by the NaiveBayes algorithm. Requires the designated
   * locations. The saved data is a representation of the [[wordRelatedModelData]] and
   * [[classRelatedModelData]].
   * @param wordRelated, the save location for the wordRelated data
   * @param classRelated, the save location for the classRelated data
   */
  def saveModelDataSet(wordRelated: String, classRelated: String) : Unit = {
    wordRelatedModelData.get.writeAsCsv(wordRelated, "\n", "|", WriteMode.OVERWRITE)
    classRelatedModelData.get.writeAsCsv(classRelated, "\n", "|", WriteMode.OVERWRITE)
  }

  /**
   * Save the improvment data set. Requires the designated save location. The saved data is a
   * representation of the [[improvementData]] data set.
   * @param path, the save location for the improvment data
   */
  def saveImprovementDataSet(path: String) : Unit = {
    improvementData.get.writeAsCsv(path, "\n", "|", WriteMode.OVERWRITE)
  }

  /**
   * Sets the [[wordRelatedModelData]] and the [[classRelatedModelData]] to the given data sets.
   * @param wordRelated, the data set representing the wordRelated model
   * @param classRelated, the data set representing the classRelated model
   */
  def setModelDataSet(
      wordRelated : DataSet[(String, String, Double)],
      classRelated: DataSet[(String, Double, Double)])
    : Unit = {
    this.wordRelatedModelData = Some(wordRelated)
    this.classRelatedModelData = Some(classRelated)
  }

  def setImprovementDataSet(impSet : DataSet[(String, Double)]) : Unit = {
    this.improvementData = Some(impSet)
  }

}

object MultinomialNaiveBayes {

  // ========================================== Parameters =========================================
  // TODO code comments
  case object P1 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(0)
  }

  case object P2 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(0)
  }

  case object P3 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(0)
  }

  case object SR1 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(0)
  }

  case object S1 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(0)
  }

  case object R1 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(0)
  }

  // ======================================== Factory Methods ======================================
  def apply(): MultinomialNaiveBayes = {
    new MultinomialNaiveBayes()
  }

  // ====================================== Operations =============================================
  /**
   * Trains the models to fit the training data. The resulting
   * [[MultinomialNaiveBayes.wordRelatedModelData]] and
   * [[MultinomialNaiveBayes.classRelatedModelData]] are stored in the [[MultinomialNaiveBayes]]
   * instance.
   */

  implicit val fitNNB = new FitOperation[MultinomialNaiveBayes, (String, String)] {
    /**
     * The [[FitOperation]] used to create the model. Requires an instance of
     * [[MultinomialNaiveBayes]], a [[ParameterMap]] and the input data set. This data set
     * maps (string -> string) containing (label -> text, words separated by ",")
     * @param instance of [[MultinomialNaiveBayes]]
     * @param fitParameters, additional parameters
     * @param input, the to processed data set
     */
    override def fit(
        instance: MultinomialNaiveBayes,
        fitParameters: ParameterMap,
        input: DataSet[(String, String)])
      : Unit = {

      val resultingParameters = instance.parameters ++ fitParameters

      //Count the amount of documents for each class.
      // 1. Map: replace the document text by a 1
      // 2. Group-Reduce: sum the 1s by class
      val documentsPerClass: DataSet[(String, Int)] = input.map { input => (input._1, 1)}
        .groupBy(0).sum(1) // (class name -> count of documents)

      //Count the amount of occurrences of each word for each class.
      // 1. FlatMap: split the document into its words and add a 1 to each tuple
      // 2. Group-Reduce: sum the 1s by class, word
      var singleWordsInClass: DataSet[(String, String, Int)] = input
        .flatMap(new SingleWordSplitter())
        .groupBy(0, 1).sum(2) // (class name -> word -> count of that word)

      //POSSIBILITY 2: all words in class (order of operators)
      //SCHNEIDER/RENNIE 1: ignore/reduce word frequency information
        //the allWordsInClass data set does only contain distinct
        //words for schneiders approach: ndw(cj), nothing changes for rennies approach
      val p2 = resultingParameters(P2)
      val sr1 = resultingParameters(SR1)

      // (class name -> count of all words in that class)
      var allWordsInClass: DataSet[(String, Int)] = null

      if (p2 == 0) {
        if (sr1 == 0 || sr1 == 2) {
          //Count all the words for each class.
          // 1. Reduce: add the count for each word in a class together
          // 2. Map: remove the field that contains the word
          allWordsInClass = singleWordsInClass.groupBy(0).reduce {
            (singleWords1, singleWords2) =>
              (singleWords1._1, singleWords1._2, singleWords1._3 + singleWords2._3)
          }.map(singleWords =>
            (singleWords._1, singleWords._3)) // (class name -> count of all words in that class)
        } else if (sr1 == 1) {
          //Count all distinct words for each class.
          // 1. Map: set the word count to 1
          // 2. Reduce: add the count for each word in a class together
          // 3. Map: remove the field that contains the word
          allWordsInClass = singleWordsInClass
            .map(singleWords => (singleWords._1, singleWords._2, 1))
            .groupBy(0).reduce {
            (singleWords1, singleWords2) =>
              (singleWords1._1, singleWords1._2, singleWords1._3 + singleWords2._3)
          }.map(singleWords =>
            (singleWords._1, singleWords._3))//(class name -> count of distinct words in that class)
        }
      } else if (p2 == 1) {
        if (sr1 == 0 || sr1 == 2) {
          //Count all the words for each class.
          // 1. Map: remove the field that contains the word
          // 2. Reduce: add the count for each word in a class together
          allWordsInClass = singleWordsInClass
            .map(singleWords => (singleWords._1, singleWords._3))
            .groupBy(0).reduce {
            (singleWords1, singleWords2) => (singleWords1._1, singleWords1._2 + singleWords2._2)
          } // (class name -> count of all words in that class)
        } else if (sr1 == 1) {
          //Count all distinct words for each class.
          // 1. Map: remove the field that contains the word, set the word count to 1
          // 2. Reduce: add the count for each word in a class together
          allWordsInClass = singleWordsInClass
            .map(singleWords => (singleWords._1, 1))
            .groupBy(0).reduce {
            (singleWords1, singleWords2) => (singleWords1._1, singleWords1._2 + singleWords2._2)
          } // (class name -> count of distinct words in that class)
        }
      }
      //END SCHNEIDER/RENNIE 1
      //END POSSIBILITY 2

      //POSSIBILITY 1: way of calculating document count
      val p1 = resultingParameters(P1)
      var pc: DataSet[(String, Double)] = null // (class name -> P(c) in class)

      if (p1 == 0) {
        val documentsCount: Double = input.count() //count of all documents
        //Calculate P(c)
        // 1. Map: divide count of documents for a class through total count of documents
        pc = documentsPerClass.map(line => (line._1, line._2 / documentsCount))
      } else if (p1 == 1) {
        //Create a data set that contains only one double value: the count of all documents
        // 1. Reduce: At the count of documents together
        // 2. Map: Remove field that contains document identifier
        val documentCount: DataSet[(Double)] = documentsPerClass
          .reduce((line1, line2) => (line1._1, line1._2 + line2._2))
          .map(line => line._2) //(count of all documents)

        //calculate P(c)
        // 1. Map: divide count of documents for a class through total count of documents
        //    (only element in documentCount data set)
        pc = documentsPerClass.map(new RichMapFunction[(String, Int), (String, Double)] {

            var broadcastSet: util.List[Double] = null

            override def open(config: Configuration): Unit = {
              broadcastSet = getRuntimeContext.getBroadcastVariable[Double]("documentCount")
              if (broadcastSet.size() != 1) {
                throw new RuntimeException("The document count data set used by p1 = 1 has the " +
                  "wrong size! Please use p1 = 0 if the problem can not be solved.")
              }
            }

            override def map(value: (String, Int)): (String, Double) = {
              (value._1, value._2 / broadcastSet.get(0))
            }
          }).withBroadcastSet(documentCount, "documentCount")
      }
      //END POSSIBILITY 1

      // (list of all words, but distinct)
      val vocabulary = singleWordsInClass.map(tuple => (tuple._2, 1)).distinct(0)
      // (count of items in vocabulary list)
      val vocabularyCount: Double = vocabulary.count()

      //calculate the P(w|c) value for words, that are not part of a class, needed for smoothing
      // 1. Map: use P(w|c) formula with smoothing with n(c_j, w_t) = 0
      val pwcNotInClass: DataSet[(String, Double)] = allWordsInClass
        .map(line =>
          (line._1, 1 / (line._2 + vocabularyCount))) // (class name -> P(w|c) word not in class)

      //SCHNEIDER/RENNIE 1: ignore/reduce word frequency information
        //The singleWordsInClass data set must be changed before, the calculation of pwc starts for
        //schneider, it needs this form classname -> word -> number of documents containing wt in cj
      if (sr1 == 1) {
        //Calculate the required data set (see above)
        // 1. FlatMap: class -> word -> 1 (one tuple for each document in which this word occurs)
        // 2. Group-Reduce: sum all 1s where the first two fields equal
        // 3. Map: Remove unesseccary count of word and replace with 1
        singleWordsInClass = input
          .flatMap(new SingleDistinctWordSplitter())
          .groupBy(0, 1)
          .reduce((line1, line2) => (line1._1, line1._2, line1._3 + line2._3))
      }
      //END SCHNEIDER/RENNIE 1

      //POSSIBILITY 3: way of calculating pwc
      val p3 = resultingParameters(P3)
      var pwc: DataSet[(String, String, Double)] = null // (class name -> word -> P(w|c))

      if (p3 == 0) {
          //Join the singleWordsInClass data set with the allWordsInClass data set to use the
          //information for the calculation of p(w|c).
          val wordsInClass = singleWordsInClass
            .join(allWordsInClass).where(0).equalTo(0) {
              (single, all) => (single._1, single._2, single._3, all._2)
            } // (class name -> word -> count of that word -> count of all words in that class)

          //calculate the P(w|c) value for each word in each class
          // 1. Map: use normal P(w|c) formula
          pwc = wordsInClass
            .map(line => (line._1, line._2, (line._3 + 1) / (line._4 + vocabularyCount)))

      } else if (p3 == 1) {
        //calculate the P(w|c) value for each word in class
        //  1. Map: use normal P(w|c) formula / use the
        pwc = singleWordsInClass
          .map(new RichMapFunction[(String, String, Int), (String, String, Double)] {

          var broadcastMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

          override def open(config: Configuration): Unit = {
            val collection = getRuntimeContext
              .getBroadcastVariable[(String, Int)]("allWordsInClass").asScala
            for (record <- collection) {
              broadcastMap.put(record._1, record._2)
            }
          }

          override def map(value: (String, String, Int)): (String, String, Double) = {
            (value._1, value._2, (value._3 + 1) / (broadcastMap(value._1) + vocabularyCount))
          }
        }).withBroadcastSet(allWordsInClass, "allWordsInClass")

      }
      //END POSSIBILITY 3

      //stores all the word related information in one data set
      // 1. Map: Caluclate logarithms
      val wordRelatedModelData = pwc.map(line => (line._1, line._2, Math.log(line._3)))

      //store all class related information in one data set
      // 1. Join: P(c) data set and P(w|c) data set not in class and calculate logarithms
      val classRelatedModelData = pc
        .join(pwcNotInClass)
        .where(0).equalTo(0) {
          (line1, line2) => (line1._1, Math.log(line1._2), Math.log(line2._2))
        } // (class name -> log(P(c)) -> log(P(w|c) not in class))

      instance.wordRelatedModelData = Some(wordRelatedModelData)
      instance.classRelatedModelData = Some(classRelatedModelData)

      //RENNIE 1: transform document frequency
        //for this, the improvementData set must be set
        //calculate (word -> log number of documents in all classes / docs with that word)

      val r1 = resultingParameters(R1)

      if (r1 == 1) {
        val totalDocumentCount: DataSet[(Double)] = documentsPerClass
          .reduce((line1, line2) => (line1._1, line1._2 + line2._2))
          .map(line => line._2) // (count of all documents)

        //number of occurences over all documents of all classes
        val wordCountTotal = input
          .flatMap(new SingleDistinctWordSplitter())
          .map(line => (line._2, 1))
          .groupBy(0)
          .reduce((line1, line2) => (line1._1, line1._2 + line2._2))
           // (word -> count of documents with that word)

        val improvementData = wordCountTotal.map(new RichMapFunction[(String, Int),
          (String, Double)] {

          var broadcastSet: util.List[Double] = null

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Double]("totalDocumentCount")
            if (broadcastSet.size() != 1) {
              throw new RuntimeException("The total document count data set used by 11 = 1 has " +
                "the wrong size! Please use r1 = 0 if the problem can not be solved.")
            }
          }

          override def map(value: (String, Int)): (String, Double) = {
            (value._1, Math.log(broadcastSet.get(0) / value._2))
          }
        }).withBroadcastSet(totalDocumentCount, "totalDocumentCount")

        instance.improvementData = Some(improvementData)
      }

    }
  }

  // Model (String, String, Double, Double, Double)
  implicit def predictNNB = new PredictDataSetOperation[
      MultinomialNaiveBayes,
      (Int, String),
      (Int, String)]() {

    val mapInitializer = new BroadcastVariableInitializer[
        (String, Double, Double),
        immutable.Map[String, Double]] {
      def initializeBroadcastVariable(
          data: java.lang.Iterable[(String, Double, Double)])
        : immutable.Map[String, Double] = {
        data.asScala.map(x => (x._1, x._3)).toMap
      }
    }

    override def predictDataSet(
        instance: MultinomialNaiveBayes,
        predictParameters: ParameterMap,
        input: DataSet[(Int, String)])
    : DataSet[(Int, String)] = {

      if (instance.wordRelatedModelData.isEmpty || instance.classRelatedModelData.isEmpty) {
        throw new RuntimeException("The NormalNaiveBayes has not been fitted to the " +
            "data. This is necessary before a prediction on other data can be made.")
      }

      val wordRelatedModelData = instance.wordRelatedModelData.get
      val classRelatedModelData = instance.classRelatedModelData.get

      val resultingParameters = instance.parameters ++ predictParameters

      //split the texts from the input data set into its words
      val words: DataSet[(Int, String)] = input.flatMap {
        pair => pair._2.split(" ").map { word => (pair._1, word)}
      }

      //genreate word counts for each word with a key
      // 1. Map: put a 1 to each key
      // 2. Group-Reduce: group by id and word and sum the 1s
      val wordsAndCount: DataSet[(Int, String, Int)] = words.map(line => (line._1, line._2, 1))
        .groupBy(0, 1).sum(2) // (id -> word -> word count in text)

      //calculate the count of all words for a text identified by its key
      val wordsInText: DataSet[(Int, Int)] = wordsAndCount.map(line => (line._1, line._3))
        .groupBy(0).sum(1) //(id -> all words in text)

      //generate a data set containing all words that are in model for each id, class pair
      // 1. Join: wordRelatedModelData with wordsAndCount on
      //  words (id -> class -> word ->  word count -> log(P(w|c))
      val foundWords: DataSet[(Int, String, String, Int, Double)] = wordRelatedModelData
        .joinWithHuge(wordsAndCount).where(1).equalTo(1) {
        (wordR, wordsAC) => (wordsAC._1, wordR._1, wordsAC._2, wordsAC._3, wordR._3)
      }

      //SCHNEIDER/RENNIE 1: ignore/reduce word frequency information
      //RENNIE 1: transform document frequency

      val sr1 = resultingParameters(SR1)
      val r1 = resultingParameters(R1)
      var improvementData: DataSet[(String, Double)] = null

      if (r1 == 1) {
        //The improvementData data set is needed
        if (instance.improvementData.isEmpty) {
          throw new RuntimeException("R1 = 1, for that additional data is needed, but it was not" +
            "found. Make sure to set R1 = 1 when fitting the training data.")
        }
        improvementData = instance.improvementData.get
      }

      if (sr1 == 1 && r1 == 1) {
        throw new RuntimeException("Parameter sr1 and r1 are both set to 1, which is not allowed.")
      }

      var sumPwcFoundWords: DataSet[(Int, String, Double)] = null

      if (sr1 == 0 && r1 == 0) {
        //calculate sumpwc for found words
        // 1. Map: Remove unneded information from foundWords and calculate the sumP(w|c) for each
        //  word (id -> class -> word count * log(P(w|c))
        // 2. Group-Reduce: on id and class, sum all (word count * log(P(w|c))) results
        sumPwcFoundWords = foundWords
          .map(line => (line._1, line._2, line._4 * line._5))
          .groupBy(0, 1)
          .reduce((line1, line2) =>
            (line1._1, line1._2, line1._3 + line2._3)) //(id -> class -> sum(log(P(w|c))
      } else if (sr1 == 1 && r1 == 0) {
        //same as sr1 == 0, but there is no multiplication with the word counts
        sumPwcFoundWords = foundWords
          .map(line => (line._1, line._2, line._5))
          .groupBy(0, 1)
          .reduce((line1, line2) =>
            (line1._1, line1._2, line1._3 + line2._3)) //(id -> class -> sum(log(P(w|c))
      } else if (sr1 == 2 && r1 == 0) {
        //same es sr1 == 0, but multiplication with log(wordcount + 1)
        sumPwcFoundWords = foundWords
          .map(line => (line._1, line._2, Math.log(line._4 + 1) * line._5))
          .groupBy(0, 1)
          .reduce((line1, line2) =>
            (line1._1, line1._2, line1._3 + line2._3)) //(id -> class -> sum(log(P(w|c))
      } else if (sr1 == 0 && r1 == 1) {
        //same as r1 = 0, but the word frequency is multiplied with with log (n_d(c) / n_d(w_t))
        //for that a join with the improvementData data set must be performed first to get the
        //needed additional information.
        // Join: (id -> class ->  word count * log P(w|c) * log (number of documents in class /
                                                            // word frequency in all classes)
        sumPwcFoundWords = foundWords
          .joinWithTiny(improvementData).where(2).equalTo(0) {
            (found, imp) => (found._1, found._2, found._4 * found._5 * imp._2)
          }
          .groupBy(0, 1).reduce((line1, line2) =>
            (line1._1, line1._2, line1._3 + line2._3)) //(id -> class -> sum(log(P(w|c)) */
      } else if (sr1 == 2 && r1 == 1) {
        //combination of r1 = 1 and sr1 =2
        sumPwcFoundWords = foundWords
          .joinWithTiny(improvementData).where(2).equalTo(0) {
            (found, imp) => (found._1, found._2, Math.log(found._4 + 1) * found._5 * imp._2)
          }
          .groupBy(0, 1)
          .reduce((line1, line2) =>
            (line1._1, line1._2, line1._3 + line2._3)) //(id -> class -> sum(log(P(w|c)) */
      }

      var sumPwcNotFoundWords: DataSet[(Int, String, Double)] = null

      if (sr1 == 0 && r1 == 0) {
        //calculate sumwpc for words that are not in model in that class
        // 1. Map: Discard word and log(P(w|c) from foundWords
        // 2. Group-Reduce: calculate sum count of found words for each document,
        //  class pair (id -> class -> sum(wordCount))
        // 3. Join: with wordsInText on id, to get the count of all words per document
        // 4. Map: calculate sumPWcNotFound (id -> class ->
        //  (all words in document - found word in document) *
        //  log(P(w|c) not in class (provided by broadcast))
        sumPwcNotFoundWords = foundWords
          .map(line => (line._1, line._2, line._4))
          .groupBy(0, 1)
          .reduce((line1, line2) => (line1._1, line1._2, line1._3 + line2._3))
          .join(wordsInText).where(0).equalTo(0) {
          (foundW, wordsIT) => (foundW._1, foundW._2, foundW._3, wordsIT._2)
        }.mapWithBcInitializer(classRelatedModelData)(mapInitializer){
          (map, value) =>
            (value._1, value._2, (value._4 - value._3) * map(value._2))
        }
      } else {
        //If the word frequency is changed (as in SR1 = 1, SR1 = 2, R1 = 1), the
        //sumPwcNotFoundWords can not be calculated as above. They must be calculated the same way
        //as for the sumPwcFoundWords data set (because each word frequency for each word is
        //important).
        //
        //Prepare foundWords data set for differenz
        // 1. Map: discard log(P(w|c) and wordCount from foundWords (id -> class -> word)
        //        = all words that are known in each document for each class
        //
        //Prepare wordsAndCounts data set for difference
        // 1. FlatMap:create for every tuple a (document id -> class -> word -> word count)
        //    tuple for every class
        //
        //Create notFoundWords data set
        // 1: CoGroup: Only tuples (id, class, word) that are not in preparedFoundWords
        //
        //The result is a data set, that contains all words for each document for each class
        //that are not part of that class and the word counts.
        //
        //Then calcualte sumPwcNotfoundWords

        val preparedFoundWords: DataSet[(Int, String, String)] = foundWords
          .map(line => (line._1, line._2, line._3))

        val wordsAndCountsExtended: DataSet[(Int, String, String, Int)] = wordsAndCount
          .flatMap(new Extender())
          .withBroadcastSet(classRelatedModelData, "classRelatedModelData")

        val notFoundWords: DataSet[(Int, String, String, Int)] = wordsAndCountsExtended
          .coGroup(preparedFoundWords).where(0, 1, 2)
          .equalTo(0, 1, 2)(new DifferenceCoGrouper) //(id -> class -> word -> word count)

        if ((sr1 == 1 && r1 == 0) || (sr1 == 2 && r1 == 0)) {
          //calculate the sum of all Pwc for every not found word (id -> class -> sumPwcNotFound)
          // 1. Map: calculate the pwc value for every word (id -> class -> pwc)
          // 2. Sum: Sum these pwc values for each class and document
          sumPwcNotFoundWords = notFoundWords
            .map(new RichMapFunction[(Int, String, String, Int), (Int, String, Double)] {

            var broadcastMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
            //class -> log(P(w|c) not found word in class)

            override def open(config: Configuration): Unit = {
              val collection = getRuntimeContext
                .getBroadcastVariable[(String, Double, Double)]("classRelatedModelData")
                .asScala
              for (record <- collection) {
                broadcastMap.put(record._1, record._3)
              }
            }

            override def map(value: (Int, String, String, Int)): (Int, String, Double) = {
              if (sr1 == 1 && r1 == 0) {
                //same as sr1 == 0, but there is no multiplication with the word counts
                return (value._1, value._2, broadcastMap(value._2))
              } else if (sr1 == 2 && r1 == 0) {
                //same es sr1 == 0, but multiplication with log(wordcount + 1)
                return (value._1, value._2, Math.log(value._4 + 1) * broadcastMap(value._2))
              }
              throw new RuntimeException("sumPwcNotFound could not be calculated because you" +
                "choosed a not allowed parameter combination.")
            }
          }).withBroadcastSet(classRelatedModelData, "classRelatedModelData")
            .groupBy(0, 1).sum(2)
        } else if ((sr1 == 0 && r1 == 1) || (sr1 == 2 && r1 == 1)) {

          //same as r1 = 0, but the word frequency is multiplied with
          //log (n_d(c) / n_d(w_t)) for that a join with the improvementData data set must be
          //performed first to get the needed additional information.
          // 1. Join with improvement data: (id -> class ->  word count ->
            // log (number of documents in class / word frequency in all classes))
          // 2a. Map: (id -> class -> word count * log (see above) * log(P(w|c))
          // 2b. Map: (id -> class -> log(word count + 1) * log (see above) * log(P(w|c))
          sumPwcNotFoundWords = notFoundWords
            .join(improvementData).where(2).equalTo(0) {
            (nf, imp) => (nf._1, nf._2, nf._4, imp._2)
          }.map(new RichMapFunction[(Int, String, Int, Double), (Int, String, Double)] {

            var broadcastMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
            //(class -> log(P(w|c) not found word in class)

            override def open(config: Configuration): Unit = {
              val collection = getRuntimeContext
                .getBroadcastVariable[(String, Double, Double)]("classRelatedModelData")
                .asScala
              for (record <- collection) {
                broadcastMap.put(record._1, record._3)
              }
            }

            override def map(value: (Int, String, Int, Double)): (Int, String, Double) = {
              if (sr1 == 0 && r1 == 1) {
                return (value._1, value._2, value._3 * value._4 * broadcastMap(value._2))
              } else if (sr1 == 2 && r1 == 1) {
                return (value._1, value._2, Math.log(value._3 + 1)
                  * value._4 * broadcastMap(value._2))
              }
              throw new RuntimeException("sumPwcNotFound could not be calculated because you" +
                "choosed a not allowed parameter combination.")
            }
          }).withBroadcastSet(classRelatedModelData, "classRelatedModelData")
            .groupBy(0, 1).sum(2) //(id -> class -> sum(log(P(w|c))
        }
      }

      var sumPwc: DataSet[(Int, String, Double)] = null

      if (sr1 == 0 && r1 == 0) {
        //sum those pwc sums
        // 1. Join sumPwcFoundWords and sumPwcNotFoundWords
        // 2. Map: add sums from found words and sums from not found words
        sumPwc = sumPwcFoundWords
          .join(sumPwcNotFoundWords).where(0, 1).equalTo(0, 1) {
          (found, notfound) => (found._1, found._2, found._3 + notfound._3)
        } //(id -> class name -> sum log(p(w|c)))
      } else {
        //sum those pwc sums
        // 1. CoGroup Found with NotFound on document id
        // add values together, when both have a same class
        // submit tuple, when a class misses for the other one
        sumPwc = sumPwcFoundWords.coGroup(sumPwcNotFoundWords)
          .where(0).equalTo(0) {
          (foundVs, notFoundVs, out: Collector[(Int, String, Double)]) =>
            //1. Create two Maps
            var foundMap = Map[String, Double]()
            var notFoundMap = Map[String, Double]()

            var currentID = -1

            for (foundV <- foundVs) {
              if (currentID == -1) {
                currentID = foundV._1
              }
              foundMap += foundV._2 -> foundV._3
            }
            for (notFoundV <- notFoundVs) {
              notFoundMap += notFoundV._2 -> notFoundV._3
            }
            //2. Go through these maps and collect tuples
            for (tuple <- foundMap) {
              if (notFoundMap.contains(tuple._1)) {
                out.collect(currentID, tuple._1, tuple._2 + notFoundMap(tuple._1))
                notFoundMap.remove(tuple._1)
              } else {
                out.collect(currentID, tuple._1, tuple._2)
              }
            }
            for (tuple <- notFoundMap) {
              if (foundMap.contains(tuple._1)) {
                out.collect(currentID, tuple._1, tuple._2 + foundMap(tuple._1))
              } else {
                out.collect(currentID, tuple._1, tuple._2)
              }
            }
        }
      }
      //END RENNIE 1
      //END SCHNEIDER/RENNIE 1

      //SCHNEIDER 1: ignore P(c_j) in cMAP formula
      val s1 = resultingParameters(S1)
      var posterior: DataSet[(Int, String, Double)] = null
      if (s1 == 0) {
        //calculate posterior values for each class
        // 1. Map: add sumPwc values with log(P(c)) (provided by broadcast)
        posterior = sumPwc
          .map(new RichMapFunction[(Int, String, Double), (Int, String, Double)] {

          var broadcastMap: mutable.Map[String, Double] =
            mutable.Map[String, Double]() //class -> log(P(c))

          override def open(config: Configuration): Unit = {
            val collection = getRuntimeContext
              .getBroadcastVariable[(String, Double, Double)]("classRelatedModelData")
              .asScala
            for (record <- collection) {
              broadcastMap.put(record._1, record._2)
            }
          }

          override def map(value: (Int, String, Double)): (Int, String, Double) = {
            (value._1, value._2, value._3 + broadcastMap(value._2))
          }
        }).withBroadcastSet(classRelatedModelData, "classRelatedModelData")
      } else if (s1 == 1) {
        posterior = sumPwc
      }
      //choose the highest probable class
      // 1. Reduce: keep only highest probability
      posterior.groupBy(0).reduce(new CalculateReducer())
        .map(line => (line._1, line._2)) //(id -> class)


    }
  }


  /*
  * ************************************************************************************************
  * *******************************************Function Classes*************************************
  * ************************************************************************************************
   */

  /**
   * Transforms a (String, String) tuple into a (String, String, Int)) tuple.
   * The second string from the input gets split into its words, for each word a tuple is collected
   * with the Int 1.
   */
  class SingleWordSplitter() extends FlatMapFunction[(String, String), (String, String, Int)] {
    override def flatMap(value: (String, String), out: Collector[(String, String, Int)]): Unit = {
      for (token: String <- value._2.split(" ")) {
        out.collect((value._1, token, 1))
      }
    }

    // Can be implemented via: input.flatMap{ pair => pair._2.split(" ")
    // .map{ token => (pair._1, token ,1)} }
  }

  /**
   * Transforms a (String, String) tuple into a (String, String, Int)) tuple.
   * The second string from the input gets split into its words, for each distinct word a tuple
   * is collected with the Int 1.
   */
  class SingleDistinctWordSplitter() extends FlatMapFunction[
    (String, String),
    (String, String, Int)] {
    override def flatMap(value: (String, String), out: Collector[(String, String, Int)]): Unit = {
      var x: ListBuffer[String] = ListBuffer()
      for (token: String <- value._2.split(" ")) {
        x.append(token)
      }
      x = x.distinct
      for (item <- x) {
        out.collect((value._1, item, 1))
      }
    }
  }

  /**
   * Used to extend the wordsAndCount data set. Transforms (id -> word -> word count) into
   * (id -> class -> word -> word count) for each possible class.
   */
  class Extender() extends RichFlatMapFunction[(Int, String, Int), (Int, String, String, Int)] {

    var broadcastSet = Set[String]()

    override def open(config: Configuration): Unit = {
      val collection = getRuntimeContext
        .getBroadcastVariable[(String, Double, Double)]("classRelatedModelData")
        .asScala
      for (record <- collection) {
        broadcastSet += record._1
      }
    }

    override def flatMap(value: (Int, String, Int), out: Collector[(Int, String, String, Int)]):
      Unit = {
      for (c: String <- broadcastSet) {
        out.collect((value._1, c, value._2, value._3))
      }
    }
  }

  /**
   * Chooses for each label that class with the highest value
   */
  class CalculateReducer() extends ReduceFunction[(Int, String, Double)] {
    override def reduce(
        value1: (Int, String, Double),
        value2: (Int, String, Double))
    : (Int, String, Double) = {
      if (value1._3 > value2._3) {
        value1
      } else {
        value2
      }
    }
  }

  /**
   * Difference of two data sets
   */
  class DifferenceCoGrouper extends CoGroupFunction[
    (Int, String, String, Int),
    (Int, String, String),
    (Int, String, String, Int)] {
    override def coGroup(first: lang.Iterable[(Int, String, String, Int)],
                         second: lang.Iterable[(Int, String, String)],
                         out: Collector[(Int, String, String, Int)]): Unit = {
      if (!second.iterator().hasNext()) {
        out.collect(first.iterator().next())
      }

    }
  }

}
