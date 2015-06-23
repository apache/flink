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

package org.apache.flink.ml

import org.apache.flink.api.common.functions.{RichMapPartitionFunction, RichMapFunction}
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.ml.statistics.{DataStats, DiscreteHistogram, ContinuousHistogram}
import org.apache.flink.util.Collector

/** Convenience functions for machine learning tasks
  *
  * This object contains convenience functions for machine learning tasks:
  *
  * - readLibSVM:
  *   Reads a libSVM/SVMLight input file and returns a data set of [[LabeledVector]].
  *   The file format is specified [http://svmlight.joachims.org/ here].
  *
  * - writeLibSVM:
  *   Writes a data set of [[LabeledVector]] in libSVM/SVMLight format to disk. THe file format
  *   is specified [http://svmlight.joachims.org/ here].
  */
object MLUtils {

  val DIMENSION = "dimension"

  val HISTOGRAM_STATS = "MIN_MAX_HISTOGRAM_STATS"

  /** Reads a file in libSVM/SVMLight format and converts the data into a data set of
    * [[LabeledVector]]. The dimension of the [[LabeledVector]] is determined automatically.
    *
    * Since the libSVM/SVMLight format stores a vector in its sparse form, the [[LabeledVector]]
    * will also be instantiated with a [[SparseVector]].
    *
    * @param env executionEnvironment [[ExecutionEnvironment]]
    * @param filePath Path to the input file
    * @return [[DataSet]] of [[LabeledVector]] containing the information of the libSVM/SVMLight
    *        file
    */
  def readLibSVM(env: ExecutionEnvironment, filePath: String): DataSet[LabeledVector] = {
    val labelCOODS = env.readTextFile(filePath).flatMap {
      line =>
        // remove all comments which start with a '#'
        val commentFreeLine = line.takeWhile(_ != '#').trim

        if(commentFreeLine.nonEmpty) {
          val splits = commentFreeLine.split(' ')
          val label = splits.head.toDouble
          val sparseFeatures = splits.tail
          val coos = sparseFeatures.map {
            str =>
              val pair = str.split(':')
              require(pair.length == 2, "Each feature entry has to have the form <feature>:<value>")

              // libSVM index is 1-based, but we expect it to be 0-based
              val index = pair(0).toInt - 1
              val value = pair(1).toDouble

              (index, value)
          }

          Some((label, coos))
        } else {
          None
        }
    }

    // Calculate maximum dimension of vectors
    val dimensionDS = labelCOODS.map {
      labelCOO =>
        labelCOO._2.map( _._1 + 1 ).max
    }.reduce(scala.math.max(_, _))

    labelCOODS.map{ new RichMapFunction[(Double, Array[(Int, Double)]), LabeledVector] {
      var dimension = 0

      override def open(configuration: Configuration): Unit = {
        dimension = getRuntimeContext.getBroadcastVariable(DIMENSION).get(0)
      }

      override def map(value: (Double, Array[(Int, Double)])): LabeledVector = {
        new LabeledVector(value._1, SparseVector.fromCOO(dimension, value._2))
      }
    }}.withBroadcastSet(dimensionDS, DIMENSION)
  }

  /** Writes a [[DataSet]] of [[LabeledVector]] to a file using the libSVM/SVMLight format.
    * 
    * @param filePath Path to output file
    * @param labeledVectors [[DataSet]] of [[LabeledVector]] to write to disk
    * @return
    */
  def writeLibSVM(filePath: String, labeledVectors: DataSet[LabeledVector]): DataSink[String] = {
    val stringRepresentation = labeledVectors.map{
      labeledVector =>
        val vectorStr = labeledVector.vector.
          // remove zero entries
          filter( _._2 != 0).
          map{case (idx, value) => (idx + 1) + ":" + value}.
          mkString(" ")

        labeledVector.label + " " + vectorStr
    }

    stringRepresentation.writeAsText(filePath)
  }

  /** Create a [[ContinuousHistogram]] from the input data
    *
    * @param bins Number of bins required
    * @param data input [[DataSet]] of [[Double]]
    * @return [[ContinuousHistogram]] over the data
    */
  def createContinuousHistogram(data: DataSet[Double], bins: Int): DataSet[ContinuousHistogram] = {
    val stats = DataStats.dataStats(data.map(x => DenseVector(Array(x))))

    data.mapPartition(new RichMapPartitionFunction[Double, ContinuousHistogram] {
      var statistics: (Double, Double) = _

      override def open(configuration: Configuration): Unit = {
        val stats = getRuntimeContext.getBroadcastVariable[DataStats](HISTOGRAM_STATS).get(0)
        val minimum = stats.fields.head.min
        val maximum = stats.fields.head.max
        statistics = (minimum - 2 * (maximum - minimum), maximum + 2 * (maximum - minimum))
      }

      override def mapPartition(
          values: java.lang.Iterable[Double],
          out: Collector[ContinuousHistogram])
        : Unit = {
        val localHistogram = new ContinuousHistogram(bins, statistics._1, statistics._2)
        val iterator = values.iterator()
        while (iterator.hasNext) {
          localHistogram.add(iterator.next())
        }
        out.collect(localHistogram)
      }
    })
      .withBroadcastSet(stats, HISTOGRAM_STATS)
      .reduce((x, y) => x.merge(y, bins))
  }

  /** Create a [[DiscreteHistogram]] from the input data
    *
    * @param data input [[DataSet]] of [[Double]]
    * @return [[DiscreteHistogram]] over the data
    */
  def createDiscreteHistogram(data: DataSet[Double]): DataSet[DiscreteHistogram] = {
    data.mapPartition(new RichMapPartitionFunction[Double, DiscreteHistogram] {
      override def mapPartition(
          values: java.lang.Iterable[Double],
          out: Collector[DiscreteHistogram])
        : Unit = {
        val localHistogram = new DiscreteHistogram(Int.MaxValue)
        val iterator = values.iterator()
        while (iterator.hasNext) {
          localHistogram.add(iterator.next())
        }
        out.collect(localHistogram)
      }
    }).reduce((x, y) => x.merge(y, 0))
  }
}
