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

package org.apache.flink.ml.statistics

import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.statistics.FieldType._
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import scala.Double.MaxValue
import scala.collection.mutable

/** Class for representing statistics over a DataSet[Vector]
  *
  * =Parameters=
  * -[[total]]:
  * Total number of elements in the data set
  *
  * -[[dimension]]:
  * Dimensionality of the data set
  *
  * -[[fields]]:
  * Array of [[FieldStats]] over each attribute/field of data
  *
  */
class DataStats(val total: Long, val dimension: Int, val fields: Array[FieldStats])
  extends Serializable

object DataStats {

  /** Evaluate statistics for a [[DataSet[Vector]]
    *
    * @param data The data set
    * @return Statistics
    *
    */
  def dataStats(data: DataSet[Vector]): DataSet[DataStats] = {
    dataStats(data, Array.ofDim(0))
  }

  /** Evaluate statistics for a [[DataSet[Vector]] when some of the fields are to considered as
    * discrete.
    *
    * @param data The data set
    * @param discreteFields Array of indices of fields which are discrete valued
    * @return Statistics
    *
    */
  def dataStats(data: DataSet[Vector], discreteFields: Array[Int]): DataSet[DataStats] = {

    data.mapPartition(new RichMapPartitionFunction[Vector, StatisticsCollector] {
      override def mapPartition(
          values: java.lang.Iterable[Vector],
          out: Collector[StatisticsCollector])
        : Unit = {
        val localStats = new StatisticsCollector(discreteFields)
        val iterator = values.iterator()
        while (iterator.hasNext) {
          localStats.add(iterator.next())
        }
        out.collect(localStats)
      }
    }).reduce((x, y) => {
      x.merge(y)
    }).map(x => x.getLocalValue)
  }
}

/** Accumulator for aggregating min, max, mean, variance and class counts (for discrete fields only)
  *
  */
private final class StatisticsCollector(val categorical: Array[Int]) {
  private var localTotal: Long = 0
  private var localDimension: Int = -1
  // the fieldStats will for now contain sum and sum of squares instead of mean and variance
  private var localFieldStats: Array[FieldStats] = Array.ofDim(0)

  def add(V: Vector): Unit = {
    // if this is the first element seen, initialize things according to dimension
    if (localTotal == 0) {
      localDimension = V.size
      localFieldStats = Array.ofDim[FieldStats](localDimension)
      for (i <- 0 to localDimension - 1) {
        if (categorical.contains(i)) {
          localFieldStats(i) = new FieldStats(DISCRETE)
          localFieldStats(i).setDiscreteParameters(new mutable.HashMap[Double, Int]())
        } else {
          localFieldStats(i) = new FieldStats(CONTINUOUS)
          localFieldStats(i).setContinuousParameters(MaxValue, -MaxValue, 0, 0)
        }
      }
    }
    addVec(V)
  }

  def getLocalValue: DataStats = {
    // this is where we actually convert the sums and sum of square to mean and variance.
    new DataStats(
      localTotal,
      localDimension,
      localFieldStats.map(x => {
        if (x.fieldType == CONTINUOUS) {
          val ret = new FieldStats(CONTINUOUS)
          ret.setContinuousParameters(x._min, x._max, x._mean / localTotal,
            x._variance / localTotal - Math.pow(x._mean / localTotal, 2))
          ret
        } else {
          x
        }
      }))
  }

  def merge(stats: StatisticsCollector): StatisticsCollector = {
    val toMerge = stats.getLocalValue
    val mergeWith = getLocalValue
    if (toMerge.total == 0) {
      this
    }
    if (localDimension != toMerge.dimension) {
      throw new RuntimeException("Dimension mismatch error in data")
    }
    val newTotal = localTotal + toMerge.total
    for (i <- 0 to localDimension - 1) {
      val x = (mergeWith.fields(i), toMerge.fields(i))
      if (x._1.fieldType == DISCRETE && x._2.fieldType == DISCRETE) {
        val counts = x._1.categoryCounts.clone()
        x._2.categoryCounts.iterator.foreach(field => {
          if (counts.contains(field._1)) {
            counts.update(field._1, field._2 + counts.get(field._1).get)
          } else {
            counts.put(field._1, field._2)
          }
        })
        localFieldStats(i).setDiscreteParameters(counts)
      } else if (x._1.fieldType == CONTINUOUS && x._2.fieldType == CONTINUOUS) {
        val newMin = Math.min(x._1.min, x._2.min)
        val newMax = Math.max(x._1.max, x._2.max)
        val newSum = x._1.mean * localTotal + x._2.mean * toMerge.total
        val newSumSquare = (x._1.variance + Math.pow(x._1.mean, 2)) * localTotal +
          (x._2.variance + Math.pow(x._2.mean, 2)) * toMerge.total
        // we're keeping sum and sum of squares. Not exact mean and variance!!
        // But not your problem, eh? It's me who had to spent hours debugging this lil mess.
        localFieldStats(i).setContinuousParameters(newMin, newMax, newSum, newSumSquare)
      } else {
        throw new RuntimeException("An unexpected error occurred")
      }
    }
    localTotal = newTotal
    this
  }

  private def addVec(V: Vector): Unit = {
    if (V.size != localDimension) {
      throw new RuntimeException("Dimension mismatch error in data")
    }
    localTotal += 1
    for (i <- 0 to localDimension - 1) {
      if (categorical.contains(i)) {
        if (localFieldStats(i)._counts.get(V(i)).nonEmpty) {
          localFieldStats(i)._counts.update(V(i), localFieldStats(i)._counts.get(V(i)).get + 1)
        } else {
          localFieldStats(i)._counts.put(V(i), 1)
        }
      } else {
        localFieldStats(i)._min = Math.min(localFieldStats(i)._min, V(i))
        localFieldStats(i)._max = Math.max(localFieldStats(i)._max, V(i))
        localFieldStats(i)._mean += V(i)
        localFieldStats(i)._variance += (V(i) * V(i))
      }
    }
  }
}
