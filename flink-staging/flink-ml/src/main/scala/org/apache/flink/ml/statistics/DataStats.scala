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

import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.FieldType._
import org.apache.flink.ml.math.Vector

/**
 * Class for representing statistics for a DataSet[Vector]
 *
 * =Parameters=
 * -[[total]]:
 * Total number of elements in the data set
 *
 * -[[dimension]]:
 * Dimensionality of the data set
 *
 * -[[fields]]:
 * Column wise [[FieldStats]] for every field/attribute in the data
 *
 */
class DataStats(val total: Long, val dimension: Int, val fields: Array[FieldStats])
  extends Serializable

object DataStats {

  /** Evaluate statistics for a [[DataSet]] of [[Vector]]
    *
    * @param data Input data set
    * @param discreteFields Array of indices of discrete valued fields
    * @return statistics for the data set
    */
  def dataStats(data: DataSet[Vector], discreteFields: Array[Int]): DataSet[DataStats] = {
    data.mapPartition(values => {
      if (values.isEmpty) {
        Seq.empty
      } else {
        val firstVec = values.next()
        val dimension = firstVec.size
        val localStats = new StatisticsCollector(discreteFields, dimension)
        localStats.add(firstVec)
        values.foreach(vec => {
          require(vec.size == dimension, "Dimension mismatch error in data.")
          localStats.add(vec)
        })
        Seq(localStats)
      }
    }).reduce(_.merge(_))
      .map(_.getLocalValue)
  }
}

/**
 * Accumulator for aggregating statistics for data
 * Min, max, mean, variance are collected for all [[CONTINUOUS]] fields.
 * Class counts are collected for all [[DISCRETE]] fields.
 *
 */
private final class StatisticsCollector(val categorical: Array[Int], val dimension: Int) {

  private val localFieldStats: Array[FieldStats] = Array.ofDim(dimension)
  private var localTotal: Long = 0

  localFieldStats.indices.dropWhile(categorical.contains(_)).foreach(
    localFieldStats(_) = new FieldStats(CONTINUOUS)
  )
  categorical.foreach(localFieldStats(_) = new FieldStats(DISCRETE))

  /**
   * Adds a new vector to this statistics collector
   *
   * @param V Vector
   */
  def add(V: Vector): Unit = {
    // increment the total
    localTotal += 1

    // update the fields accordingly if they are continuous or discrete.
    localFieldStats.zipWithIndex.foreach(f => {
      val (field, index) = f
      val newVal = V(index)
      field.fieldType match {
        case CONTINUOUS =>
          field.setContinuousParameters(
            math.min(field.min, newVal),
            math.max(field.max, newVal),
            // we directly only maintain sums.
            field.mean + newVal,
            // similarly, only maintain sum of squares.
            field.variance + math.pow(newVal, 2)
          )
        case DISCRETE =>
          field.categoryCounts.get(newVal) match {
            case None => field.categoryCounts.put(newVal, 1)
            case Some(value) => field.categoryCounts.put(newVal, value + 1)
          }
      }
    })
  }

  /**
   * Return the DataStats represented by this Statistics Collector.
   *
   * @return DataStats collected so far
   */
  def getLocalValue: DataStats = {
    // this is where we convert the sums and sum of square to mean and variance.
    new DataStats(
      localTotal,
      dimension,
      localFieldStats.map(field => {
        field.fieldType match {
          case CONTINUOUS =>
            val ret = new FieldStats(CONTINUOUS)
            ret.setContinuousParameters(field._min, field._max, field._mean / localTotal,
              field._variance / localTotal - Math.pow(field._mean / localTotal, 2))
            ret
          case DISCRETE => field
        }
      })
    )
  }

  /**
   * Merge statistics collected by another [[StatisticsCollector]] into this collector
   *
   * @param other Statistics collector to be merged
   * @return itself, after merging
   */
  def merge(other: StatisticsCollector): StatisticsCollector = {
    if (other.localTotal == 0) {
      this
    }
    else if (dimension != other.dimension) {
      throw new RuntimeException("Dimension mismatch error in data")
    }
    else {
      localTotal += other.localTotal

      localFieldStats.zipWithIndex.foreach(field => {
        val (field1, field2) = (field._1, other.localFieldStats.apply(field._2))
        if (field1.fieldType == field2.fieldType) {
          field1.fieldType match {
            case CONTINUOUS =>
              field1.setContinuousParameters(
                math.min(field1.min, field2.min),
                math.max(field1.max, field2.max),
                field1.mean + field2.mean,
                field1.variance + field2.variance
              )
            case DISCRETE =>
              field2.categoryCounts.iterator.foreach(classCount => {
                val (key, count) = classCount
                field1.categoryCounts.get(key) match {
                  case None => field1.categoryCounts.put(key, count)
                  case Some(value) => field1.categoryCounts.put(key, value + count)
                }
              })
          }
        } else {
          throw new RuntimeException("Invalid merge operation on two different Statistics " +
            "Collectors.")
        }
      })
    }
    this
  }
}
