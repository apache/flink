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

package eu.enx.flink.dfwssp.utils

import breeze.linalg.{DenseVector, VectorBuilder, normalize}
import breeze.stats.distributions.Gaussian
import eu.enx.flink.dfwssp.model.ColumnVector
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._

/**
 * Sample data generator for Frank Wolfe Lasso Regression
 */

object DataGenerator {

  def columnGenerator(dim: Int, num: Int, dis: String): Stream[ColumnVector] = {
    var gen: Stream[ColumnVector] = null
    dis match {
      case "uniform" =>
        gen = Stream.range(0, num).map {
          i => ColumnVector(i, normalize(DenseVector.rand[Double](dim)).toArray)
        }
      case "gaussian" =>
        gen = Stream.range(0, num).map {
          i => {
            val g = Gaussian(i % dim, 1)
            val h = DenseVector((0 until dim).map(i => g.probability(i, i + 1)).toArray)
            ColumnVector(i, normalize(h).toArray)
          }
        }
    }
    gen
  }

  def loadSparseBinaryMatrix(env: ExecutionEnvironment,
                             filename: String,
                             dimension: Int,
    ignoreFirstLine: Boolean = true):
  DataSet[ColumnVector] = {

    val csv = env.readCsvFile[(Int, String)](filename, ignoreFirstLine = true)
    val bagOfEntry = csv.flatMap {
      tuple => {
        val id = tuple._1
        val nonZeroCols = tuple._2.split(" ").map(x => x.toInt)
        List.fill(nonZeroCols.length)(id).zip(nonZeroCols)
      }
    }.groupBy(1)

    val cols = bagOfEntry.reduceGroup(
      iterator => {
        val s = iterator.toSeq
        val id = s(0)._1
        val indices = s.map { case (row, col) => col }
        val v = new VectorBuilder[Double](dimension)
        for (colIndex <- indices) {
          v.add(colIndex, 1)
        }
        ColumnVector(id, v.toDenseVector.toArray)
      })

    cols
  }

  def signalGenerator(colVec: DataSet[ColumnVector],
                      noise: Double,
                      coeff: DataSet[SparseEntry]): DataSet[Array[Double]] = {
    val joinedDataSet = colVec.joinWithTiny(coeff)
                              .where("idx")
                              .equalTo("index")
    val ret = joinedDataSet.map {
      new ElementWiseMul
    }.reduce {
      (left, right) => (DenseVector(left) + DenseVector(right)).toArray
    }
    ret.map {
      x => normalize(DenseVector(x) + DenseVector.rand[Double](x.length) *= noise).toArray
    }
  }

  def sparseEntryGenerator(indices: Array[Int], coeff: Array[Double]): Array[SparseEntry] = {
    indices.zip(coeff).map(x => SparseEntry(x._1, x._2))
  }

  case class SparseEntry(index: Int, value: Double)

  case class SparseBinaryData(id: Int, values: Array[Int]) {
    override def toString: String = {
      val l = values.length
      val s = if (l <= 10) {
        "SparseBinaryVector(" + id + ", [" + values.mkString(" ") + "])"
      } else {
        "SparseBinaryVector(" + id + ", [" + values.slice(0, 3).mkString(" ") + " ... " +
          values.slice(l - 3, l).mkString(" ") + "])"
      }
      s
    }
  }

  class ElementWiseMul extends RichMapFunction[(ColumnVector, SparseEntry), Array[Double]] {
    override def map(value: (ColumnVector, SparseEntry)): Array[Double] = {
      (DenseVector(value._1.values) *= value._2.value).toArray
    }
  }

}
