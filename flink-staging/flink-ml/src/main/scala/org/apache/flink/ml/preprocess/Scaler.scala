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

package org.apache.flink.ml.preprocess

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.Vector

/** Scales observations, so that all features have mean equal to zero and standard deviation equal to one
  *
  * This transformer takes a a DenseMatrix of values and maps it into the
  * scaled DenseMatrix that each feature has mean zero and standard deviation equal to one.
  */
class Scaler extends Transformer[Vector, Vector] with Serializable {

  override def transform(input: DataSet[Vector], parameters: ParameterMap):
  DataSet[Vector] = {
    input.map {
      matrix => {
        scaleMatrix(matrix)
      }
    }
  }

  /** Calculates the mean of each feature
    *
    * @param matrix
    */
  private def scaleMatrix(matrix: Vector):Vector = {
    val m = mean(matrix)
    val std = standardDeviation(matrix,m)
    val scaledMatrix = matrix.copy

    for(cols <- 0 until matrix.numCols){
      for(rows <- 0 until matrix.numRows){
        val temp = (matrix.apply(rows,cols) - m(cols))/std(cols)
        scaledMatrix.update(rows,cols,temp)
      }
    }
    return scaledMatrix
  }

  /** Calculates the mean of each feature
    *
    * @param matrix
    */
  private def mean(matrix: Vector): Array[Double] = {
    val m = new Array[Double](matrix.numCols)

    for (cols <- 0 until matrix.numCols){
      var temp = 0.0
      for (rows <- 0 until matrix.numRows) {
        temp = temp + matrix.apply(rows, cols)
      }
      m.update(cols,temp)
    }
    return m
  }

  /** Calculates the standard deviation of each feature
    * σ = Σ(x_i-mean)^^2 /n
    *
    * @param matrix
    * @param m
    */
  private def standardDeviation(matrix: Vector, m:Array[Double]): Array[Double] = {
    val std = new Array[Double](matrix.numCols)

    for (cols <- 0 until matrix.numCols){
      var temp = 0.0
      for (rows <- 0 until matrix.numRows) {
        temp = temp + math.pow(matrix.apply(rows, cols) - m.apply(cols),2)
      }
      std.update(cols,temp/matrix.numRows)
    }
    return std
  }

}

object Scaler{
  case object WithMean extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(true)
  }
  case object WithStd extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(true)
  }
}
