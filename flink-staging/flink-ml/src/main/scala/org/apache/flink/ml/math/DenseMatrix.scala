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

package org.apache.flink.ml.math

/**
 * Dense matrix implementation of [[Matrix]]. Stores data in column major order in a continuous
 * double array.
 *
 * @param numRows Number of rows
 * @param numCols Number of columns
 * @param values Array of matrix elements in column major order
 */
case class DenseMatrix(val numRows: Int,
                  val numCols: Int,
                  val values: Array[Double]) extends Matrix {

  require(numRows * numCols == values.length, s"The number of values ${values.length} does " +
    s"not correspond to its dimensions ($numRows, $numCols).")

  /**
   * Element wise access function
   *
   * @param row row index
   * @param col column index
   * @return matrix entry at (row, col)
   */
  override def apply(row: Int, col: Int): Double = {
    require(0 <= row && row < numRows, s"Row $row is out of bounds [0, $numRows).")
    require(0 <= col && col < numCols, s"Col $col is out of bounds [0, $numCols).")

    val index = col * numRows + row

    values(index)
  }

  override def toString: String = {
    s"DenseMatrix($numRows, $numCols, ${values.mkString(", ")})"
  }

}

object DenseMatrix {

  def apply(numRows: Int, numCols: Int, values: Array[Int]): DenseMatrix = {
    new DenseMatrix(numRows, numCols, values.map(_.toDouble))
  }

  def apply(numRows: Int, numCols: Int, values: Double*): DenseMatrix = {
    new DenseMatrix(numRows, numCols, values.toArray)
  }

  def zeros(numRows: Int, numCols: Int): DenseMatrix = {
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(0.0))
  }

  def eye(numRows: Int, numCols: Int): DenseMatrix = {
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(1.0))
  }
}
