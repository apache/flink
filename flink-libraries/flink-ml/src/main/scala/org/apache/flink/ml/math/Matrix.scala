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

/** Base trait for a matrix representation
  *
  */
trait Matrix {

  /** Number of rows
    *
    * @return number of rows in the matrix
    */
  def numRows: Int

  /** Number of columns
    *
    * @return number of columns in the matrix
    */
  def numCols: Int

  /** Element wise access function
    *
    * @param row row index
    * @param col column index
    * @return matrix entry at (row, col)
    */
  def apply(row: Int, col: Int): Double

  /** Element wise update function
    *
    * @param row row index
    * @param col column index
    * @param value value to set at (row, col)
    */
  def update(row: Int, col: Int, value: Double): Unit

  /** Copies the matrix instance
    *
    * @return Copy of itself
    */
  def copy: Matrix

  def equalsMatrix(matrix: Matrix): Boolean = {
    if(numRows == matrix.numRows && numCols == matrix.numCols) {
      val coordinates = for(row <- 0 until numRows; col <- 0 until numCols) yield (row, col)
      coordinates forall { case(row, col) => this.apply(row, col) == matrix(row, col)}
    } else {
      false
    }
  }

}
