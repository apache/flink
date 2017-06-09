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
 * @param data Array of matrix elements in column major order
 */
case class DenseMatrix(numRows: Int, numCols: Int, data: Array[Double])
  extends Matrix with Serializable {

  import DenseMatrix._

  require(numRows * numCols == data.length, s"The number of values ${data.length} does " +
    s"not correspond to its dimensions ($numRows, $numCols).")

  /**
   * Element wise access function
   *
   * @param row row index
   * @param col column index
   * @return matrix entry at (row, col)
   */
  override def apply(row: Int, col: Int): Double = {
    val index = locate(row, col)

    data(index)
  }

  override def toString: String = {
    val result = StringBuilder.newBuilder
    result.append(s"DenseMatrix($numRows, $numCols)\n")

    val columnsFieldWidths = for(row <- 0 until math.min(numRows, MAX_ROWS)) yield {
      var column = 0
      var maxFieldWidth = 0

      while(column * maxFieldWidth < LINE_WIDTH && column < numCols) {
        val fieldWidth = printEntry(row, column).length + 2

        if(fieldWidth > maxFieldWidth) {
          maxFieldWidth = fieldWidth
        }

        if(column * maxFieldWidth < LINE_WIDTH) {
          column += 1
        }
      }

      (column, maxFieldWidth)
    }

    val (columns, fieldWidths) = columnsFieldWidths.unzip

    val maxColumns = columns.min
    val fieldWidth = fieldWidths.max

    for(row <- 0 until math.min(numRows, MAX_ROWS)) {
      for(col <- 0 until maxColumns) {
        val str = printEntry(row, col)

        result.append(" " * (fieldWidth - str.length) + str)
      }

      if(maxColumns < numCols) {
        result.append("...")
      }

      result.append("\n")
    }

    if(numRows > MAX_ROWS) {
      result.append("...\n")
    }

    result.toString()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case dense: DenseMatrix =>
        numRows == dense.numRows && numCols == dense.numCols && data.sameElements(dense.data)
      case _ => false
    }
  }

  override def hashCode: Int = {
    val hashCodes = List(numRows.hashCode(), numCols.hashCode(), java.util.Arrays.hashCode(data))

    hashCodes.foldLeft(3){(left, right) => left * 41 + right}
  }

  /** Element wise update function
    *
    * @param row row index
    * @param col column index
    * @param value value to set at (row, col)
    */
  override def update(row: Int, col: Int, value: Double): Unit = {
    val index = locate(row, col)

    data(index) = value
  }

  /** Converts the DenseMatrix to a SparseMatrix
    *
    * @return SparseMatrix build from all the non-null values
    */
  def toSparseMatrix: SparseMatrix = {
    val entries = for(row <- 0 until numRows; col <- 0 until numCols) yield {
      (row, col, apply(row, col))
    }

    SparseMatrix.fromCOO(numRows, numCols, entries.filter(_._3 != 0))
  }

  /** Calculates the linear index of the respective matrix entry
    *
    * @param row row index
    * @param col column index
    * @return the index of the value according to the row and index
    */
  private def locate(row: Int, col: Int): Int = {
    require(0 <= row && row < numRows && 0 <= col && col < numCols,
      (row, col) + " not in [0, " + numRows + ") x [0, " + numCols + ")")

    row + col * numRows
  }

  /** Converts the entry at (row, col) to string
    *
    * @param row row index
    * @param col column index
    * @return Takes the value according to the row and index and convert it to string
    */
  private def printEntry(row: Int, col: Int): String = {
    val index = locate(row, col)

    data(index).toString
  }

  /** Copies the matrix instance
    *
    * @return Copy of itself
    */
  override def copy: DenseMatrix = {
    new DenseMatrix(numRows, numCols, data.clone)
  }
}

object DenseMatrix {

  val LINE_WIDTH = 100
  val MAX_ROWS = 50

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
