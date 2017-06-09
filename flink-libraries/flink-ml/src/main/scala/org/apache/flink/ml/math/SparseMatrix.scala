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

import scala.util.Sorting

/** Sparse matrix using the compressed sparse column (CSC) representation.
  *
  * More details concerning the compressed sparse column (CSC) representation can be found
  * [http://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_column_.28CSC_or_CCS.29].
  *
  * @param numRows Number of rows
  * @param numCols Number of columns
  * @param rowIndices Array containing the row indices of non-zero entries
  * @param colPtrs Array containing the starting offsets in data for each column
  * @param data Array containing the non-zero entries in column-major order
  */
class SparseMatrix(
    val numRows: Int,
    val numCols: Int,
    val rowIndices: Array[Int],
    val colPtrs: Array[Int],
    val data: Array[Double])
  extends Matrix
  with Serializable {

  /** Element wise access function
    *
    * @param row row index
    * @param col column index
    * @return matrix entry at (row, col)
    */
  override def apply(row: Int, col: Int): Double = {
    val index = locate(row, col)

    if(index < 0){
      0
    } else {
     data(index)
    }
  }

  def toDenseMatrix: DenseMatrix = {
    val result = DenseMatrix.zeros(numRows, numCols)

    for(row <- 0 until numRows; col <- 0 until numCols) {
      result(row, col) = apply(row, col)
    }

    result
  }

  /** Element wise update function
    *
    * @param row row index
    * @param col column index
    * @param value value to set at (row, col)
    */
  override def update(row: Int, col: Int, value: Double): Unit = {
    val index = locate(row, col)

    if(index < 0) {
      throw new IllegalArgumentException("Cannot update zero value of sparse matrix at index " +
      s"($row, $col)")
    } else {
      data(index) = value
    }
  }

  override def toString: String = {
    val result = StringBuilder.newBuilder

    result.append(s"SparseMatrix($numRows, $numCols)\n")

    var columnIndex = 0

    val fieldWidth = math.max(numRows, numCols).toString.length
    val valueFieldWidth = data.map(_.toString.length).max + 2

    for(index <- 0 until colPtrs.last) {
      while(colPtrs(columnIndex + 1) <= index){
        columnIndex += 1
      }

      val rowStr = rowIndices(index).toString
      val columnStr = columnIndex.toString
      val valueStr = data(index).toString

      result.append("(" + " " * (fieldWidth - rowStr.length) + rowStr + "," +
        " " * (fieldWidth - columnStr.length) + columnStr + ")")
      result.append(" " * (valueFieldWidth - valueStr.length) + valueStr)
      result.append("\n")
    }

    result.toString
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case sm: SparseMatrix if numRows == sm.numRows && numCols == sm.numCols =>
        rowIndices.sameElements(sm.rowIndices) && colPtrs.sameElements(sm.colPtrs) &&
        data.sameElements(sm.data)
      case _ => false
    }
  }

  override def hashCode: Int = {
    val hashCodes = List(numRows.hashCode(), numCols.hashCode(),
      java.util.Arrays.hashCode(rowIndices), java.util.Arrays.hashCode(colPtrs),
      java.util.Arrays.hashCode(data))

    hashCodes.foldLeft(5){(left, right) => left * 41 + right}
  }

  private def locate(row: Int, col: Int): Int = {
    require(0 <= row && row < numRows && 0 <= col && col < numCols,
      (row, col) + " not in [0, " + numRows + ") x [0, " + numCols + ")")

    val startIndex = colPtrs(col)
    val endIndex = colPtrs(col + 1)

    java.util.Arrays.binarySearch(rowIndices, startIndex, endIndex, row)
  }

  /** Copies the matrix instance
    *
    * @return Copy of itself
    */
  override def copy: SparseMatrix = {
    new SparseMatrix(numRows, numCols, rowIndices.clone, colPtrs.clone(), data.clone)
  }
}

object SparseMatrix{

  /** Constructs a sparse matrix from a coordinate list (COO) representation where each entry
    * is stored as a tuple of (rowIndex, columnIndex, value).
    *
    * @param numRows Number of rows
    * @param numCols Number of columns
    * @param entries Data entries in the matrix
    * @return Newly constructed sparse matrix
    */
  def fromCOO(numRows: Int, numCols: Int, entries: (Int, Int, Double)*): SparseMatrix = {
    fromCOO(numRows, numCols, entries)
  }

  /** Constructs a sparse matrix from a coordinate list (COO) representation where each entry
    * is stored as a tuple of (rowIndex, columnIndex, value).
    *
    * @param numRows Number of rows
    * @param numCols Number of columns
    * @param entries Data entries in the matrix
    * @return Newly constructed sparse matrix
    */
  def fromCOO(numRows: Int, numCols: Int, entries: Iterable[(Int, Int, Double)]): SparseMatrix = {
    val entryArray = entries.toArray

    entryArray.foreach{ case (row, col, _) =>
      require(0 <= row && row < numRows && 0 <= col && col <= numCols,
        (row, col) + " not in [0, " + numRows + ") x [0, " + numCols + ")")
    }

    val COOOrdering = new Ordering[(Int, Int, Double)] {
      override def compare(x: (Int, Int, Double), y: (Int, Int, Double)): Int = {
        if(x._2 < y._2) {
          -1
        } else if(x._2 > y._2) {
          1
        } else {
          x._1 - y._1
        }
      }
    }

    Sorting.quickSort(entryArray)(COOOrdering)

    val nnz = entryArray.length

    val data = new Array[Double](nnz)
    val rowIndices = new Array[Int](nnz)
    val colPtrs = new Array[Int](numCols + 1)

    var (lastRow, lastCol, lastValue) = entryArray(0)

    rowIndices(0) = lastRow
    data(0) = lastValue

    var i = 1
    var lastDataIndex = 0

    while(i < nnz) {
      val (curRow, curCol, curValue) = entryArray(i)

      if(lastRow == curRow && lastCol == curCol) {
        // add values with identical coordinates
        data(lastDataIndex) += curValue
      } else {
        lastDataIndex += 1
        data(lastDataIndex) = curValue
        rowIndices(lastDataIndex) = curRow
        lastRow = curRow
      }

      while(lastCol < curCol) {
        lastCol += 1
        colPtrs(lastCol) = lastDataIndex
      }

      i += 1
    }

    lastDataIndex += 1
    while(lastCol < numCols) {
      colPtrs(lastCol + 1) = lastDataIndex
      lastCol += 1
    }

    val prunedRowIndices = if(lastDataIndex < nnz) {
      val prunedArray = new Array[Int](lastDataIndex)
      rowIndices.copyToArray(prunedArray)
      prunedArray
    } else {
      rowIndices
    }

    val prunedData = if(lastDataIndex < nnz) {
      val prunedArray = new Array[Double](lastDataIndex)
      data.copyToArray(prunedArray)
      prunedArray
    } else {
      data
    }

    new SparseMatrix(numRows, numCols, prunedRowIndices, colPtrs, prunedData)
  }

  /** Convenience method to convert a single tuple with an integer value into a SparseMatrix.
    * The problem is that providing a single tuple to the fromCOO method, the Scala type inference
    * cannot infer that the tuple has to be of type (Int, Int, Double) because of the overloading
    * with the Iterable type.
    *
    * @param numRows Number of rows
    * @param numCols Number of columns
    * @param entry Data entries in the matrix
    * @return Newly constructed sparse matrix
    */
  def fromCOO(numRows: Int, numCols: Int, entry: (Int, Int, Int)): SparseMatrix = {
    fromCOO(numRows, numCols, (entry._1, entry._2, entry._3.toDouble))
  }
}
