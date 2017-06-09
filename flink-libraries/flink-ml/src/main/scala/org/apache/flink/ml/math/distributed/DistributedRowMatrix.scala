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

package org.apache.flink.ml.math.distributed

import org.apache.flink.api.scala._
import org.apache.flink.ml.math._
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.distributed.DistributedMatrix._

/** Represents distributed row-major matrix.
  *
  * @param data    [[DataSet]] which contains [[IndexedRow]]s
  * @param numRows Number of rows
  * @param numCols Number of columns
  */
class DistributedRowMatrix(
  val data: DataSet[IndexedRow],
  val numRows: Int,
  val numCols: Int
) extends DistributedMatrix {

  /** Collects the data in the form of a sequence of coordinates associated with their values.
    * This operation immediately triggers program execution.
    *
    * @return Returns the matrix in the sparse coordinate format
    */
  def toCOO: Seq[(MatrixRowIndex, MatrixColIndex, Double)] = {
    val localRows = data.collect()

    for {
      IndexedRow(rowIndex, vector) <- localRows
      (columnIndex, value) <- vector
    } yield (rowIndex, columnIndex, value)
  }

  /** Collects the data in the form of a SparseMatrix. This operation immediately triggers program
    * execution.
    *
    * @return Returns the matrix as a local [[SparseMatrix]]
    */
  def toLocalSparseMatrix: SparseMatrix = {
    val localMatrix = SparseMatrix.fromCOO(this.numRows, this.numCols, this.toCOO)
    require(localMatrix.numRows == this.numRows)
    require(localMatrix.numCols == this.numCols)

    localMatrix
  }

  // TODO: convert to dense representation on the distributed matrix and collect it afterward
  /** Collects the data in the form of a DenseMatrix. This operation immediately triggers program
    * execution.
    *
    * @return Returns the matrix as a [[DenseMatrix]]
    */
  def toLocalDenseMatrix: DenseMatrix = this.toLocalSparseMatrix.toDenseMatrix

  /** Applies a high-order function to couple of rows.
    *
    * @param func  a function to be applied
    * @param other a [[DistributedRowMatrix]] to apply the function together
    * @return Applies the function and returns a new [[DistributedRowMatrix]]
    */
  def byRowOperation(
    func: (Vector, Vector) => Vector,
    other: DistributedRowMatrix
  ): DistributedRowMatrix = {
    val otherData = other.data
    require(this.numCols == other.numCols)
    require(this.numRows == other.numRows)

    val result = this.data
      .fullOuterJoin(otherData)
      .where("rowIndex")
      .equalTo("rowIndex")(
        (left: IndexedRow, right: IndexedRow) => {
          val row1 = Option(left) match {
            case Some(row: IndexedRow) => row
            case None =>
              IndexedRow(right.rowIndex, SparseVector.fromCOO(right.values.size, List((0, 0.0))))
          }
          val row2 = Option(right) match {
            case Some(row: IndexedRow) => row
            case None =>
              IndexedRow(left.rowIndex, SparseVector.fromCOO(left.values.size, List((0, 0.0))))
          }
          IndexedRow(row1.rowIndex, func(row1.values, row2.values))
        }
      )
    new DistributedRowMatrix(result, numRows, numCols)
  }

  /** Adds this matrix to another matrix.
    *
    * @param other a [[DistributedRowMatrix]] to be added
    * @return [[DistributedRowMatrix]] representing the two matrices added.
    */
  def add(other: DistributedRowMatrix): DistributedRowMatrix = {
    val addFunction = (x: Vector, y: Vector) => (x.asBreeze + y.asBreeze).fromBreeze
    this.byRowOperation(addFunction, other)
  }

  /** Subtracts another matrix from this matrix.
    *
    * @param other a [[DistributedRowMatrix]] to be subtracted from this matrix
    * @return [[DistributedRowMatrix]] representing the original matrix subtracted by the supplied
    *        matrix.
    */
  def subtract(other: DistributedRowMatrix): DistributedRowMatrix = {
    val subFunction = (x: Vector, y: Vector) => (x.asBreeze - y.asBreeze).fromBreeze
    this.byRowOperation(subFunction, other)
  }
}

object DistributedRowMatrix {

  /** Builds a [[DistributedRowMatrix]] from a [[DataSet]] in COO.
    *
    * @param data     [[DataSet]] which contains matrix elements in the form of
    *                 (row index, column index, value)
    * @param numRows  Number of rows
    * @param numCols  Number of columns
    * @param isSorted If false, sorts the row to properly build the matrix representation.
    *                 If already sorted, set this parameter to true to skip sorting.
    * @return the [[DistributedRowMatrix]] build from the original coordinate matrix
    */
  def fromCOO(data: DataSet[(MatrixRowIndex, MatrixColIndex, Double)],
    numRows: Int,
    numCols: Int,
    isSorted: Boolean = false
  ): DistributedRowMatrix = {
    val vectorData: DataSet[(MatrixRowIndex, SparseVector)] = data
      .groupBy(0)
      .reduceGroup(sparseRow => {
        require(sparseRow.nonEmpty)
        val sortedRow =
          if (isSorted) {
            sparseRow.toList
          } else {
            sparseRow.toList.sortBy(row => row._2)
          }
        val (indices, values) = sortedRow.map(x => (x._2, x._3)).unzip
        (sortedRow.head._1, SparseVector(numCols, indices.toArray, values.toArray))
      })

    val zippedData = vectorData.map(x => IndexedRow(x._1.toInt, x._2))

    new DistributedRowMatrix(zippedData, numRows, numCols)
  }
}

/** Represents a row in row-major matrix. */
case class IndexedRow(rowIndex: MatrixRowIndex, values: Vector) extends Ordered[IndexedRow] {
  def compare(other: IndexedRow) = this.rowIndex.compare(other.rowIndex)

  override def toString: String = s"($rowIndex, ${values.toString})"
}
