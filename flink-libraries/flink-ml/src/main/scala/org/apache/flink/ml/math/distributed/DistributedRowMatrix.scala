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

import breeze.linalg.{CSCMatrix => BreezeSparseMatrix, Matrix => BreezeMatrix, Vector => BreezeVector}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{Matrix => FlinkMatrix, _}

/**
  * Distributed row-major matrix representation.
  * @param numRows Number of rows.
  * @param numCols Number of columns.
  */
class DistributedRowMatrix(val data: DataSet[IndexedRow],
                           val numRows: Int,
                           val numCols: Int )
    extends DistributedMatrix {



  /**
    * Collects the data in the form of a sequence of coordinates associated with their values.
    * @return
    */
  def toCOO: Seq[(Int, Int, Double)] = {

    val localRows = data.collect()

    for (IndexedRow(rowIndex, vector) <- localRows;
         (columnIndex, value) <- vector) yield (rowIndex, columnIndex, value)
  }

  /**
    * Collects the data in the form of a SparseMatrix
    * @return
    */
  def toLocalSparseMatrix: SparseMatrix = {
    val localMatrix =
      SparseMatrix.fromCOO(this.numRows, this.numCols, this.toCOO)
    require(localMatrix.numRows == this.numRows)
    require(localMatrix.numCols == this.numCols)
    localMatrix
  }

  //TODO: convert to dense representation on the distributed matrix and collect it afterward
  def toLocalDenseMatrix: DenseMatrix = this.toLocalSparseMatrix.toDenseMatrix

  /**
    * Apply a high-order function to couple of rows
    * @param fun
    * @param other
    * @return
    */
  def byRowOperation(fun: (Vector, Vector) => Vector,
                     other: DistributedRowMatrix): DistributedRowMatrix = {
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
                IndexedRow(
                    right.rowIndex,
                    SparseVector.fromCOO(right.values.size, List((0, 0.0))))
            }
            val row2 = Option(right) match {
              case Some(row: IndexedRow) => row
              case None =>
                IndexedRow(
                    left.rowIndex,
                    SparseVector.fromCOO(left.values.size, List((0, 0.0))))
            }
            IndexedRow(row1.rowIndex, fun(row1.values, row2.values))
          }
      )
    new DistributedRowMatrix(result, numRows, numCols)
  }

  /**
    * Add the matrix to another matrix.
    * @param other
    * @return
    */
  def sum(other: DistributedRowMatrix): DistributedRowMatrix = {
    val sumFunction: (Vector, Vector) => Vector = (x: Vector, y: Vector) =>
      (x.asBreeze + y.asBreeze).fromBreeze
    this.byRowOperation(sumFunction, other)
  }

  /**
    * Subtracts another matrix.
    * @param other
    * @return
    */
  def subtract(other: DistributedRowMatrix): DistributedRowMatrix = {
    val subFunction: (Vector, Vector) => Vector = (x: Vector, y: Vector) =>
      (x.asBreeze - y.asBreeze).fromBreeze
    this.byRowOperation(subFunction, other)
  }
}

object DistributedRowMatrix {

  type MatrixRowIndex = Int

  /**
    * Builds a DistributedRowMatrix from a dataset in COO
    * @param isSorted If false, sorts the row to properly build the matrix representation.
    *                 If already sorted, set this parameter to true to skip sorting.
    * @return
    */
  def fromCOO(data: DataSet[(Int, Int, Double)],
              numRows: Int,
              numCols: Int,
              isSorted: Boolean = false): DistributedRowMatrix = {
    val vectorData: DataSet[(Int, SparseVector)] = data
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
        (sortedRow.head._1,
         SparseVector(numCols, indices.toArray, values.toArray))
      })

    val zippedData = vectorData.map(x => IndexedRow(x._1.toInt, x._2))

    new DistributedRowMatrix(zippedData, numRows, numCols)
  }
}

case class IndexedRow(rowIndex: Int, values: Vector)
    extends Ordered[IndexedRow] {

  def compare(other: IndexedRow) = this.rowIndex.compare(other.rowIndex)

  override def toString: String = s"($rowIndex,${values.toString})"
}
