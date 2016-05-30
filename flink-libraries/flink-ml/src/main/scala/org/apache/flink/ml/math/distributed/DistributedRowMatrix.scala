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

import java.lang

import breeze.linalg.{CSCMatrix => BreezeSparseMatrix, Matrix => BreezeMatrix, Vector => BreezeVector}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{Matrix => FlinkMatrix, _}
import org.apache.flink.util.Collector
import org.apache.flink.ml.math.Breeze._
import scala.collection.JavaConversions._

/**
  * Distributed row-major matrix representation.
  * @param numRowsOpt If None, will be calculated from the DataSet.
  * @param numColsOpt If None, will be calculated from the DataSet.
  */
class DistributedRowMatrix(data: DataSet[IndexedRow],
                           numRowsOpt: Option[Int] = None,
                           numColsOpt: Option[Int] = None)
    extends DistributedMatrix {

  lazy val getNumRows: Int = numRowsOpt match {
    case Some(rows) => rows
    case None => data.count().toInt
  }

  lazy val getNumCols: Int = numColsOpt match {
    case Some(cols) => cols
    case None => calcCols
  }

  val getRowData = data

  private def calcCols: Int =
    data.first(1).collect().headOption match {
      case Some(vector) => vector.values.size
      case None => 0
    }

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
      SparseMatrix.fromCOO(this.getNumRows, this.getNumCols, this.toCOO)
    require(localMatrix.numRows == this.getNumRows)
    require(localMatrix.numCols == this.getNumCols)
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
    val otherData = other.getRowData
    require(this.getNumCols == other.getNumCols)
    require(this.getNumRows == other.getNumRows)


    val result = this.data
      .fullOuterJoin(otherData)
      .where("rowIndex")
      .equalTo("rowIndex")(
          (left: IndexedRow, right: IndexedRow) => {
            val row1 = Option(left).getOrElse(IndexedRow(
                    right.rowIndex,
                    SparseVector.fromCOO(right.values.size, List((0, 0.0)))))
            val row2 = Option(right).getOrElse(IndexedRow(
                    left.rowIndex,
                    SparseVector.fromCOO(left.values.size, List((0, 0.0)))))

            IndexedRow(row1.rowIndex, fun(row1.values, row2.values))
          }
      )
    new DistributedRowMatrix(result, numRowsOpt, numColsOpt)
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

    new DistributedRowMatrix(zippedData, Some(numRows), Some(numCols))
  }
}

case class IndexedRow(rowIndex: Int, values: Vector)
    extends Ordered[IndexedRow] {

  def compare(other: IndexedRow) = this.rowIndex.compare(other.rowIndex)

  override def toString: String = s"($rowIndex,${values.toString}"
}
