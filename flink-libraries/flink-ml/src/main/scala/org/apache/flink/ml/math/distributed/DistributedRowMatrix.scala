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
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichGroupReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.distributed.DistributedMatrix.{MatrixColIndex, MatrixRowIndex}
import org.apache.flink.ml.math.{Matrix => FlinkMatrix, _}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
  * Distributed row-major matrix representation.
  * @param numRows Number of rows.
  * @param numCols Number of columns.
  */
class DistributedRowMatrix(val data: DataSet[IndexedRow],
                           val numRows: Int,
                           val numCols: Int)
    extends DistributedMatrix {

  /**
    * Collects the data in the form of a sequence of coordinates associated with their values.
    * @return
    */
  def toCOO: Seq[(MatrixRowIndex, MatrixColIndex, Double)] = {

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

  def toBlockMatrix(
      rowsPerBlock: Int = 1024, colsPerBlock: Int = 1024): BlockMatrix = {
    require(rowsPerBlock > 0 && colsPerBlock > 0,
            "Block sizes must be a strictly positive value.")
    require(rowsPerBlock <= numRows && colsPerBlock <= numCols,
            "Blocks can't be bigger than the matrix")

    val blockMapper = BlockMapper(
        numRows, numCols, rowsPerBlock, colsPerBlock)

    val splitRows: DataSet[(Int, Int, Vector)] =
      data.flatMap(new RowSplitter(blockMapper))

    val rowGroupReducer = new RowGroupReducer(blockMapper)

    val blocks =
      splitRows.groupBy(0).reduceGroup(new RowGroupReducer(blockMapper))

    new BlockMatrix(blocks, blockMapper)
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
  def fromCOO(data: DataSet[(MatrixRowIndex, MatrixColIndex, Double)],
              numRows: Int,
              numCols: Int,
              isSorted: Boolean = false): DistributedRowMatrix = {
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
        (sortedRow.head._1,
         SparseVector(numCols, indices.toArray, values.toArray))
      })

    val zippedData = vectorData.map(x => IndexedRow(x._1.toInt, x._2))

    new DistributedRowMatrix(zippedData, numRows, numCols)
  }
}

case class IndexedRow(rowIndex: MatrixRowIndex, values: Vector)
    extends Ordered[IndexedRow] {

  def compare(other: IndexedRow) = this.rowIndex.compare(other.rowIndex)

  override def toString: String = s"($rowIndex,${values.toString})"
}

/**
  * Serializable Reduction function used by the toBlockMatrix function. Takes an ordered list of
  * indexed row and split those rows to form blocks.
  */
class RowGroupReducer(blockMapper: BlockMapper)
    extends RichGroupReduceFunction[(Int, Int, Vector), (Int, Block)] {

  override def reduce(values: lang.Iterable[(Int, Int, Vector)],
                      out: Collector[(Int, Block)]): Unit = {

    val sortedRows = values.toList.sortBy(_._2)
    val blockID = sortedRows.head._1
    val coo = for {
      (_, rowIndex, vec) <- sortedRows
      (colIndex, value) <- vec if value != 0
    } yield (rowIndex, colIndex, value)

    val block: Block = Block(
        SparseMatrix.fromCOO(
            blockMapper.rowsPerBlock, blockMapper.colsPerBlock, coo))
    out.collect((blockID, block))
  }
}

class RowSplitter(blockMapper: BlockMapper)
    extends RichFlatMapFunction[IndexedRow, (Int, Int, Vector)] {
  override def flatMap(
      row: IndexedRow, out: Collector[(Int, Int, Vector)]): Unit = {
    val IndexedRow(rowIndex, vector) = row
    val splitRow = sliceVector(vector)
    for ((mappedCol, slice) <- splitRow) {
      val mappedRow =
        math.floor(rowIndex * 1.0 / blockMapper.rowsPerBlock).toInt
      val blockID = blockMapper.getBlockIdByMappedCoord(mappedRow, mappedCol)
      out.collect((blockID, rowIndex % blockMapper.rowsPerBlock, slice))
    }
  }

  def sliceVector(v: Vector): List[(Int, Vector)] = {

    def getSliceByColIndex(index: Int): Int =
      index / blockMapper.colsPerBlock

    val (vectorIndices, vectorValues) =
      v.iterator.filter(_._2 != 0).toList.unzip
    require(vectorIndices.size == vectorValues.size)

    val cellsToSlice = for (i <- vectorIndices.indices) yield
      (getSliceByColIndex(vectorIndices(i)),
       vectorIndices(i) % blockMapper.colsPerBlock,
       vectorValues(i))

    cellsToSlice
      .groupBy(_._1)
      .map {
        case (sliceID, seq) =>
          (sliceID,
           SparseVector(blockMapper.colsPerBlock,
                        seq.map(_._2).toArray,
                        seq.map(_._3).toArray))
      }
      .toList
  }
}
