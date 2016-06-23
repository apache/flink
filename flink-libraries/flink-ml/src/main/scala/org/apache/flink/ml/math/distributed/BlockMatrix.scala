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

import org.apache.flink.api.common.functions.{MapFunction, RichGroupReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{createTypeInformation, _}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.ml.math.distributed.BlockMatrix.BlockID
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
  * Distributed Matrix represented as blocks. 
  * A BlockMapper instance is used to track blocks of the matrix.
  * Every block in a BlockMatrix has an associated ID that also 
  * identifies its position in the BlockMatrix.
  * @param data
  * @param blockMapper
  */
class BlockMatrix(
    data: DataSet[(BlockID, Block)],
    blockMapper: BlockMapper
)
    extends DistributedMatrix {

  val getDataset = data

  val numCols = blockMapper.numCols
  val numRows = blockMapper.numRows


  val getBlockCols = blockMapper.numBlockCols
  val getBlockRows = blockMapper.numBlockRows

  val getRowsPerBlock = blockMapper.rowsPerBlock
  val getColsPerBlock = blockMapper.colsPerBlock

  val getNumBlocks = blockMapper.numBlocks

  /**
    * Compares the format of two block matrices
    * @return
    */
  def hasSameFormat(other: BlockMatrix): Boolean =
    this.numRows == other.numRows &&
    this.numCols == other.numCols &&
    this.getRowsPerBlock == other.getRowsPerBlock &&
    this.getColsPerBlock == other.getColsPerBlock

  /**
    * Perform an operation on pairs of block. Pairs are formed taking
    * matching blocks from the two matrices that are placed in the same position.
    * A function is then applied to the pair to return a new block.
    * These blocks are then composed in a new block matrix.
    */
  def blockPairOperation(
      fun: (Block, Block) => Block, other: BlockMatrix): BlockMatrix = {
    require(hasSameFormat(other))

    /*Full outer join on blocks. The full outer join is required because of
    the sparse nature of the matrix.
    Matching blocks may be missing and a block of zeros is used instead.*/
    val processedBlocks =
      this.getDataset.fullOuterJoin(other.getDataset).where(0).equalTo(0) {
        (left: (BlockID, Block), right: (BlockID, Block)) =>
          {

            val (id1, block1) = Option(left).getOrElse(
                (right._1, Block.zero(right._2.getRows, right._2.getCols)))

            val (id2, block2) = Option(right).getOrElse(
                (left._1, Block.zero(left._2.getRows, left._2.getCols)))

            require(id1 == id2)
            (id1, fun(block1, block2))
          }
      }
    new BlockMatrix(processedBlocks, blockMapper)
  }

  /**
    * Add the matrix to another matrix.
    * @param other
    * @return
    */
  def sum(other: BlockMatrix): BlockMatrix = {
    val sumFunction: (Block, Block) => Block = (b1: Block, b2: Block) =>
      Block((b1.toBreeze + b2.toBreeze).fromBreeze)

    this.blockPairOperation(sumFunction, other)
  }

  /**
    * Subtracts another matrix.
    * @param other
    * @return
    */
  def subtract(other: BlockMatrix): BlockMatrix = {
    val subFunction: (Block, Block) => Block = (b1: Block, b2: Block) =>
      Block((b1.toBreeze - b2.toBreeze).fromBreeze)

    this.blockPairOperation(subFunction, other)
  }

  /**
    * Multiplies two block matrices of the same format.
    * The matrix passed as a paremeter is used as right operand.
    */
  def multiply(other: BlockMatrix): BlockMatrix = {

    /*
      The multiplication is done multiplying rows and columns of blocks locally.
      The two matrices A(r1 x c1) and B(r2 x c2) are divided in blocks:
      b x c blocks for A and c x d blocks for B.

      The result of the multiplication is the matrix C (r1 x c2) composed of b x d blocks.
      This matrix is computed using the following formula:

      C_xy=SUM_i(A_xi * B_iy)

      where x, i and y are coordinates in the mapped space, A_rc and B_rc are blocks.

      So we proceed grouping pair of blocks according to their mapped coordinates,
      multiply the pairs and sum (reduction) them to obtain the block at the coord xy.


     */

    //Checking that the two matrices are compatible for multiplication.
    require(this.getBlockCols == other.getBlockRows)
    require(this.getColsPerBlock == other.getColsPerBlock)
    require(this.getRowsPerBlock == other.getRowsPerBlock)

    /*BlockID is converted to mapped coordinates that will be required to
      group blocks together.*/
    val otherWithCoord =
      other.getDataset.map(new MapToMappedCoord(blockMapper))

    val dataWithCoord = data.map(new MapToMappedCoord(blockMapper))

    //here we join pairs of blocks to be multiplied...
    val joinedBlocks = dataWithCoord.join(otherWithCoord).where(1).equalTo(0)

    //... and group them by the row of the first block and the column of the second block.
    val groupedBlocks = joinedBlocks.groupBy(x => (x._1._1, x._2._2))

    //here all the pairs in the group are multiplied and then reduced with a sum
    val reducedBlocks =
      groupedBlocks.reduceGroup(new GroupMultiplyReduction(blockMapper))

    //Finally a new block matrix is built.
    new BlockMatrix(reducedBlocks,
                    BlockMapper(this.numRows,
                                other.numCols,
                                this.blockMapper.rowsPerBlock,
                                this.blockMapper.colsPerBlock))
  }

  def toRowMatrix: DistributedRowMatrix = {
    val indexedRows = data
    //map id to mapped coordinates
      .map(
          new MapToMappedCoord(blockMapper)
      )
      //group by block row
      .groupBy(blockWithCoord => blockWithCoord._1)
      //turn a group of blocks in a seq of rows
      .reduceGroup(new ToRowMatrixReducer(blockMapper))
    new DistributedRowMatrix(indexedRows, numRows, numCols)
  }
}

object BlockMatrix {

  type BlockID = Int

  class MatrixFormatException(message: String) extends Exception(message)

  class WrongMatrixCoordinatesException(message: String)
      extends Exception(message)
}

/**
  * MapFunction that converts from BlockID to mapped coordinates using a BlockMapper.
  */
private class MapToMappedCoord(blockMapper: BlockMapper)
    extends MapFunction[(Int, Block), (Int, Int, Block)] {
  override def map(value: (BlockID, Block)): (Int, Int, Block) = {
    val (i, j) = blockMapper.getBlockMappedCoordinates(value._1)
    (i, j, value._2)
  }
}

/**
  * GroupReduce function used in the conversion to row matrix format.
  * Taken a list of blocks on the same row, it then returns a list of IndexedRows.
  */
private class ToRowMatrixReducer(blockMapper: BlockMapper)
    extends RichGroupReduceFunction[(Int, Int, Block), IndexedRow] {

  override def reduce(values: lang.Iterable[(Int, Int, Block)],
                      out: Collector[IndexedRow]): Unit = {

    val blockGroup = values.toList

    require(blockGroup.nonEmpty)

    val groupRow = blockGroup.head._1

    //all blocks must have the same row
    require(blockGroup.forall(block => block._1 == groupRow))

    //map every block to its mapped column
    val groupElements = blockGroup
      .map(x => (x._2, x._3))
      .sortBy(_._1)
      .flatMap(
          block => {

            //map coordinates from block space to original space
            block._2.getBlockData.toIterator
              .filter(_._3 != 0)
              .map(
                  element => {
                    val (i, j, value) = element

                    //here are calculated the coordinates in the original space for every value
                    (i + (groupRow * blockMapper.rowsPerBlock),
                     j + block._1 * blockMapper.colsPerBlock,
                     value)
                  }
              )
          }
      )

    //Elements are grouped by row to build an IndexedRow
    groupElements
      .groupBy(_._1)
      .foreach(row => {
        val cooVector = row._2.map(x => (x._2, x._3))
        out.collect(IndexedRow(
                row._1, SparseVector.fromCOO(blockMapper.numCols, cooVector)))
      })
  }
}

private class GroupMultiplyReduction(blockMapper: BlockMapper)
    extends RichGroupReduceFunction[
        ((Int, Int, Block), (Int, Int, Block)), (BlockID, Block)] {
  override def reduce(
      values: lang.Iterable[((BlockID, Int, Block), (Int, Int, Block))],
      out: Collector[(BlockID, Block)]): Unit = {
    val multipliedGroups: Seq[(Int, Int, Block)] = values.map {
      case ((i, j, left), (s, t, right)) => (i, t, left.multiply(right))
    }.toSeq

    val res = multipliedGroups.reduce(
        (l, r) => {
          val ((i, j, left), (_, _, right)) = (l, r)

          (i, j, left.sum(right))
        }
    )
    out.collect((blockMapper.getBlockIdByMappedCoord(res._1, res._2), res._3))
  }
}
