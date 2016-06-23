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

import org.apache.flink.ml.math.distributed.DistributedMatrix.{MatrixColIndex, MatrixRowIndex}

/**
  * This class is in charge of handling all the spatial logic required by BlockMatrix.
  * It introduces a new space of zero-indexed coordinates (i,j), called "mapped coordinates".
  * This is a space where blocks are indexed (starting from zero).
  *
  * So every coordinate in the original space can be mapped to this space and to the
  * corrisponding block.  A block have a row and a column coordinate that explicits
  * its position. Every set of coordinates in the mapped space corresponds to a square
  * of size rowPerBlock x colsPerBlock.
  *
  */
case class BlockMapper( //original matrix size
                       numRows: Int,
                       numCols: Int,
                       //block size
                       rowsPerBlock: Int,
                       colsPerBlock: Int) {

  require(numRows >= rowsPerBlock && numCols >= colsPerBlock)
  val numBlockRows: Int = math.ceil(numRows * 1.0 / rowsPerBlock).toInt
  val numBlockCols: Int = math.ceil(numCols * 1.0 / colsPerBlock).toInt
  val numBlocks = numBlockCols * numBlockRows

  /**
    * Translates absolute coordinates to the mapped coordinates of the block
    * these coordinates belong to.
    * @param i
    * @param j
    * @return
    */
  def absCoordToMappedCoord(i: MatrixRowIndex, j: MatrixColIndex): (Int, Int) =
    getBlockMappedCoordinates(getBlockIdByCoordinates(i, j))

  /**
    * Retrieves a block id from original coordinates
    * @param i Original row
    * @param j Original column
    * @return Block ID
    */
  def getBlockIdByCoordinates(i: MatrixRowIndex, j: MatrixColIndex): Int = {

    if (i < 0 || j < 0 || i >= numRows || j >= numCols) {
      throw new IllegalArgumentException(s"Invalid coordinates ($i,$j).")
    } else {
      val mappedRow = i / rowsPerBlock
      val mappedColumn = j / colsPerBlock
      val res = mappedRow * numBlockCols + mappedColumn

      assert(res <= numBlocks)
      res
    }
  }

  /**
    * Retrieves mapped coordinates for a given block.
    * @param blockId
    * @return
    */
  def getBlockMappedCoordinates(blockId: Int): (Int, Int) = {
    if (blockId < 0 || blockId > numBlockCols * numBlockRows) {
      throw new IllegalArgumentException(
          s"BlockId numeration starts from 0. $blockId is not a valid Id"
      )
    } else {
      val i = blockId / numBlockCols
      val j = blockId % numBlockCols
      (i, j)
    }
  }

  /**
    * Retrieves the ID of the block at the given coordinates
    * @param i
    * @param j
    * @return
    */
  def getBlockIdByMappedCoord(i: Int, j: Int): Int = {

    if (i < 0 || j < 0 || i >= numBlockRows || j >= numBlockCols) {
      throw new IllegalArgumentException(s"Invalid coordinates ($i,$j).")
    } else {
      i * numBlockCols + j
    }
  }
}
