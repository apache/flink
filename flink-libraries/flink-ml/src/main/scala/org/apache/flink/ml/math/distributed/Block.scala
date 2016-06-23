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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{Matrix => FlinkMatrix, SparseMatrix}

class Block() {

  var blockData: FlinkMatrix = null

  def setBlockData(flinkMatrix: FlinkMatrix) = blockData = flinkMatrix

  def getBlockData = blockData

  def toBreeze = blockData.asBreeze

  def getCols = blockData.numCols

  def getRows = blockData.numRows

  //TODO: evaluate efficiency of conversion to and from Breeze
  def multiply(other: Block) = {

    require(this.getCols == other.getRows)

    Block((blockData.asBreeze * other.toBreeze).fromBreeze)
  }

  def sum(other: Block) =
    Block((blockData.asBreeze + other.toBreeze).fromBreeze)

  override def equals(other: Any) = {
    other match {
      case x: Block =>
        this.blockData.equalsMatrix(x.getBlockData)
      case _ => false
    }
  }
}

object Block {

  def apply(data: FlinkMatrix) = {
    val b = new Block()
    b.setBlockData(data)
    b
  }

  def zero(rows: Int, cols: Int) = {
    val b = new Block()
    val zeros = SparseMatrix.fromCOO(rows, cols, List((0, 0, 0.0)))
    b.setBlockData(zeros)
    b
  }
}
