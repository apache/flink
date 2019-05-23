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

package org.apache.flink.table.runtime.join

import org.apache.flink.types.Row

/**
  * An utility to generate reusable padding results for outer joins.
  */
class OuterJoinPaddingUtil(leftArity: Int, rightArity: Int) extends java.io.Serializable{

  private val resultArity = leftArity + rightArity
  private val leftNullPaddingResult = new Row(resultArity)
  private val rightNullPaddingResult = new Row(resultArity)

  // Initialize the two reusable padding results.
  var i = 0
  while (i < leftArity) {
    leftNullPaddingResult.setField(i, null)
    i = i + 1
  }
  i = 0
  while (i < rightArity) {
    rightNullPaddingResult.setField(i + leftArity, null)
    i = i + 1
  }

  /**
    * Returns a padding result with the given right row.
    * @param rightRow the right row to pad
    * @return the reusable null padding result
    */
  final def padRight(rightRow: Row): Row = {
    var i = 0
    while (i < rightArity) {
      leftNullPaddingResult.setField(leftArity + i, rightRow.getField(i))
      i = i + 1
    }
    leftNullPaddingResult
  }

  /**
    * Returns a padding result with the given left row.
    * @param leftRow the left row to pad
    * @return the reusable null padding result
    */
  final def padLeft(leftRow: Row): Row = {
    var i = 0
    while (i < leftArity) {
      rightNullPaddingResult.setField(i, leftRow.getField(i))
      i = i + 1
    }
    rightNullPaddingResult
  }
}
