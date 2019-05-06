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

package org.apache.flink.table.codegen.agg.batch

import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.`type`.{InternalType, InternalTypes, RowType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.CodeGenOperatorFactory

import org.junit.Test

/**
  * Test for [[SortAggCodeGenerator]].
  */
class SortAggCodeGeneratorTest extends BatchAggTestBase {

  val localOutputType = new RowType(
    Array[InternalType](
      InternalTypes.STRING, InternalTypes.STRING,
      InternalTypes.LONG, InternalTypes.LONG,
      InternalTypes.DOUBLE, InternalTypes.LONG,
      createInternalTypeFromTypeInfo(imperativeAggFunc.getAccumulatorType)),
    Array(
      "f0", "f4",
      "agg1Buffer1", "agg1Buffer2",
      "agg2Buffer1", "agg2Buffer2",
      "agg3Buffer"))

  @Test
  def testLocal(): Unit = {
    testOperator(
      getOperatorWithKey(isMerge = false, isFinal = false),
      Array(
        row("key1", 8L, 8D, 8L, "aux1"),
        row("key1", 4L, 4D, 4L, "aux1"),
        row("key1", 2L, 2D, 2L, "aux1"),
        row("key2", 3L, 3D, 3L, "aux2")
      ),
      Array(
        row("key1", "aux1", 14L, 3L, 14D, 3L, row(14L, 3L)),
        row("key2", "aux2", 3L, 1L, 3D, 1L, row(3L, 1L)))
    )
  }

  @Test
  def testGlobal(): Unit = {
    testOperator(
      getOperatorWithKey(isMerge = true, isFinal = true),
      Array(
        row("key1", "aux1", 8L, 2L, 8D, 2L, row(8L, 2L)),
        row("key1", "aux1", 4L, 2L, 4D, 2L, row(4L, 2L)),
        row("key1", "aux1", 6L, 2L, 6D, 2L, row(6L, 2L)),
        row("key2", "aux2", 8L, 2L, 8D, 2L, row(8L, 2L))
      ),
      Array(
        row("key1", "aux1", 3.0D, 3.0D, 3.0D),
        row("key2", "aux2", 4.0D, 4.0D, 4.0D))
    )
  }

  @Test
  def testComplete(): Unit = {
    testOperator(
      getOperatorWithKey(isMerge = false, isFinal = true),
      Array(
        row("key1", 8L, 8D, 8L, "aux1"),
        row("key1", 4L, 4D, 4L, "aux1"),
        row("key1", 4L, 4D, 4L, "aux1"),
        row("key1", 6L, 6D, 6L, "aux1"),
        row("key2", 3L, 3D, 3L, "aux2")
      ),
      Array(
        row("key1", "aux1", 5.5D, 5.5D, 5.5D),
        row("key2", "aux2", 3.0D, 3.0D, 3.0D))
    )
  }

  private def getOperatorWithKey(isMerge: Boolean, isFinal: Boolean)
    : (CodeGenOperatorFactory[BaseRow], RowType, RowType) = {
    val localOutputType = new RowType(
      Array[InternalType](
        InternalTypes.STRING, InternalTypes.STRING,
        InternalTypes.LONG, InternalTypes.LONG,
        InternalTypes.DOUBLE, InternalTypes.LONG,
        createInternalTypeFromTypeInfo(imperativeAggFunc.getAccumulatorType)),
      Array(
        "f0", "f4",
        "agg1Buffer1", "agg1Buffer2",
        "agg2Buffer1", "agg2Buffer2",
        "agg3Buffer"))

    val (iType, oType) = if (isMerge && isFinal) {
      (localOutputType, globalOutputType)
    } else if (!isMerge && isFinal) {
      (inputType, globalOutputType)
    } else {
      (inputType, localOutputType)
    }
    val auxGrouping = if (isMerge) Array(1) else Array(4)
    val genOp = SortAggCodeGenerator.genWithKeys(
      ctx, relBuilder, aggInfoList, iType, oType, Array(0), auxGrouping, isMerge, isFinal)
    (new CodeGenOperatorFactory[BaseRow](genOp), iType, oType)
  }
}
