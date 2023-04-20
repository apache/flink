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
package org.apache.flink.table.planner.codegen.agg.batch

import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, LogicalType, RowType, VarCharType}

import org.junit.Test

/** Test for [[SortAggCodeGenerator]]. */
class SortAggCodeGeneratorTest extends BatchAggTestBase {

  val localOutputType = RowType.of(
    Array[LogicalType](
      VarCharType.STRING_TYPE,
      VarCharType.STRING_TYPE,
      new BigIntType(),
      new BigIntType(),
      new DoubleType(),
      new BigIntType(),
      fromTypeInfoToLogicalType(imperativeAggFunc.getAccumulatorType)
    ),
    Array("f0", "f4", "agg1Buffer1", "agg1Buffer2", "agg2Buffer1", "agg2Buffer2", "agg3Buffer")
  )

  @Test
  def testLocal(): Unit = {
    testOperator(
      getOperatorWithKey(isMerge = false, isFinal = false),
      Array(
        row("key1", 8L, 8d, 8L, "aux1"),
        row("key1", 4L, 4d, 4L, "aux1"),
        row("key1", 2L, 2d, 2L, "aux1"),
        row("key2", 3L, 3d, 3L, "aux2")
      ),
      Array(
        row("key1", "aux1", 14L, 3L, 14d, 3L, row(14L, 3L)),
        row("key2", "aux2", 3L, 1L, 3d, 1L, row(3L, 1L)))
    )
  }

  @Test
  def testGlobal(): Unit = {
    testOperator(
      getOperatorWithKey(isMerge = true, isFinal = true),
      Array(
        row("key1", "aux1", 8L, 2L, 8d, 2L, row(8L, 2L)),
        row("key1", "aux1", 4L, 2L, 4d, 2L, row(4L, 2L)),
        row("key1", "aux1", 6L, 2L, 6d, 2L, row(6L, 2L)),
        row("key2", "aux2", 8L, 2L, 8d, 2L, row(8L, 2L))
      ),
      Array(row("key1", "aux1", 3L, 3.0d, 3L), row("key2", "aux2", 4L, 4.0d, 4L))
    )
  }

  @Test
  def testComplete(): Unit = {
    testOperator(
      getOperatorWithKey(isMerge = false, isFinal = true),
      Array(
        row("key1", 8L, 8d, 8L, "aux1"),
        row("key1", 4L, 4d, 4L, "aux1"),
        row("key1", 4L, 4d, 4L, "aux1"),
        row("key1", 6L, 6d, 6L, "aux1"),
        row("key2", 3L, 3d, 3L, "aux2")
      ),
      Array(row("key1", "aux1", 5L, 5.5d, 5L), row("key2", "aux2", 3L, 3.0d, 3L))
    )
  }

  private def getOperatorWithKey(
      isMerge: Boolean,
      isFinal: Boolean): (CodeGenOperatorFactory[RowData], RowType, RowType) = {
    val localOutputType = RowType.of(
      Array[LogicalType](
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        new BigIntType(),
        new BigIntType(),
        new DoubleType(),
        new BigIntType(),
        fromTypeInfoToLogicalType(imperativeAggFunc.getAccumulatorType)
      ),
      Array("f0", "f4", "agg1Buffer1", "agg1Buffer2", "agg2Buffer1", "agg2Buffer2", "agg3Buffer")
    )

    val (iType, oType) = if (isMerge && isFinal) {
      (localOutputType, globalOutputType)
    } else if (!isMerge && isFinal) {
      (inputType, globalOutputType)
    } else {
      (inputType, localOutputType)
    }
    val auxGrouping = if (isMerge) Array(1) else Array(4)
    val genOp = SortAggCodeGenerator.genWithKeys(
      ctx,
      relBuilder,
      aggInfoList,
      iType,
      oType,
      Array(0),
      auxGrouping,
      isMerge,
      isFinal)
    (new CodeGenOperatorFactory[RowData](genOp), iType, oType)
  }
}
