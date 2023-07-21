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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction.LongAvgAggFunction
import org.apache.flink.table.planner.plan.utils.{AggregateInfo, AggregateInfoList}
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.types.logical._

import org.apache.calcite.rel.core.AggregateCall
import org.junit.Test
import org.mockito.Mockito.{mock, when}

/** Test for [[HashAggCodeGenerator]]. */
class HashAggCodeGeneratorTest extends BatchAggTestBase {

  val localOutputType = RowType.of(
    Array[LogicalType](
      VarCharType.STRING_TYPE,
      VarCharType.STRING_TYPE,
      new BigIntType(),
      new BigIntType(),
      new DoubleType(),
      new BigIntType(),
      new BigIntType(),
      new BigIntType()),
    Array(
      "f0",
      "f4",
      "agg1Buffer1",
      "agg1Buffer2",
      "agg2Buffer1",
      "agg2Buffer2",
      "agg3Buffer1",
      "agg3Buffer2")
  )

  // override imperativeAggFunc, hash agg only handle DeclarativeAggregateFunction
  override val aggInfo3: AggregateInfo = {
    val aggInfo = mock(classOf[AggregateInfo])
    val call = mock(classOf[AggregateCall])
    when(aggInfo.agg).thenReturn(call)
    when(call.getName).thenReturn("avg3")
    when(call.hasFilter).thenReturn(false)
    when(aggInfo.function).thenReturn(new LongAvgAggFunction)
    when(aggInfo.externalAccTypes).thenReturn(Array(DataTypes.BIGINT, DataTypes.BIGINT))
    when(aggInfo.argIndexes).thenReturn(Array(3))
    when(aggInfo.aggIndex).thenReturn(2)
    aggInfo
  }

  override val aggInfoList =
    AggregateInfoList(Array(aggInfo1, aggInfo2, aggInfo3), None, countStarInserted = false, Array())

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
        row("key1", "aux1", 14L, 3L, 14d, 3L, 14L, 3L),
        row("key2", "aux2", 3L, 1L, 3d, 1L, 3L, 1L))
    )
  }

  @Test
  def testGlobal(): Unit = {
    testOperator(
      getOperatorWithKey(isMerge = true, isFinal = true),
      Array(
        row("key1", "aux1", 8L, 2L, 8d, 2L, 8L, 2L),
        row("key1", "aux1", 4L, 2L, 4d, 2L, 4L, 2L),
        row("key1", "aux1", 6L, 2L, 6d, 2L, 6L, 2L),
        row("key2", "aux2", 8L, 2L, 8d, 2L, 8L, 2L)
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
    val (iType, oType) = if (isMerge && isFinal) {
      (localOutputType, globalOutputType)
    } else if (!isMerge && isFinal) {
      (inputType, globalOutputType)
    } else {
      (inputType, localOutputType)
    }
    val auxGrouping = if (isMerge) Array(1) else Array(4)
    val genOp = HashAggCodeGenerator.genWithKeys(
      ctx,
      relBuilder,
      aggInfoList,
      iType,
      oType,
      Array(0),
      auxGrouping,
      isMerge,
      isFinal,
      false,
      ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES.defaultValue(),
      ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue,
      ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE.defaultValue.getBytes.toInt
    )
    (new CodeGenOperatorFactory[RowData](genOp), iType, oType)
  }

}
