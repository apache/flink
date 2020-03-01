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

import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, LogicalType, RowType}
import org.junit.Test

/**
  * Test for [[AggWithoutKeysCodeGenerator]].
  */
class AggWithoutKeysTest extends BatchAggTestBase {

  val localOutputType = RowType.of(
    Array[LogicalType](
      new BigIntType(), new BigIntType(),
      new DoubleType(), new BigIntType(),
      fromTypeInfoToLogicalType(imperativeAggFunc.getAccumulatorType)),
    Array(
      "agg1Buffer1", "agg1Buffer2",
      "agg2Buffer1", "agg2Buffer2",
      "agg3Buffer"))

  override val globalOutputType = RowType.of(
    Array[LogicalType](
      new BigIntType(),
      new DoubleType(),
      new BigIntType()),
    Array(
      "agg1Output",
      "agg2Output",
      "agg3Output"))

  @Test
  def testLocal(): Unit = {
    testOperator(
      getOperatorWithoutKey(isMerge = false, isFinal = false),
      Array(
        row("key1", 8L, 8D, 8L, "aux1"),
        row("key1", 4L, 4D, 4L, "aux1"),
        row("key1", 2L, 2D, 2L, "aux1"),
        row("key2", 3L, 3D, 3L, "aux1")
      ),
      Array(row(17L, 4L, 17D, 4L, row(17L, 4L))))
  }

  @Test
  def testGlobal(): Unit = {
    testOperator(
      getOperatorWithoutKey(isMerge = true, isFinal = true),
      Array(
        row(8L, 2L, 8D, 2L, row(8L, 2L)),
        row(4L, 2L, 4D, 2L, row(4L, 2L)),
        row(6L, 2L, 6D, 2L, row(6L, 2L))
      ),
      Array(row(3L, 3.0D, 3L)))
  }

  @Test
  def testComplete(): Unit = {
    testOperator(
      getOperatorWithoutKey(isMerge = false, isFinal = true),
      Array(
        row("key1", 8L, 8D, 8L, "aux1"),
        row("key1", 4L, 4D, 4L, "aux1"),
        row("key1", 4L, 4D, 4L, "aux1"),
        row("key1", 4L, 4D, 4L, "aux1")
      ),
      Array(row(5L, 5.0D, 5L)))
  }

  private def getOperatorWithoutKey(isMerge: Boolean, isFinal: Boolean)
    : (CodeGenOperatorFactory[BaseRow], RowType, RowType) = {
    val (iType, oType) = if (isMerge && isFinal) {
      (localOutputType, globalOutputType)
    } else if (!isMerge && isFinal) {
      (inputType, globalOutputType)
    } else {
      (inputType, localOutputType)
    }
    val genOp = AggWithoutKeysCodeGenerator.genWithoutKeys(
      ctx, relBuilder, aggInfoList, iType, oType, isMerge, isFinal, "Without")
    (new CodeGenOperatorFactory[BaseRow](genOp), iType, oType)
  }
}
