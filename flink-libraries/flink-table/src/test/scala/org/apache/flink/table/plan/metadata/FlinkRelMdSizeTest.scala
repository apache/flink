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

package org.apache.flink.table.plan.metadata

import org.apache.flink.api.common.typeinfo.BasicTypeInfo

import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdSizeTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testAverageColumnSizesOnAggregateBatchExec(): Unit = {
    assertEquals(Seq(8D, 8D, 8D), mq.getAverageColumnSizes(splittableLocalAgg).toSeq)
    assertEquals(Seq(8D, 8D), mq.getAverageColumnSizes(splittableGlobalAggWithLocalAgg).toSeq)
    assertEquals(Seq(8D, 4D, 8D, 8D),
      mq.getAverageColumnSizes(splittableLocalAggWithAuxGrouping).toSeq)
    assertEquals(Seq(8D, 4D, 8D),
      mq.getAverageColumnSizes(splittableGlobalAggWithLocalAggWithAuxGrouping).toSeq)
  }

  @Test
  def testAverageColumnSizesOnOverWindowAggBatchExec(): Unit = {
    assertEquals(Seq(8D, 32D, 4D, 32D, 8D, 8D), mq.getAverageColumnSizes(overWindowAgg).toSeq)
  }

  @Test
  def testAverageColumnSizesOnLogicalOverWindow(): Unit = {
    assertEquals(Seq(8D, 32D, 4D, 32D, 8D, 8D), mq.getAverageColumnSizes(logicalOverWindow).toSeq)
  }

  @Test
  def testAverageColumnSizesOnExpand(): Unit = {
    assertEquals(Seq(8D, 32D, 8D, 8D), mq.getAverageColumnSizes(aggWithExpand).toSeq)
    assertEquals(Seq(8.0, 8.0, 32.0, 4.0, 8.0),
      mq.getAverageColumnSizes(aggWithAuxGroupAndExpand).toSeq)
  }

  @Test
  def testAverageColumnSizesOnFlinkLogicalWindowAggregate(): Unit = {
    assertEquals(Seq(4D, 32D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(flinkLogicalWindowAgg).toSeq)
    assertEquals(Seq(8D, 4D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(flinkLogicalWindowAggWithAuxGroup).toSeq)
  }

  @Test
  def testAverageColumnSizesOnLogicalWindowAggregate(): Unit = {
    assertEquals(Seq(4D, 32D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(logicalWindowAgg).toSeq)
    assertEquals(Seq(8D, 4D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(logicalWindowAggWithAuxGroup).toSeq)
  }

  @Test
  def testAverageColumnSizesOnWindowAggregateBatchExec(): Unit = {
    assertEquals(Seq(4D, 32D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(globalWindowAggWithLocalAgg).toSeq)
    assertEquals(Seq(4D, 32D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(globalWindowAggWithoutLocalAgg).toSeq)
    assertEquals(Seq(8D, 4D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(globalWindowAggWithLocalAggWithAuxGrouping).toSeq)
    assertEquals(Seq(8D, 4D, 8D, 12D, 12D, 12D, 12D),
      mq.getAverageColumnSizes(globalWindowAggWithoutLocalAggWithAuxGrouping).toSeq)
  }

  @Test
  def testAverageColumnSizeOnCalc(): Unit = {
    val ts = relBuilder.scan("t1").build()
    relBuilder.push(ts)
    // projects: $0==1, $0, $1, true, 2.1, 2
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.getRexBuilder.makeLiteral(
        2.1D, typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, false), true),
      relBuilder.getRexBuilder.makeLiteral(
        2L, typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, false), true))
    val outputRowType = relBuilder.project(projects).build().getRowType
    relBuilder.push(ts)
    val expr = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    // calc => projects + filter: $0 <= 2
    val calc = buildCalc(ts, outputRowType, projects, List(expr))
    assertEquals(Seq(1D, 8D, 32D, 1D, 8D, 8D), mq.getAverageColumnSizes(calc).toSeq)
  }

  @Test
  def testAverageColumnSizeOnProject(): Unit = {
    assertEquals(Seq(1D, 8D, 32D, 1D, 8D, 8D), mq.getAverageColumnSizes(project).toSeq)
  }


  @Test
  def testAverageColumnSizeOnValues(): Unit = {
    assertEquals(Seq(8D, 1D, 12D, 12D, 12D, 8D, 4D), mq.getAverageColumnSizes(emptyValues).toSeq)
    assertEquals(Seq(6.25D, 1D, 9.25D, 12D, 9.25D, 8D, 1D), mq.getAverageColumnSizes(values).toSeq)
  }

  @Test
  def testAverageColumnSizeOnUnion(): Unit = {
    assertEquals(Seq(8D, 32D), mq.getAverageColumnSizes(union).toSeq)
    assertEquals(Seq(8D, 32D), mq.getAverageColumnSizes(unionAll).toSeq)
  }

  @Test
  def testAverageColumnSizeOnUnionBatchExec(): Unit = {
    assertEquals(Seq(8D, 32D), mq.getAverageColumnSizes(unionBatchExec).toSeq)
  }

  @Test
  def testAverageColumnSizeOnJoin(): Unit = {
    assertEquals(Seq(4D, 8D, 8D, 32D, 4D, 32D), mq.getAverageColumnSizes(innerJoin).toSeq)
  }

  @Test
  def testAverageColumnSizeOnSemiJoin(): Unit = {
    assertEquals(Seq(4D, 8D), mq.getAverageColumnSizes(semiJoin).toSeq)
  }

  @Test
  def testAverageColumnSizeOnRank(): Unit = {
    assertEquals(Seq(8D, 32D, 4D, 32D, 8D), mq.getAverageColumnSizes(flinkLogicalRank).toSeq)
    assertEquals(Seq(8D, 32D, 4D, 32D, 8D), mq.getAverageColumnSizes(globalBatchExecRank).toSeq)
    assertEquals(Seq(8D, 32D, 4D, 32D), mq.getAverageColumnSizes(localBatchExecRank).toSeq)
    assertEquals(Seq(8D, 32D, 4D, 32D), mq.getAverageColumnSizes(streamExecRowNumber).toSeq)
  }

}
