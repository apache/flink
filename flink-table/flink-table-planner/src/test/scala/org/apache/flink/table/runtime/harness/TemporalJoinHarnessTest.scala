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
package org.apache.flink.table.runtime.harness

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin.TEMPORAL_JOIN_CONDITION
import org.apache.flink.table.plan.nodes.datastream.DataStreamTemporalJoinToCoProcessTranslator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowKeySelector
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, TestTableConfig}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import com.google.common.collect.ImmutableList
import org.hamcrest.CoreMatchers
import org.hamcrest.Matchers.{endsWith, startsWith}
import org.junit.Assert.assertTrue
import org.junit.Test

import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

class TemporalJoinHarnessTest extends HarnessTestBase {

  private val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

  private val config = new TestTableConfig
  config.setIdleStateRetentionTime(Time.milliseconds(2), Time.milliseconds(4))

  private val ORDERS_KEY = "o_currency"

  private val ORDERS_PROCTIME = "o_proctime"

  private val ORDERS_ROWTIME = "o_rowtime"

  private val RATES_KEY = "r_currency"

  private val RATES_ROWTIME = "r_rowtime"

  private val ordersRowtimeType = new RowTypeInfo(
    Array[TypeInformation[_]](
      Types.LONG,
      Types.STRING,
      TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
    Array("o_amount", ORDERS_KEY, ORDERS_ROWTIME))

  private val ordersProctimeType = new RowTypeInfo(
    Array[TypeInformation[_]](
      Types.LONG,
      Types.STRING,
      TimeIndicatorTypeInfo.PROCTIME_INDICATOR),
    Array("o_amount", ORDERS_KEY, ORDERS_PROCTIME))

  private val ratesRowtimeType = new RowTypeInfo(
    Array[TypeInformation[_]](
      Types.STRING,
      Types.LONG,
      TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
    Array(RATES_KEY, "r_rate", RATES_ROWTIME))

  private val ratesProctimeType = new RowTypeInfo(
    Array[TypeInformation[_]](
      Types.STRING,
      Types.LONG,
      TimeIndicatorTypeInfo.PROCTIME_INDICATOR),
    Array(RATES_KEY, "r_rate", "r_proctime"))

  private val joinRowtimeType = new RowTypeInfo(
    ordersRowtimeType.getFieldTypes ++ ratesRowtimeType.getFieldTypes,
    ordersRowtimeType.getFieldNames ++ ratesRowtimeType.getFieldNames)

  private val rexBuilder = new RexBuilder(typeFactory)

  @Test
  def testRowtime() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // process without conversion rates
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 0L)))

    // process (out of order) with conversion rates
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 2L)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 2L, "Euro", 114L, 1L)))

    // initiate conversion rates
    testHarness.processElement2(new StreamRecord(CRow("US Dollar", 102L, 1L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, 1L)))
    testHarness.processElement2(new StreamRecord(CRow("Yen", 1L, 1L)))

    // process again without conversion rates
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 0L)))

    // process with conversion rates
    testHarness.processElement1(new StreamRecord(CRow(1L, "US Dollar", 3L)))
    testHarness.processElement1(new StreamRecord(CRow(50L, "Yen", 4L)))
    expectedOutput.add(new StreamRecord(CRow(1L, "US Dollar", 3L, "US Dollar", 102L, 1L)))
    expectedOutput.add(new StreamRecord(CRow(50L, "Yen", 4L, "Yen", 1L, 1L)))

    // update Euro #1
    testHarness.processElement2(new StreamRecord(CRow("Euro", 116L, 5L)))

    // process with old Euro
    testHarness.processElement1(new StreamRecord(CRow(3L, "Euro", 4L)))
    expectedOutput.add(new StreamRecord(CRow(3L, "Euro", 4L, "Euro", 114L, 1L)))

    // again update Euro #2
    testHarness.processElement2(new StreamRecord(CRow("Euro", 119L, 7L)))

    // process with updated Euro #1
    testHarness.processElement1(new StreamRecord(CRow(3L, "Euro", 5L)))
    expectedOutput.add(new StreamRecord(CRow(3L, "Euro", 5L, "Euro", 116L, 5L)))

    // process US Dollar
    testHarness.processElement1(new StreamRecord(CRow(5L, "US Dollar", 7L)))
    expectedOutput.add(new StreamRecord(CRow(5L, "US Dollar", 7L, "US Dollar", 102L, 1L)))

    assertTrue(testHarness.getOutput.isEmpty)
    testHarness.processWatermark1(new Watermark(10L))
    assertTrue(testHarness.getOutput.isEmpty)
    testHarness.processWatermark2(new Watermark(10L))
    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testEventsWithSameRowtime() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    val time1 = 1L
    testHarness.processElement2(new StreamRecord(CRow("Euro", 112L, time1)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, time1)))

    val time2 = 2L
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", time2)))
    testHarness.processElement1(new StreamRecord(CRow(22L, "Euro", time2)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", time2, "Euro", 114L, time1)))
    expectedOutput.add(new StreamRecord(CRow(22L, "Euro", time2, "Euro", 114L, time1)))

    testHarness.processWatermark1(new Watermark(time2))
    testHarness.processWatermark2(new Watermark(time2))
    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testRowtimePickCorrectRowFromTemporalTable() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()

    val expectedOutput = processEuro(testHarness)

    testHarness.processWatermark1(new Watermark(10L))
    testHarness.processWatermark2(new Watermark(10L))
    verify(new util.LinkedList(expectedOutput.asJava), testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testRowtimeWatermarkHandling() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()

    val expectedOutput = processEuro(testHarness)

    testHarness.processWatermark1(new Watermark(3L))
    testHarness.processWatermark2(new Watermark(2L))
    verify(new util.LinkedList(expectedOutput.slice(0, 2).asJava), testHarness.getOutput)

    testHarness.processWatermark1(new Watermark(12L))
    testHarness.processWatermark2(new Watermark(5L))
    verify(new util.LinkedList(expectedOutput.slice(0, 5).asJava), testHarness.getOutput)

    testHarness.processWatermark2(new Watermark(10L))
    verify(new util.LinkedList(expectedOutput.asJava), testHarness.getOutput)

    testHarness.close()
  }

  /**
    * Cleaning up the state when processing watermark exceeding all events should always keep
    * one latest event in TemporalTable.
    */
  @Test
  def testRowtimeStateCleanUpShouldAlwaysKeepOneLatestRow() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()

    val expectedOutput = processEuro(testHarness)

    testHarness.processWatermark1(new Watermark(9999L))
    testHarness.processWatermark2(new Watermark(9999L))

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 10000L)))

    testHarness.processWatermark1(new Watermark(10000L))
    testHarness.processWatermark2(new Watermark(10000L))

    expectedOutput += new StreamRecord(CRow(2L, "Euro", 10000L, "Euro", 9L, 9L))
    verify(new util.LinkedList(expectedOutput.asJava), testHarness.getOutput)
    
    testHarness.close()
  }

  def processEuro(
    testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow])
  : ArrayBuffer[Object] = {

    // process conversion rates
    testHarness.processElement2(new StreamRecord(CRow("Euro", 1L, 1L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 3L, 3L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 5L, 5L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 7L, 7L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 9L, 9L)))

    // process orders
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 0L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 1L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 2L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 3L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 4L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 5L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 6L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 7L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 8L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 9L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 10L)))

    var expectedOutput = new ArrayBuffer[Object]()
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 1L, "Euro", 1L, 1L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 2L, "Euro", 1L, 1L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 3L, "Euro", 3L, 3L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 4L, "Euro", 3L, 3L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 5L, "Euro", 5L, 5L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 6L, "Euro", 5L, 5L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 7L, "Euro", 7L, 7L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 8L, "Euro", 7L, 7L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 9L, "Euro", 9L, 9L))
    expectedOutput += new StreamRecord(CRow(2L, "Euro", 10L, "Euro", 9L, 9L))
    expectedOutput
  }

  @Test
  def testProctime() {
    val testHarness = createTestHarness(new OrdersRatesProctimeTemporalJoinInfo)

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // process without conversion rates
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", null)))

    // initiate conversion rates
    testHarness.processElement2(new StreamRecord(CRow("US Dollar", 102L, null)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, null)))
    testHarness.processElement2(new StreamRecord(CRow("Yen", 1L, null)))

    // process with conversion rates
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", null)))
    testHarness.processElement1(new StreamRecord(CRow(1L, "US Dollar", null)))
    testHarness.processElement1(new StreamRecord(CRow(50L, "Yen", null)))

    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", null, "Euro", 114L, null)))
    expectedOutput.add(new StreamRecord(CRow(1L, "US Dollar", null, "US Dollar", 102L, null)))
    expectedOutput.add(new StreamRecord(CRow(50L, "Yen", null, "Yen", 1L, null)))

    // update Euro
    testHarness.processElement2(new StreamRecord(CRow("Euro", 116L, null)))

    // process Euro
    testHarness.processElement1(new StreamRecord(CRow(3L, "Euro", null)))

    expectedOutput.add(new StreamRecord(CRow(3L, "Euro", null, "Euro", 116L, null)))

    // again update Euro
    testHarness.processElement2(new StreamRecord(CRow("Euro", 119L, null)))

    // process US Dollar
    testHarness.processElement1(new StreamRecord(CRow(5L, "US Dollar", null)))

    expectedOutput.add(new StreamRecord(CRow(5L, "US Dollar", null, "US Dollar", 102L, null)))

    verify(expectedOutput, testHarness.getOutput, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonEquiProctime() {
    val testHarness = createTestHarness(
      new ProctimeTemporalJoinInfo(
        new RowTypeInfo(
          ordersProctimeType.getFieldTypes :+ Types.INT,
          ordersProctimeType.getFieldNames :+ "foo"),
        new RowTypeInfo(
          ratesProctimeType.getFieldTypes :+ Types.INT,
          ratesProctimeType.getFieldNames :+ "bar"),
        ORDERS_KEY,
        RATES_KEY,
        ORDERS_PROCTIME) {
        /**
          * @return [[LogicalTemporalTableJoin.TEMPORAL_JOIN_CONDITION]](...) AND
          *        leftInputRef(3) > rightInputRef(3)
          */
        override def getRemaining(rexBuilder: RexBuilder): RexNode = {
          rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            super.getRemaining(rexBuilder),
            rexBuilder.makeCall(
              SqlStdOperatorTable.GREATER_THAN,
              makeLeftInputRef("foo"),
              makeRightInputRef("bar")))
        }
      })

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // initiate conversion rates
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, null, 42)))
    testHarness.processElement2(new StreamRecord(CRow("Yen", 1L, null, 42)))

    // process with conversion rates
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", null, 0)))
    testHarness.processElement1(new StreamRecord(CRow(50L, "Yen", null, 44)))

    expectedOutput.add(new StreamRecord(CRow(50L, "Yen", null, 44, "Yen", 1L, null, 42)))

    // update Euro
    testHarness.processElement2(new StreamRecord(CRow("Euro", 116L, null, 44)))

    // process Euro
    testHarness.processElement1(new StreamRecord(CRow(3L, "Euro", null, 42)))
    testHarness.processElement1(new StreamRecord(CRow(4L, "Euro", null, 44)))
    testHarness.processElement1(new StreamRecord(CRow(5L, "Euro", null, 1337)))

    expectedOutput.add(new StreamRecord(CRow(5L, "Euro", null, 1337, "Euro", 116L, null, 44)))

    // process US Dollar
    testHarness.processElement1(new StreamRecord(CRow(5L, "US Dollar", null, 1337)))

    verify(expectedOutput, testHarness.getOutput, new RowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testMissingTemporalJoinCondition() {
    expectedException.expect(classOf[IllegalStateException])
    expectedException.expectMessage(startsWith(s"Missing ${TEMPORAL_JOIN_CONDITION.getName}"))

    translateJoin(new TemporalJoinInfo(
      ordersProctimeType,
      ratesProctimeType,
      ORDERS_KEY,
      RATES_KEY) {

      override def isEqui: Boolean = true

      override def getRemaining(rexBuilder: RexBuilder): RexNode = rexBuilder.makeLiteral(true)
    })
  }

  @Test
  def testNonEquiMissingTemporalJoinCondition() {
    expectedException.expect(classOf[IllegalStateException])
    expectedException.expectMessage(startsWith(s"Missing ${TEMPORAL_JOIN_CONDITION.getName}"))

    translateJoin(new TemporalJoinInfo(
      ordersProctimeType,
      ratesProctimeType,
      ORDERS_KEY,
      RATES_KEY) {

      override def isEqui: Boolean = true

      override def getRemaining(rexBuilder: RexBuilder): RexNode = {
        rexBuilder.makeCall(
          SqlStdOperatorTable.GREATER_THAN,
          rexBuilder.makeCall(
            SqlStdOperatorTable.CONCAT,
            rexBuilder.makeLiteral("A"),
            makeLeftInputRef(ORDERS_KEY)),
          makeRightInputRef(RATES_KEY))
      }
    })
  }

  @Test
  def testTwoTemporalJoinConditions() {
    expectedException.expect(classOf[IllegalStateException])
    expectedException.expectMessage(startsWith(s"Multiple $TEMPORAL_JOIN_CONDITION functions"))

    translateJoin(
      new OrdersRatesProctimeTemporalJoinInfo() {
        override def getRemaining(rexBuilder: RexBuilder): RexNode = {
          rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            super.getRemaining(rexBuilder),
            super.getRemaining(rexBuilder))
        }
      })
  }

  @Test
  def testIncorrectTemporalJoinCondition() {
    expectedException.expect(classOf[IllegalStateException])
    expectedException.expectMessage(startsWith(s"Unsupported invocation"))

    translateJoin(
      new OrdersRatesProctimeTemporalJoinInfo() {
        override def getRemaining(rexBuilder: RexBuilder): RexNode = {
          rexBuilder.makeCall(
            TEMPORAL_JOIN_CONDITION,
            makeLeftInputRef(leftKey),
            makeLeftInputRef(leftKey),
            makeLeftInputRef(leftKey),
            makeRightInputRef(rightKey))
        }
      })
  }

  @Test
  def testUnsupportedPrimaryKeyInTemporalJoinCondition() {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      CoreMatchers.allOf[String](
        startsWith("Unsupported expression"),
        endsWith("Expected input reference")))

    translateJoin(
      new OrdersRatesProctimeTemporalJoinInfo() {
        override def getRemaining(rexBuilder: RexBuilder): RexNode = {
          LogicalTemporalTableJoin.makeProcTimeTemporalJoinConditionCall(
            rexBuilder,
            makeLeftInputRef(leftTimeAttribute),
            rexBuilder.makeCall(
              SqlStdOperatorTable.CONCAT,
              rexBuilder.makeLiteral("A"),
              makeRightInputRef(RATES_KEY)))
        }
      })
  }

  @Test
  def testMultipleJoinKeys() {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(startsWith(s"Only single column join key"))

    translateJoin(
      new TemporalJoinInfo(
        ordersProctimeType,
        ratesProctimeType,
        ImmutableIntList.of(0, 1),
        ImmutableIntList.of(1, 0)) {

        override def getRemaining(rexBuilder: RexBuilder): RexNode = {
          LogicalTemporalTableJoin.makeProcTimeTemporalJoinConditionCall(
            rexBuilder,
            makeLeftInputRef(ORDERS_PROCTIME),
            makeRightInputRef(RATES_KEY))
        }
      })
  }

  @Test
  def testNonInnerJoin() {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(startsWith(s"Only ${JoinRelType.INNER} temporal join"))

    translateJoin(new OrdersRatesProctimeTemporalJoinInfo, JoinRelType.FULL)
  }

  def createTestHarness(temporalJoinInfo: TemporalJoinInfo)
    : KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] = {

    val (leftKeySelector, rightKeySelector, joinOperator) =
      translateJoin(temporalJoinInfo)

    new KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow](
      joinOperator,
      leftKeySelector.asInstanceOf[KeySelector[CRow, String]],
      rightKeySelector.asInstanceOf[KeySelector[CRow, String]],
      BasicTypeInfo.STRING_TYPE_INFO,
      1,
      1,
      0)
  }

  // ---------------------- Row time TTL tests ----------------------

  @Test
  def testRowTimeJoinCleanupTimerUpdatedFromProbeSide(): Unit = {
    // min=2ms max=4ms
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.setProcessingTime(1L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 1L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, 0L)))

    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 1L, "Euro", 114L, 0L)))

    testHarness.processBothWatermarks(new Watermark(2L))

    // this should update the clean-up timer to 8
    testHarness.setProcessingTime(4L)
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 4L)))

    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 4L, "Euro", 114L, 0L)))

    // this should now do nothing (also it does not update the timer as 5 + 2ms (min) < 8)
    testHarness.setProcessingTime(5L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 5L)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 5L, "Euro", 114L, 0L)))

    testHarness.processBothWatermarks(new Watermark(5L))

    // this should now clean up the state
    testHarness.setProcessingTime(8L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 7L))) // this should find no match

    testHarness.processBothWatermarks(new Watermark(10L))

    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testRowTimeJoinCleanupTimerUpdatedFromBuildSide(): Unit = {
    // min=2ms max=4ms
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.setProcessingTime(1L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 1L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, 0L)))

    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 1L, "Euro", 114L, 0L)))

    testHarness.processBothWatermarks(new Watermark(2L))

    // this should update the clean-up timer to 8
    testHarness.setProcessingTime(4L)
    testHarness.processElement2(new StreamRecord(CRow("Euro", 117L, 4L)))

    // this should now do nothing
    testHarness.setProcessingTime(5L)

    // so this should be joined with the "old" value
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 3L)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 3L, "Euro", 114L, 0L)))

    testHarness.processBothWatermarks(new Watermark(5L))

    // this should now clean up the state
    testHarness.setProcessingTime(8L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 7L))) // this should find no match

    testHarness.processBothWatermarks(new Watermark(10L))

    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testRowTimeJoinCleanupTimerUpdatedAfterEvaluation(): Unit = {
    // min=2ms max=4ms
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.setProcessingTime(1L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 1L)))
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, 0L)))

    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 1L, "Euro", 114L, 0L)))

    testHarness.setProcessingTime(4L)

    // this should trigger an evaluation, which should also update the clean-up timer to 8
    testHarness.processBothWatermarks(new Watermark(2L))

    // this should now do nothing (also it does not update the timer as 5 + 2ms (min) < 8)
    testHarness.setProcessingTime(5L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 3L)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 3L, "Euro", 114L, 0L)))

    testHarness.processBothWatermarks(new Watermark(5L))

    // this should now clean up the state
    testHarness.setProcessingTime(8L)

    // so this should not find any match
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 7L)))

    testHarness.processBothWatermarks(new Watermark(10L))

    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  // ---------------------- Processing time TTL tests ----------------------

  @Test
  def testProcessingTimeJoinCleanupTimerUpdatedFromProbeSide(): Unit = {
    // min=2ms max=4ms
    val testHarness = createTestHarness(new OrdersRatesProctimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.setProcessingTime(1L)

    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, 0L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 1L)))

    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 1L, "Euro", 114L, 0L)))

    // this should push further the clean-up the state
    testHarness.setProcessingTime(4L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 6L)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 6L, "Euro", 114L, 0L)))

    // this should do nothing
    testHarness.setProcessingTime(5L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 8L)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 8L, "Euro", 114L, 0L)))

    // this should clean up the state
    testHarness.setProcessingTime(8L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 10L)))

    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testProcessingTimeJoinCleanupTimerUpdatedFromBuildSide(): Unit = {
    // min=2ms max=4ms
    val testHarness = createTestHarness(new OrdersRatesProctimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.setProcessingTime(1L)

    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, 0L)))
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 1L)))

    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 1L, "Euro", 114L, 0L)))

    // this should push further the clean-up the state
    testHarness.setProcessingTime(4L)

    testHarness.processElement2(new StreamRecord(CRow("Euro", 116L, 1L)))

    // this should do nothing
    testHarness.setProcessingTime(5L)

    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 6L)))
    expectedOutput.add(new StreamRecord(CRow(2L, "Euro", 6L, "Euro", 116L, 1L)))

    // this should clean up the state
    testHarness.setProcessingTime(8L)

    // so this should find no match
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 10L)))

    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  def translateJoin(joinInfo: TemporalJoinInfo, joinRelType: JoinRelType = JoinRelType.INNER)
    : (CRowKeySelector, CRowKeySelector, TwoInputStreamOperator[CRow, CRow, CRow]) = {

    val leftType = joinInfo.leftRowType
    val rightType = joinInfo.rightRowType
    val joinType = new RowTypeInfo(
      leftType.getFieldTypes ++ rightType.getFieldTypes,
      leftType.getFieldNames ++ rightType.getFieldNames)

    val joinTranslator = DataStreamTemporalJoinToCoProcessTranslator.create(
      "TemporalJoin",
      config,
      joinType,
      new RowSchema(typeFactory.createTypeFromTypeInfo(leftType, false)),
      new RowSchema(typeFactory.createTypeFromTypeInfo(rightType, false)),
      joinInfo,
      rexBuilder)

    val joinOperator = joinTranslator.getJoinOperator(
      joinRelType,
      joinType.getFieldNames,
      "TemporalJoin")

    (joinTranslator.getLeftKeySelector(),
      joinTranslator.getRightKeySelector(),
      joinOperator)
  }

  abstract class TemporalJoinInfo(
      val leftRowType: RowTypeInfo,
      val rightRowType: RowTypeInfo,
      leftKeys: ImmutableIntList,
      rightKeys: ImmutableIntList)
    extends JoinInfo(leftKeys, rightKeys, ImmutableList.of[RexNode]()) {

    def this(
      leftRowType: RowTypeInfo,
      rightRowType: RowTypeInfo,
      leftKey: String,
      rightKey: String) =
      this(
        leftRowType,
        rightRowType,
        ImmutableIntList.of(leftRowType.getFieldIndex(leftKey)),
        ImmutableIntList.of(rightRowType.getFieldIndex(rightKey)))

    override def isEqui: Boolean = false

    def makeLeftInputRef(leftField: String): RexNode = {
      rexBuilder.makeInputRef(
        typeFactory.createTypeFromTypeInfo(leftRowType.getTypeAt(leftField), false),
        leftRowType.getFieldIndex(leftField))
    }

    def makeRightInputRef(rightField: String): RexNode = {
      rexBuilder.makeInputRef(
        typeFactory.createTypeFromTypeInfo(rightRowType.getTypeAt(rightField), false),
        rightRowType.getFieldIndex(rightField) + leftRowType.getFieldTypes.length)
    }
  }

  class OrdersRatesProctimeTemporalJoinInfo()
    extends ProctimeTemporalJoinInfo(
      ordersProctimeType,
      ratesProctimeType,
      ORDERS_KEY,
      RATES_KEY,
      ORDERS_PROCTIME)

  class ProctimeTemporalJoinInfo(
      leftRowType: RowTypeInfo,
      rightRowType: RowTypeInfo,
      val leftKey: String,
      val rightKey: String,
      val leftTimeAttribute: String)
    extends TemporalJoinInfo(leftRowType, rightRowType, leftKey, rightKey) {

    override def getRemaining(rexBuilder: RexBuilder): RexNode = {
      LogicalTemporalTableJoin.makeProcTimeTemporalJoinConditionCall(
        rexBuilder,
        makeLeftInputRef(leftTimeAttribute),
        makeRightInputRef(rightKey))
    }
  }

  class OrdersRatesRowtimeTemporalJoinInfo()
    extends RowtimeTemporalJoinInfo(
      ordersRowtimeType,
      ratesRowtimeType,
      ORDERS_KEY,
      RATES_KEY,
      ORDERS_ROWTIME,
      RATES_ROWTIME)

  class RowtimeTemporalJoinInfo(
      leftRowType: RowTypeInfo,
      rightRowType: RowTypeInfo,
      leftKey: String,
      rightKey: String,
      leftTimeAttribute: String,
      rightTimeAttribute: String)
    extends TemporalJoinInfo(
      leftRowType,
      rightRowType,
      leftKey,
      rightKey) {
    override def getRemaining(rexBuilder: RexBuilder): RexNode = {
      LogicalTemporalTableJoin.makeRowTimeTemporalJoinConditionCall(
        rexBuilder,
        makeLeftInputRef(leftTimeAttribute),
        makeRightInputRef(rightTimeAttribute),
        makeRightInputRef(rightKey))
    }
  }

  class MissingTemporalJoinConditionJoinInfo(
      leftRowType: RowTypeInfo,
      rightRowType: RowTypeInfo,
      leftKey: String,
      rightKey: String,
      isEquiJoin: Boolean)
    extends TemporalJoinInfo(leftRowType, rightRowType, leftKey, rightKey) {

    override def isEqui: Boolean = isEquiJoin

    override def getRemaining(rexBuilder: RexBuilder): RexNode = if (isEquiJoin) {
      rexBuilder.makeLiteral(true)
    }
    else {
      rexBuilder.makeCall(
        SqlStdOperatorTable.GREATER_THAN,
        rexBuilder.makeCall(
          SqlStdOperatorTable.CONCAT,
          rexBuilder.makeLiteral("A"),
          makeLeftInputRef(leftKey)),
        makeRightInputRef(rightKey))
    }
  }
}
