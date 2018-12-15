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
import java.lang.{Long => JLong, Integer => JInt}
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
import org.apache.flink.table.api.{TableConfig, Types, ValidationException}
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin.TEMPORAL_JOIN_CONDITION
import org.apache.flink.table.plan.nodes.datastream.DataStreamTemporalJoinToCoProcessTranslator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowKeySelector
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, TestStreamQueryConfig}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.hamcrest.{CoreMatchers, Matcher}
import org.hamcrest.Matchers.{endsWith, startsWith}
import org.junit.Assert.assertTrue
import org.junit.Test

import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

class TemporalJoinHarnessTest extends HarnessTestBase {

  private val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

  private val tableConfig = new TableConfig

  private val queryConfig =
    new TestStreamQueryConfig(Time.milliseconds(2), Time.milliseconds(4))

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
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 0L: JLong)))

    // process (out of order) with conversion rates
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 2L: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(2L: JLong, "Euro", 2L: JLong, "Euro", 114L: JLong,
      1L: JLong)))

    // initiate conversion rates
    testHarness.processElement2(new StreamRecord(CRow.of("US Dollar", 102L: JLong, 1L: JLong)))
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 114L: JLong, 1L: JLong)))
    testHarness.processElement2(new StreamRecord(CRow.of("Yen", 1L: JLong, 1L: JLong)))

    // process again without conversion rates
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 0L: JLong)))

    // process with conversion rates
    testHarness.processElement1(new StreamRecord(CRow.of(1L: JLong, "US Dollar", 3L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(50L: JLong, "Yen", 4L: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(1L: JLong, "US Dollar", 3L: JLong, "US Dollar",
      102L: JLong, 1L: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(50L: JLong, "Yen", 4L: JLong, "Yen", 1L: JLong,
      1L: JLong)))

    // update Euro #1
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 116L: JLong, 5L: JLong)))

    // process with old Euro
    testHarness.processElement1(new StreamRecord(CRow.of(3L: JLong, "Euro", 4L: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(3L: JLong, "Euro", 4L: JLong, "Euro",
      114L: JLong, 1L: JLong)))

    // again update Euro #2
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 119L: JLong, 7L: JLong)))

    // process with updated Euro #1
    testHarness.processElement1(new StreamRecord(CRow.of(3L: JLong, "Euro", 5L: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(3L: JLong, "Euro", 5L: JLong, "Euro",
      116L: JLong, 5L: JLong)))

    // process US Dollar
    testHarness.processElement1(new StreamRecord(CRow.of(5L: JLong, "US Dollar", 7L: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(5L: JLong, "US Dollar", 7L: JLong, "US Dollar",
      102L: JLong, 1L: JLong)))

    assertTrue(testHarness.getOutput.isEmpty)
    testHarness.processWatermark1(new Watermark(10L: JLong))
    assertTrue(testHarness.getOutput.isEmpty)
    testHarness.processWatermark2(new Watermark(10L: JLong))
    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testEventsWithSameRowtime() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    val time1: JLong = 1L
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 112L: JLong, time1: JLong)))
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 114L: JLong, time1: JLong)))

    val time2: JLong = 2L
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", time2: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(22L: JLong, "Euro", time2: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(2L: JLong, "Euro", time2: JLong, "Euro",
      114L: JLong, time1: JLong)))
    expectedOutput.add(new StreamRecord(CRow.of(22L: JLong, "Euro", time2: JLong, "Euro",
      114L: JLong, time1: JLong)))

    testHarness.processWatermark1(new Watermark(time2: JLong))
    testHarness.processWatermark2(new Watermark(time2: JLong))
    verify(expectedOutput, testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testRowtimePickCorrectRowFromTemporalTable() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()

    val expectedOutput = processEuro(testHarness)

    testHarness.processWatermark1(new Watermark(10L: JLong))
    testHarness.processWatermark2(new Watermark(10L: JLong))
    verify(new util.LinkedList(expectedOutput.asJava), testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testRowtimeWatermarkHandling() {
    val testHarness = createTestHarness(new OrdersRatesRowtimeTemporalJoinInfo())

    testHarness.open()

    val expectedOutput = processEuro(testHarness)

    testHarness.processWatermark1(new Watermark(3L: JLong))
    testHarness.processWatermark2(new Watermark(2L: JLong))
    verify(new util.LinkedList(expectedOutput.slice(0, 2).asJava), testHarness.getOutput)

    testHarness.processWatermark1(new Watermark(12L: JLong))
    testHarness.processWatermark2(new Watermark(5L: JLong))
    verify(new util.LinkedList(expectedOutput.slice(0, 5).asJava), testHarness.getOutput)

    testHarness.processWatermark2(new Watermark(10L: JLong))
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

    testHarness.processWatermark1(new Watermark(9999L: JLong))
    testHarness.processWatermark2(new Watermark(9999L: JLong))

    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 10000L: JLong)))

    testHarness.processWatermark1(new Watermark(10000L: JLong))
    testHarness.processWatermark2(new Watermark(10000L: JLong))

    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 10000L: JLong, "Euro",
      9L: JLong, 9L: JLong))
    verify(new util.LinkedList(expectedOutput.asJava), testHarness.getOutput)

    testHarness.close()
  }

  def processEuro(
                   testHarness: KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow])
  : ArrayBuffer[Object] = {

    // process conversion rates
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 1L: JLong, 1L: JLong)))
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 3L: JLong, 3L: JLong)))
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 5L: JLong, 5L: JLong)))
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 7L: JLong, 7L: JLong)))
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 9L: JLong, 9L: JLong)))

    // process orders
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 0L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 1L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 2L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 3L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 4L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 5L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 6L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 7L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 8L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 9L: JLong)))
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", 10L: JLong)))

    var expectedOutput = new ArrayBuffer[Object]()
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 1L: JLong, "Euro",
      1L: JLong, 1L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 2L: JLong, "Euro",
      1L: JLong, 1L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 3L: JLong, "Euro",
      3L: JLong, 3L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 4L: JLong, "Euro",
      3L: JLong, 3L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 5L: JLong, "Euro",
      5L: JLong, 5L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 6L: JLong, "Euro",
      5L: JLong, 5L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 7L: JLong, "Euro",
      7L: JLong, 7L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 8L: JLong, "Euro",
      7L: JLong, 7L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 9L: JLong, "Euro",
      9L: JLong, 9L: JLong))
    expectedOutput += new StreamRecord(CRow.of(2L: JLong, "Euro", 10L: JLong, "Euro",
      9L: JLong, 9L: JLong))
    expectedOutput
  }

  @Test
  def testProctime() {
    val testHarness = createTestHarness(new OrdersRatesProctimeTemporalJoinInfo)

    testHarness.open()
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // process without conversion rates
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", null)))

    // initiate conversion rates
    testHarness.processElement2(new StreamRecord(CRow.of("US Dollar", 102L: JLong, null)))
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 114L: JLong, null)))
    testHarness.processElement2(new StreamRecord(CRow.of("Yen", 1L: JLong, null)))

    // process with conversion rates
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", null)))
    testHarness.processElement1(new StreamRecord(CRow.of(1L: JLong, "US Dollar", null)))
    testHarness.processElement1(new StreamRecord(CRow.of(50L: JLong, "Yen", null)))

    expectedOutput.add(new StreamRecord(CRow.of(2L: JLong, "Euro", null, "Euro",
      114L: JLong, null)))
    expectedOutput.add(new StreamRecord(CRow.of(1L: JLong, "US Dollar", null,
      "US Dollar", 102L: JLong, null)))
    expectedOutput.add(new StreamRecord(CRow.of(50L: JLong, "Yen", null, "Yen", 1L: JLong, null)))

    // update Euro
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 116L: JLong, null)))

    // process Euro
    testHarness.processElement1(new StreamRecord(CRow.of(3L: JLong, "Euro", null)))

    expectedOutput.add(new StreamRecord(CRow.of(3L: JLong, "Euro", null, "Euro",
      116L: JLong, null)))

    // again update Euro
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 119L: JLong, null)))

    // process US Dollar
    testHarness.processElement1(new StreamRecord(CRow.of(5L: JLong, "US Dollar", null)))

    expectedOutput.add(new StreamRecord(CRow.of(5L: JLong, "US Dollar", null, "US Dollar",
      102L: JLong, null)))

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
          *         leftInputRef(3) > rightInputRef(3)
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
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 114L: JLong, null, 42: JInt)))
    testHarness.processElement2(new StreamRecord(CRow.of("Yen", 1L: JLong, null, 42: JInt)))

    // process with conversion rates
    testHarness.processElement1(new StreamRecord(CRow.of(2L: JLong, "Euro", null, 0: JInt)))
    testHarness.processElement1(new StreamRecord(CRow.of(50L: JLong, "Yen", null, 44: JInt)))

    expectedOutput.add(new StreamRecord(CRow.of(50L: JLong, "Yen", null, 44: JInt, "Yen",
      1L: JLong, null, 42: JInt)))

    // update Euro
    testHarness.processElement2(new StreamRecord(CRow.of("Euro", 116L: JLong, null, 44: JInt)))

    // process Euro
    testHarness.processElement1(new StreamRecord(CRow.of(3L: JLong, "Euro", null, 42: JInt)))
    testHarness.processElement1(new StreamRecord(CRow.of(4L: JLong, "Euro", null, 44: JInt)))
    testHarness.processElement1(new StreamRecord(CRow.of(5L: JLong, "Euro", null, 1337: JInt)))

    expectedOutput.add(new StreamRecord(CRow.of(5L: JLong, "Euro", null, 1337: JInt, "Euro",
      116L: JLong, null, 44: JInt)))

    // process US Dollar
    testHarness.processElement1(new StreamRecord(CRow.of(5L: JLong, "US Dollar", null, 1337: JInt)))

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

  def translateJoin(joinInfo: TemporalJoinInfo, joinRelType: JoinRelType = JoinRelType.INNER)
  : (CRowKeySelector, CRowKeySelector, TwoInputStreamOperator[CRow, CRow, CRow]) = {

    val leftType = joinInfo.leftRowType
    val rightType = joinInfo.rightRowType
    val joinType = new RowTypeInfo(
      leftType.getFieldTypes ++ rightType.getFieldTypes,
      leftType.getFieldNames ++ rightType.getFieldNames)

    val joinTranslator = DataStreamTemporalJoinToCoProcessTranslator.create(
      "TemporalJoin",
      tableConfig,
      joinType,
      new RowSchema(typeFactory.createTypeFromTypeInfo(leftType, false)),
      new RowSchema(typeFactory.createTypeFromTypeInfo(rightType, false)),
      joinInfo,
      rexBuilder)

    val joinOperator = joinTranslator.getJoinOperator(
      joinRelType,
      joinType.getFieldNames,
      "TemporalJoin",
      queryConfig)

    (joinTranslator.getLeftKeySelector(),
      joinTranslator.getRightKeySelector(),
      joinOperator)
  }

  abstract class TemporalJoinInfo(
                                   val leftRowType: RowTypeInfo,
                                   val rightRowType: RowTypeInfo,
                                   leftKeys: ImmutableIntList,
                                   rightKeys: ImmutableIntList)
    extends JoinInfo(leftKeys, rightKeys) {

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
