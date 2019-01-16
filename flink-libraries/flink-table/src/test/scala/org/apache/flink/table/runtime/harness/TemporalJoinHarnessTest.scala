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

import java.lang.{Integer => JInt, Long => JLong}
import java.sql.Timestamp
import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, TableEnvironment, Types, ValidationException}
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin.TEMPORAL_JOIN_CONDITION
import org.apache.flink.table.plan.nodes.datastream.DataStreamTemporalJoinToCoProcessTranslator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowKeySelector
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, TestStreamQueryConfig}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row
import org.hamcrest.CoreMatchers
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

  private val ordersRowtimeFields = Array("o_amount", ORDERS_KEY, ORDERS_ROWTIME)

  private val ordersProctimeFields = Array("o_amount", ORDERS_KEY, ORDERS_PROCTIME)

  private val ratesRowtimeFields = Array(RATES_KEY, "r_rate", RATES_ROWTIME)

  private val ratesProctimeFields = Array(RATES_KEY, "r_rate", "r_proctime")

  private val ordersProctimeType = new RowTypeInfo(
    Array[TypeInformation[_]](
      Types.LONG,
      Types.STRING,
      TimeIndicatorTypeInfo.PROCTIME_INDICATOR),
    ordersProctimeFields)

  private val ratesProctimeType = new RowTypeInfo(
    Array[TypeInformation[_]](
      Types.STRING,
      Types.LONG,
      TimeIndicatorTypeInfo.PROCTIME_INDICATOR),
    ratesProctimeFields)

  private val rexBuilder = new RexBuilder(typeFactory)

  @Test
  def testRowtime() {
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

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
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

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
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

    val expectedOutput = processEuro(testHarness)

    testHarness.processWatermark1(new Watermark(10L))
    testHarness.processWatermark2(new Watermark(10L))
    verify(new util.LinkedList(expectedOutput.asJava), testHarness.getOutput)

    testHarness.close()
  }

  @Test
  def testRowtimeWatermarkHandling() {
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

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
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

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
    val testHarness = createTestHarness[(JLong, String), (String, JLong)](
      isRowtime = false,
      ordersProctimeFields,
      ratesProctimeFields)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

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
    val sql =
      s"""
         |SELECT *
         |FROM
         |  Orders as o,
         |  LATERAL TABLE (Rates(o.$ORDERS_PROCTIME)) AS r
         |WHERE r.$RATES_KEY = o.$ORDERS_KEY AND o_foo > r_bar
         |""".stripMargin

    val testHarness = createTestHarness[(JLong, String, JInt), (String, JLong, JInt)](
      isRowtime = false,
      Array("o_amount", ORDERS_KEY, "o_foo", ORDERS_PROCTIME),
      Array(RATES_KEY, "r_rate", "r_bar", "r_proctime"),
      sql = Some(sql))

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // initiate conversion rates
    testHarness.processElement2(new StreamRecord(CRow("Euro", 114L, 42, null)))
    testHarness.processElement2(new StreamRecord(CRow("Yen", 1L, 42, null)))

    // process with conversion rates
    testHarness.processElement1(new StreamRecord(CRow(2L, "Euro", 0, null)))
    testHarness.processElement1(new StreamRecord(CRow(50L, "Yen", 44, null)))

    expectedOutput.add(new StreamRecord(CRow(50L, "Yen", 44, null, "Yen", 1L, 42, null)))

    // update Euro
    testHarness.processElement2(new StreamRecord(CRow("Euro", 116L, 44, null)))

    // process Euro
    testHarness.processElement1(new StreamRecord(CRow(3L, "Euro", 42, null)))
    testHarness.processElement1(new StreamRecord(CRow(4L, "Euro", 44, null)))
    testHarness.processElement1(new StreamRecord(CRow(5L, "Euro", 1337, null)))

    expectedOutput.add(new StreamRecord(CRow(5L, "Euro", 1337, null, "Euro", 116L, 44, null)))

    // process US Dollar
    testHarness.processElement1(new StreamRecord(CRow(5L, "US Dollar", 1337, null)))

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

  def createTestHarness[IN1: TypeInformation, IN2: TypeInformation](
      isRowtime: Boolean,
      ordersFields: Seq[String],
      ratesFields: Seq[String],
      ordersKey: String = ORDERS_KEY,
      ratesKey: String = RATES_KEY,
      sql: Option[String] = None
  ): KeyedTwoInputStreamOperatorTestHarness[String, CRow, CRow, CRow] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    if (isRowtime) {
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    }
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ordersFieldExprs = ordersFields.map(f => symbol2FieldExpression(Symbol(f)))
    val orders = env.fromCollection(Seq[IN1]()).toTable(
      tEnv,
      ordersFieldExprs.dropRight(1) :+
        (if (isRowtime) ordersFieldExprs.last.rowtime else ordersFieldExprs.last.proctime): _*)
    tEnv.registerTable("Orders", orders)

    val ratesFieldExprs = ratesFields.map(f => symbol2FieldExpression(Symbol(f)))
    val rates = env.fromCollection(Seq[IN2]()).toTable(
      tEnv,
      ratesFieldExprs.dropRight(1) :+
        (if (isRowtime) ratesFieldExprs.last.rowtime else ratesFieldExprs.last.proctime): _*)
    tEnv.registerTable("RatesTable", rates)
    tEnv.registerFunction(
      "Rates",
      tEnv.scan("RatesTable").createTemporalTableFunction(ratesFields.last, ratesKey))

    val sqlQuery = tEnv.sqlQuery(sql.getOrElse(
      s"""
         |SELECT *
         |FROM
         |  Orders as o,
         |  LATERAL TABLE (Rates(o.${ordersFields.last})) AS r
         |WHERE r.$ratesKey = o.$ordersKey
         |""".stripMargin))

    val testHarness = createHarnessTester[String, CRow, CRow, CRow](
      tEnv.toAppendStream[Row](sqlQuery, queryConfig), "InnerJoin")

    testHarness.setStateBackend(getStateBackend)
    testHarness.open()

    testHarness
  }

  // ---------------------- Row time TTL tests ----------------------

  @Test
  def testRowTimeJoinCleanupTimerUpdatedFromProbeSide(): Unit = {
    // min=2ms max=4ms
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

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
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

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
    val testHarness = createTestHarness[(JLong, String, Timestamp), (String, JLong, Timestamp)](
      isRowtime = true,
      ordersRowtimeFields,
      ratesRowtimeFields)

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
    val testHarness = createTestHarness[(JLong, String), (String, JLong)](
      isRowtime = false,
      ordersProctimeFields,
      ratesProctimeFields)

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
    val testHarness = createTestHarness[(JLong, String), (String, JLong)](
      isRowtime = false,
      ordersProctimeFields,
      ratesProctimeFields)

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
}
