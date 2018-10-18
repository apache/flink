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
import org.hamcrest.Matchers.startsWith
import org.junit.Test

class TemporalJoinHarnessTest extends HarnessTestBase {

  private val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

  private val tableConfig = new TableConfig

  private val queryConfig =
    new TestStreamQueryConfig(Time.milliseconds(2), Time.milliseconds(4))

  private val ORDERS_KEY = "o_currency"

  private val ORDERS_PROCTIME = "o_proctime"

  private val RATES_KEY = "r_currency"

  private val ordersRowtimeType = new RowTypeInfo(
    Array[TypeInformation[_]](
      Types.LONG,
      Types.STRING,
      TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
    Array("o_amount", ORDERS_KEY, "o_rowtime"))

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
    Array(RATES_KEY, "r_rate", "r_rowtime"))

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
    expectedException.expectMessage(startsWith("Unsupported right primary key expression"))

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
