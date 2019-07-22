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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkContextImpl, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.plan.schema._
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.{JDouble, JLong}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{AbstractRelOptPlanner, RelOptCluster}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.{DateString, TimeString, TimestampString}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{Before, BeforeClass, Test}
import org.powermock.api.mockito.PowerMockito._
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner

import java.math.BigDecimal

import scala.collection.JavaConverters._

/**
  * Tests for [[SelectivityEstimator]].
  *
  * We use PowerMockito instead of Mockito here, because [[TableScan#getRowType]] is a final method.
  */
@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[TableScan]))
class SelectivityEstimatorTest {
  private val allFieldNames = Seq("name", "amount", "price", "flag", "partition",
    "date_col", "time_col", "timestamp_col")
  private val allFieldTypes = Seq(VARCHAR, INTEGER, DOUBLE, BOOLEAN, VARCHAR,
    DATE, TIME, TIMESTAMP)
  val (name_idx, amount_idx, price_idx, flag_idx, partition_idx,
  date_idx, time_idx, timestamp_idx) = (0, 1, 2, 3, 4, 5, 6, 7)

  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  var rexBuilder = new RexBuilder(typeFactory)
  val relDataType: RelDataType = typeFactory.createStructType(
    allFieldTypes.map(typeFactory.createSqlType).asJava,
    allFieldNames.asJava)

  val mq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()
  var scan: TableScan = _

  @Before
  def setup(): Unit = {
    scan = mockScan()
  }

  private def mockScan(
      statistic: FlinkStatistic = FlinkStatistic.UNKNOWN,
      isFilterPushedDown: Boolean = false,
      tableConfig: TableConfig = TableConfig.getDefault): TableScan = {
    val tableScan = mock(classOf[TableScan])
    val cluster = mock(classOf[RelOptCluster])
    val planner = mock(classOf[AbstractRelOptPlanner])
    val catalogManager = mock(classOf[CatalogManager])
    val functionCatalog = new FunctionCatalog(catalogManager)
    val context: FlinkContext = new FlinkContextImpl(tableConfig, functionCatalog)
    when(tableScan, "getCluster").thenReturn(cluster)
    when(cluster, "getRexBuilder").thenReturn(rexBuilder)
    when(cluster, "getPlanner").thenReturn(planner)
    when(planner, "getContext").thenReturn(context)
    when(tableScan, "getRowType").thenReturn(relDataType)
    val innerTable = mock(classOf[TableSourceTable[_]])
    val flinkTable = mock(classOf[FlinkRelOptTable])
    when(flinkTable, "unwrap", classOf[TableSourceTable[_]]).thenReturn(innerTable)
    when(flinkTable, "getFlinkStatistic").thenReturn(statistic)
    when(flinkTable, "getRowType").thenReturn(relDataType)
    when(tableScan, "getTable").thenReturn(flinkTable)
    val rowCount: JDouble = if (statistic != null && statistic.getRowCount != null) {
      statistic.getRowCount
    } else {
      100D
    }
    when(tableScan, "estimateRowCount", mq).thenReturn(rowCount)
    tableScan
  }

  private def createNumericLiteral(num: Long): RexLiteral = {
    rexBuilder.makeExactLiteral(BigDecimal.valueOf(num))
  }

  private def createNumericLiteral(num: Double): RexLiteral = {
    rexBuilder.makeExactLiteral(BigDecimal.valueOf(num))
  }

  private def createBooleanLiteral(b: Boolean): RexLiteral = {
    rexBuilder.makeLiteral(b)
  }

  private def createStringLiteral(str: String): RexLiteral = {
    rexBuilder.makeLiteral(str)
  }

  private def createDateLiteral(str: String): RexLiteral = {
    rexBuilder.makeDateLiteral(new DateString(str))
  }

  private def createTimeLiteral(str: String): RexLiteral = {
    rexBuilder.makeTimeLiteral(new TimeString(str), 0)
  }

  private def createTimeStampLiteral(str: String): RexLiteral = {
    rexBuilder.makeTimestampLiteral(new TimestampString(str), 0)
  }

  private def createTimeStampLiteral(millis: Long): RexLiteral = {
    rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), 0)
  }

  private def createInputRef(index: Int): RexInputRef = {
    createInputRefWithNullability(index, isNullable = false)
  }

  private def createInputRefWithNullability(index: Int, isNullable: Boolean): RexInputRef = {
    val relDataType = typeFactory.createSqlType(allFieldTypes(index))
    val relDataTypeWithNullability = typeFactory.createTypeWithNullability(relDataType, isNullable)
    rexBuilder.makeInputRef(relDataTypeWithNullability, index)
  }

  private def createCall(operator: SqlOperator, exprs: RexNode*): RexNode = {
    Preconditions.checkArgument(exprs.nonEmpty)
    rexBuilder.makeCall(operator, exprs: _*)
  }

  private def createCast(expr: RexNode, indexInAllFieldTypes: Int): RexNode = {
    val relDataType = typeFactory.createSqlType(allFieldTypes(indexInAllFieldTypes))
    rexBuilder.makeCast(relDataType, expr)
  }

  private def createColumnStats(
      ndv: Option[JLong] = None,
      nullCount: Option[JLong] = None,
      avgLen: Option[JDouble] = None,
      maxLen: Option[Integer] = None,
      min: Option[Number] = None,
      max: Option[Number] = None): ColumnStats = {
    new ColumnStats(
      ndv.getOrElse(null.asInstanceOf[JLong]),
      nullCount.getOrElse(null.asInstanceOf[JLong]),
      avgLen.getOrElse(null.asInstanceOf[JDouble]),
      maxLen.getOrElse(null.asInstanceOf[Integer]),
      max.orNull,
      min.orNull)
  }

  private def createFlinkStatistic(
      rowCount: Option[JLong] = None,
      colStats: Option[Map[String, ColumnStats]] = None): FlinkStatistic = {
    require(rowCount.isDefined, "rowCount requires not null now")
    val tableStats = if (colStats.isDefined) {
      new TableStats(rowCount.get, colStats.get.asJava)
    } else {
      new TableStats(rowCount.get, null)
    }
    FlinkStatistic.builder().tableStats(tableStats).build()
  }

  @Test
  def testEqualsWithLiteralOfNumericType(): Unit = {
    // amount = 50
    val predicate1 = createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic1), mq)

    // [10, 200] contains 50
    assertEquals(Some(1.0 / 80.0), estimator2.evaluate(predicate1))

    // amount = 5
    val predicate2 = createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(5))
    // [10, 200] does not contain 5
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))

    val estimator3 = new SelectivityEstimator(mockScan(statistic2), mq)
    // [10, 200] contains 50, but ndv is null
    assertEquals(estimator3.defaultEqualsSelectivity, estimator3.evaluate(predicate1))

    // min or max is null
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), None))))
    val estimator4 = new SelectivityEstimator(mockScan(statistic3), mq)
    // [10, +INF) contains 50
    assertEquals(Some(1.0 / 80.0), estimator4.evaluate(predicate1))
    // [10, +INF) does not contain 5
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // ndv is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator5 = new SelectivityEstimator(mockScan(statistic4), mq)
    // [10, +INF) contains 50
    assertEquals(estimator5.defaultEqualsSelectivity, estimator5.evaluate(predicate1))
    // [10, +INF) does not contain 5
    assertEquals(Some(0.0), estimator5.evaluate(predicate2))
  }

  @Test
  def testEqualsWithLiteralOfStringType(): Unit = {
    // name = "abc"
    val predicate1 = createCall(EQUALS, createInputRef(name_idx), createStringLiteral("abc"))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate1))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("name" ->
      createColumnStats(Some(800L), Some(0L), Some(16.0), Some(32), None, None))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator2.evaluate(predicate1))

    // name = "xyz"
    val predicate2 = createCall(EQUALS, createInputRef(name_idx), createStringLiteral("xyz"))
    assertEquals(estimator1.defaultEqualsSelectivity, estimator2.evaluate(predicate2))
  }

  @Test
  def testEqualsWithLiteralOfBooleanType(): Unit = {
    // flag = true
    val predicate1 = createCall(EQUALS, createInputRef(flag_idx), createBooleanLiteral(true))
    // flag != true
    val predicate2 = createCall(NOT_EQUALS, createInputRef(flag_idx), createBooleanLiteral(true))
    // flag = false
    val predicate3 = createCall(EQUALS, createInputRef(flag_idx), createBooleanLiteral(false))
    // flag > false
    val predicate4 = createCall(GREATER_THAN, createInputRef(flag_idx), createBooleanLiteral(false))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate1))
    assertEquals(Some(1 - estimator1.defaultEqualsSelectivity.get), estimator1.evaluate(predicate2))
    assertEquals(Some(1 - estimator1.defaultEqualsSelectivity.get), estimator1.evaluate(predicate3))
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate4))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("flag" ->
      createColumnStats(Some(2L), Some(0L), Some(1.0), Some(1), None, None))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator2.evaluate(predicate1))
  }

  @Test
  def testEqualsWithLiteralOfDateType(): Unit = {
    // date_col = "2017-10-11"
    val predicate = createCall(
      EQUALS,
      createInputRef(date_idx),
      createDateLiteral("2017-10-11"))
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "date_col" -> createColumnStats(
        Some(80L),
        None,
        None,
        None,
        None,
        None))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(estimator.defaultEqualsSelectivity, estimator.evaluate(predicate))
  }

  @Test
  def testEqualsWithLiteralOfTimeType(): Unit = {
    // time_col = "11:00:00"
    val predicate = createCall(
      EQUALS,
      createInputRef(time_idx),
      createTimeLiteral("11:00:00"))
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "time_col" -> createColumnStats(
        Some(80L),
        None,
        None,
        None,
        None,
        None))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(estimator.defaultEqualsSelectivity, estimator.evaluate(predicate))
  }

  @Test
  def testEqualsWithLiteralOfTimestampType(): Unit = {
    val predicate = createCall(
      EQUALS,
      createInputRef(timestamp_idx),
      createTimeStampLiteral(1000L))
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "timestamp_col" -> createColumnStats(
        Some(80L),
        None,
        None,
        None,
        None,
        None))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(estimator.defaultEqualsSelectivity, estimator.evaluate(predicate))
  }

  @Test
  def testEqualsWithoutLiteral(): Unit = {
    // amount = price
    val predicate = createCall(EQUALS, createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate))

    // tests with statistics
    // no overlap
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(2), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    // complete overlap
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)))))
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))

    // min or max is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), None, Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(estimator1.defaultEqualsSelectivity,
      new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate))

    // ndv is null
    val statistic5 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(estimator1.defaultEqualsSelectivity,
      new SelectivityEstimator(mockScan(statistic5), mq).evaluate(predicate))
  }

  @Test
  def testNotEqualsWithLiteral(): Unit = {
    // amount <> 50
    val predicate1 = createCall(NOT_EQUALS, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(Some(1.0 - estimator1.defaultEqualsSelectivity.get),
      estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // [10, 200] contains 50
    assertEquals(Some(1.0 - 1.0 / 80.0), estimator2.evaluate(predicate1))

    // amount <> 5
    val predicate2 = createCall(NOT_EQUALS, createInputRef(amount_idx), createNumericLiteral(5))
    // [10, 200] does not contain 5
    assertEquals(Some(1.0), estimator2.evaluate(predicate2))
  }

  @Test
  def testComparisonWithLiteralOfStringType(): Unit = {
    // name > "abc"
    val predicate = createCall(GREATER_THAN, createInputRef(name_idx), createStringLiteral("abc"))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("name" ->
      createColumnStats(Some(800L), Some(0L), Some(16.0), Some(32), None, None))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(estimator2.defaultComparisonSelectivity, estimator2.evaluate(predicate))
  }

  @Test
  def testGreaterThanWithLiteral(): Unit = {
    // amount > 50
    val predicate1 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount > 200
    val predicate2 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(200))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount > 5
    val predicate3 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(5))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount > 10
    val predicate4 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(10))
    // partial overlap(excluding min value)
    assertEquals(Some(1.0 - 1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 > amount
    val predicate5 = createCall(GREATER_THAN, createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))

    // min or max is null
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), None, Some(200)))))
    val estimator3 = new SelectivityEstimator(mockScan(statistic1), mq)
    // no overlap
    assertEquals(Some(0.0), estimator3.evaluate(predicate2))
    // partial overlap, default value
    assertEquals(estimator3.defaultComparisonSelectivity, estimator3.evaluate(predicate3))

    // ndv is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator4 = new SelectivityEstimator(mockScan(statistic2), mq)
    // no overlap
    assertEquals(Some(0.0), estimator4.evaluate(predicate2))
    // complete overlap
    assertEquals(Some(1.0), estimator4.evaluate(predicate3))
    // partial overlap, default value
    assertEquals(Some((200D - 50) / (200D - 10)), estimator4.evaluate(predicate1))
  }

  @Test
  def testGreaterThanWithoutLiteral(): Unit = {
    // amount > price
    val predicate = createCall(GREATER_THAN, createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(2), Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(19)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))

    // min or max is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), None, Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), None))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate))
    val statistic5 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(11), None),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), None, Some(10)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic5), mq).evaluate(predicate))
    // partial overlap
    val statistic6 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), None),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), None, Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic6), mq).evaluate(predicate))
  }

  @Test
  def testGreaterThanOrEqualsToWithLiteral(): Unit = {
    // amount >= 50
    val predicate1 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount >= 201
    val predicate2 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(201))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount >= 10
    val predicate3 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(10))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount >= 200
    val predicate4 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(200))
    // partial overlap(including max value)
    assertEquals(Some(1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 >= amount
    val predicate5 = createCall(GREATER_THAN_OR_EQUAL,
      createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))
  }

  @Test
  def testGreaterThanOrEqualsToWithoutLiteral(): Unit = {
    // amount >= price
    val predicate = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(2), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(20)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))
  }

  @Test
  def testLessThanWithLiteral(): Unit = {
    // amount < 50
    val predicate1 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount < 10
    val predicate2 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(10))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount < 201
    val predicate3 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(201))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount < 200
    val predicate4 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(200))
    // partial overlap(excluding max value)
    assertEquals(Some(1.0 - 1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 < amount
    val predicate5 = createCall(LESS_THAN, createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))
  }

  @Test
  def testLessThanWithoutLiteral(): Unit = {
    // amount < price
    val predicate = createCall(LESS_THAN, createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(10)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(1), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(10L), Some(8.0), Some(8), Some(1), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // partial overlap (account's nullCount is not 0)
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))

    // partial overlap
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate))
  }

  @Test
  def testLessThanOrEqualsToWithLiteral(): Unit = {
    // amount <= 50
    val predicate1 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount <= 9
    val predicate2 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(9))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount <= 200
    val predicate3 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(200))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount <= 10
    val predicate4 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(10))
    // partial overlap(excluding min value)
    assertEquals(Some(1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 <= amount
    val predicate5 = createCall(LESS_THAN_OR_EQUAL,
      createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))
  }

  @Test
  def testLessThanOrEqualsToWithoutLiteral(): Unit = {
    // amount <= price
    val predicate = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(9)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(1), Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))
  }

  @Test
  def testLike(): Unit = {
    // name like 'ross'
    val predicate = createCall(LIKE, createInputRef(name_idx), createStringLiteral("ross"))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultLikeSelectivity, estimator.evaluate(predicate))
  }

  @Test
  def testIsNull(): Unit = {
    // name is null
    val predicate = createCall(IS_NULL, createInputRefWithNullability(name_idx, true))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultIsNullSelectivity, estimator.evaluate(predicate))

    val colStats = createColumnStats(Some(80L), Some(10L), Some(16.0), Some(32), None, None)
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map("name" -> colStats)))
    assertEquals(Some(10.0 / 100.0),
      new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))
  }

  @Test
  def testIsNotNull(): Unit = {
    // name is not null
    val predicate = createCall(IS_NOT_NULL, createInputRef(name_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(Some(1.0), estimator.evaluate(predicate))

    val predicate2 = createCall(IS_NOT_NULL, createInputRefWithNullability(name_idx, true))
    assertEquals(estimator.defaultIsNotNullSelectivity, estimator.evaluate(predicate2))

    val colStats = createColumnStats(Some(80L), Some(10L), Some(16.0), Some(32), None, None)
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map("name" -> colStats)))
    assertEquals(Some(1.0 - 10.0 / 100.0),
      new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate2))
  }

  @Test
  def testIn(): Unit = {
    val estimator = new SelectivityEstimator(scan, mq)

    // name in ("abc", "def")
    val predicate1 = createCall(IN,
      createInputRef(name_idx),
      createStringLiteral("abc"),
      createStringLiteral("def"))
    // test with unsupported type
    assertEquals(Some(estimator.defaultEqualsSelectivity.get * 2), estimator.evaluate(predicate1))

    // tests with supported type
    val predicate2 = createCall(IN,
      createInputRef(amount_idx),
      createNumericLiteral(10.0),
      createNumericLiteral(20.0),
      createNumericLiteral(30.0),
      createNumericLiteral(40.0))
    // test without statistics
    assertEquals(Some(estimator.defaultEqualsSelectivity.get * 4), estimator.evaluate(predicate2))

    // test with statistics
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(50), Some(200)))))
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate2))

    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(15), Some(200)))))
    assertEquals(Some(3.0 / 80.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate2))

    // min or max is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(50), None))))
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate2))

    // ndv is null
    val statistic5 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))
    assertEquals(Some(estimator.defaultEqualsSelectivity.get * 4),
      new SelectivityEstimator(mockScan(statistic5), mq).evaluate(predicate2))

    // column interval is null
    val statistic6 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(40L), None, Some(8.0), Some(8), None, None))))
    assertEquals(Some(4.0 / 40.0),
      new SelectivityEstimator(mockScan(statistic6), mq).evaluate(predicate2))
  }

  @Test
  def testAnd(): Unit = {
    // amount <= 50 and price > 6.5
    val predicate = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(6.5))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * selectivity), estimator.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    assertEquals(Some(((50.0 - 10.0) / (200.0 - 10.0)) * ((8.0 - 6.5) / (8.0 - 2.0))),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate))
  }

  @Test
  def testOr(): Unit = {
    // amount <= 50 or price > 6.5
    val predicate = createCall(OR,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(6.5))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * 2 - selectivity * selectivity), estimator.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    val leftSelectivity = (50.0 - 10.0) / (200.0 - 10.0)
    val rightSelectivity = (8.0 - 6.5) / (8.0 - 2.0)
    assertEquals(Some((leftSelectivity + rightSelectivity) - (leftSelectivity * rightSelectivity)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate))

    // amount = 50 or amount = 60
    val predicate1 = createCall(OR,
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(60))
    )
    assertEquals(Some(2.0 / 80.0),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate1))

    // amount = 50 or amount = 60 or amount > 70
    val predicate2 = createCall(OR,
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(60)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(70))
    )
    val inSelectivity = 2.0 / 80.0
    val greaterThan70Selectivity = (200.0 - 70.0) / (200.0 - 10.0)
    assertEquals(
      Some(inSelectivity + greaterThan70Selectivity - inSelectivity * greaterThan70Selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate2))

    // amount < 50 or amount > 80
    val predicate3 = createCall(OR,
      createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(80))
    )
    val lessThan50Selectivity = (50.0 - 10.0) / (200.0 - 10.0)
    val greaterThan80Selectivity = (200.0 - 80.0) / (200.0 - 10.0)
    assertEquals(
      Some(lessThan50Selectivity + greaterThan80Selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate3))

    //  50 = amount or amount = 60 or 70 < amount or price = 5 or price < 3
    val predicate4 = createCall(OR,
      createCall(EQUALS, createNumericLiteral(50), createInputRef(amount_idx)),
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(60)),
      createCall(LESS_THAN, createNumericLiteral(70), createInputRef(amount_idx)),
      createCall(EQUALS, createInputRef(price_idx), createNumericLiteral(5)),
      createCall(LESS_THAN, createInputRef(price_idx), createNumericLiteral(3))
    )

    val inSelectivity1 = 2.0 / 80.0
    val lessThan70Selectivity = (200.0 - 70.0) / (200.0 - 10.0)
    val priceSelectivity = 1.0 / 50.0 + (3.0 - 2.0) / (8.0 - 2.0)
    assertEquals(
      Some(inSelectivity1 + lessThan70Selectivity + priceSelectivity -
        inSelectivity1 * lessThan70Selectivity * priceSelectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate4))
  }

  @Test
  def testNot(): Unit = {
    // not(amount <= 50)
    val predicate = createCall(NOT,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(Some(1.0 - estimator.defaultComparisonSelectivity.get),
      estimator.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    assertEquals(Some(1.0 - (50.0 - 10.0) / (200.0 - 10.0)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate))
  }

  @Test
  def testAndOrNot(): Unit = {
    // amount <= 50 and (name = "abc" or not(price < 4.5))
    val predicate = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(OR,
        createCall(EQUALS, createInputRef(name_idx), createStringLiteral("abc")),
        createCall(NOT,
          createCall(LESS_THAN, createInputRef(price_idx), createNumericLiteral(4.5))
        )
      )
    )

    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    val accountSelectivity1 = estimator1.defaultComparisonSelectivity.get
    val nameSelectivity1 = estimator1.defaultEqualsSelectivity.get
    val notPriceSelectivity1 = 1.0 - estimator1.defaultComparisonSelectivity.get
    val selectivity1 = accountSelectivity1 * (nameSelectivity1 + notPriceSelectivity1 -
      nameSelectivity1 * notPriceSelectivity1)
    assertEquals(Some(selectivity1), estimator1.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    val accountSelectivity2 = (50.0 - 10.0) / (200.0 - 10.0)
    val nameSelectivity2 = estimator2.defaultEqualsSelectivity.get
    // not(price < 4.5) will be converted to (price >= 4.5)
    val notPriceSelectivity2 = (8 - 4.5) / (8.0 - 2.0)
    val selectivity2 = accountSelectivity2 * (nameSelectivity2 + notPriceSelectivity2 -
      nameSelectivity2 * notPriceSelectivity2)
    assertEquals(Some(selectivity2), estimator2.evaluate(predicate))
  }

  @Test
  def testPredicateWithUdf(): Unit = {
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)

    // abs(amount) <= 50
    val predicate1 = createCall(LESS_THAN,
      createCall(SqlStdOperatorTable.ABS, createInputRef(amount_idx)),
      createNumericLiteral(50))
    // with builtin udf
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate1))

    // TODO test with not builtin udf
  }

  @Test
  def testSelectivityWithSameRexInputRefs(): Unit = {
    // amount <= 45 and amount > 40
    val predicate1 = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(45)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(40))
    )
    // amount <= 45 and amount > 40 and price > 4.5 and price < 5
    val predicate2 = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(45)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(40)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(4.5)),
      createCall(LESS_THAN, createInputRef(price_idx), createNumericLiteral(5))
    )

    // amount <= 45 and amount > 40 and price > 4.5 and cast(price as INTEGER) < 5
    val predicate3 = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(45)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(40)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(4.5)),
      createCall(LESS_THAN, createCast(createInputRef(price_idx), 1), createNumericLiteral(5))
    )

    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * selectivity), estimator.evaluate(predicate1))
    assertEquals(Some((selectivity * selectivity) * selectivity * selectivity),
      estimator.evaluate(predicate2))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(0), Some(100)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    assertEquals(Some(((45 - 40) * 1.0) / (100 - 0)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate1))
    assertEquals(Some((((45 - 40) * 1.0) / (100 - 0)) * ((5 - 4.5) / (8 - 2))),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate2))
    assertEquals(Some((((45 - 40) * 1.0) / (100 - 0)) * ((8 - 4.5) / (8 - 2)) * selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate3))

    // amount < 120 and amount => 80
    val predicate4 = createCall(AND,
      createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(120)),
      createCall(GREATER_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(80))
    )
    assertEquals(Some(((100 - 80) * 1.0) / (100 - 0)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate4))
  }

  @Test
  def testSelectivityWithSameRexInputRefsAndStringType(): Unit = {
    // name > "abc" and name < "test"
    val predicate1 = createCall(AND,
      createCall(GREATER_THAN, createInputRef(name_idx), createStringLiteral("abc")),
      createCall(LESS_THAN, createInputRef(name_idx), createStringLiteral("test"))
    )

    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * selectivity), estimator.evaluate(predicate1))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("name" ->
      createColumnStats(Some(800L), Some(0L), Some(16.0), Some(32), None, None))))
    // TODO
    assertEquals(Some(selectivity * selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate1))
  }

}

object SelectivityEstimatorTest {

  @BeforeClass
  def beforeAll(): Unit = {
    RelMetadataQuery
      .THREAD_PROVIDERS
      .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
  }

}

