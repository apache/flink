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
import org.apache.flink.table.planner.calcite.{FlinkContextImpl, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.plan.schema._
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.{JDouble, JLong}
import org.apache.flink.util.Preconditions
import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{AbstractRelOptPlanner, RelOptCluster}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall, TableScan}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.{SqlAggFunction, SqlOperator}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{Before, BeforeClass, Test}
import org.powermock.api.mockito.PowerMockito._
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import java.math.BigDecimal

import org.apache.flink.table.module.ModuleManager

import scala.collection.JavaConversions._

/**
  * Tests for [[AggCallSelectivityEstimator]].
  *
  * We use PowerMockito instead of Mockito here, because [[TableScan#getRowType]] is a final method.
  */
@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[TableScan]))
class AggCallSelectivityEstimatorTest {
  private val allFieldNames = Seq("name", "amount", "price")
  private val allFieldTypes = Seq(VARCHAR, INTEGER, DOUBLE)
  val (name_idx, amount_idx, price_idx) = (0, 1, 2)

  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  var rexBuilder = new RexBuilder(typeFactory)
  val relDataType: RelDataType = typeFactory.createStructType(
    allFieldTypes.map(typeFactory.createSqlType),
    allFieldNames)

  val mq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()
  var scan: TableScan = _

  @Before
  def setup(): Unit = {
    scan = mockScan()
  }

  private def mockScan(
      statistic: FlinkStatistic = FlinkStatistic.UNKNOWN): TableScan = {
    val tableScan = mock(classOf[TableScan])
    val cluster = mock(classOf[RelOptCluster])
    val planner = mock(classOf[AbstractRelOptPlanner])
    val catalogManager = mock(classOf[CatalogManager])
    val moduleManager = mock(classOf[ModuleManager])
    val config = new TableConfig
    val functionCatalog = new FunctionCatalog(config, catalogManager, moduleManager)
    val context = new FlinkContextImpl(new TableConfig, functionCatalog, catalogManager)
    when(tableScan, "getCluster").thenReturn(cluster)
    when(cluster, "getRexBuilder").thenReturn(rexBuilder)
    when(cluster, "getTypeFactory").thenReturn(typeFactory)
    when(cluster, "getPlanner").thenReturn(planner)
    when(planner, "getContext").thenReturn(context)
    when(tableScan, "getRowType").thenReturn(relDataType)
    val sourceTable = mock(classOf[TableSourceTable[_]])
    when(sourceTable, "unwrap", classOf[TableSourceTable[_]]).thenReturn(sourceTable)
    when(sourceTable, "getStatistic").thenReturn(statistic)
    when(sourceTable, "getRowType").thenReturn(relDataType)
    when(tableScan, "getTable").thenReturn(sourceTable)
    val rowCount: JDouble = if (statistic != null && statistic.getRowCount != null) {
      statistic.getRowCount
    } else {
      100D
    }
    when(tableScan, "estimateRowCount", mq).thenReturn(rowCount)
    tableScan
  }

  private def createAggregate(
      groupSet: Array[Int],
      sqlAggFunWithArg: Seq[(SqlAggFunction, Int)]): Aggregate = {
    createAggregate(scan, groupSet, sqlAggFunWithArg)
  }

  private def createAggregate(
      scan: TableScan,
      groupSet: Array[Int],
      sqlAggFunWithArg: Seq[(SqlAggFunction, Int)]): Aggregate = {

    val aggCalls = sqlAggFunWithArg.map {
      case (sqlAggFun, arg) =>
        val aggCallType = sqlAggFun match {
          case SqlStdOperatorTable.COUNT =>
            typeFactory.createSqlType(SqlTypeName.BIGINT)
          case _ =>
            scan.getRowType.getFieldList.get(arg).getType
        }
        AggregateCall.create(
          sqlAggFun,
          false,
          false,
          ImmutableList.of(Integer.valueOf(arg)),
          -1,
          groupSet.length,
          scan,
          aggCallType,
          scan.getRowType.getFieldNames.get(arg)
        )
    }

    LogicalAggregate.create(
      scan,
      ImmutableBitSet.of(groupSet: _*),
      null,
      ImmutableList.copyOf(aggCalls.toArray))
  }

  private def createNumericLiteral(num: Long): RexLiteral = {
    rexBuilder.makeExactLiteral(BigDecimal.valueOf(num))
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

  private def createColumnStats(
      ndv: Option[JLong] = None,
      nullCount: Option[JLong] = None,
      avgLen: Option[JDouble] = None,
      maxLen: Option[Integer] = None,
      min: Option[Comparable[_]] = None,
      max: Option[Comparable[_]] = None): ColumnStats = {
    ColumnStats.Builder
      .builder
      .setNdv(ndv.getOrElse(null.asInstanceOf[JLong]))
      .setNullCount(nullCount.getOrElse(null.asInstanceOf[JLong]))
      .setAvgLen(avgLen.getOrElse(null.asInstanceOf[JDouble]))
      .setMaxLen(maxLen.getOrElse(null.asInstanceOf[Integer]))
      .setMax(max.orNull)
      .setMin(min.orNull)
      .build
  }

  private def createFlinkStatistic(
      rowCount: Option[JLong] = None,
      colStats: Option[Map[String, ColumnStats]] = None): FlinkStatistic = {
    require(rowCount.isDefined, "rowCount must be non null now")
    val tableStats = if (colStats.isDefined) {
      new TableStats(rowCount.get, colStats.get)
    } else {
      new TableStats(rowCount.get, null)
    }
    FlinkStatistic.builder().tableStats(tableStats).build()
  }

  @Test
  def testSumWithEquals(): Unit = {
    // sum(amount), sum(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // sum(amount) = 50
    val predicate1 = createCall(EQUALS, createInputRef(1), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(se.defaultEqualsSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // [10 * 4, 20 * 4] contains 50
    assertEquals(Some(1.0 / 40.0), estimator2.evaluate(predicate1))

    // sum(amount) = 5
    val predicate2 = createCall(EQUALS, createInputRef(1), createNumericLiteral(5))
    // [10 * 4, 20 * 4] does not contain 5
    assertEquals(estimator1.defaultAggCallSelectivity, estimator2.evaluate(predicate2))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(se.defaultEqualsSelectivity, estimator3.evaluate(predicate1))
    // min and max are null
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), None, None))))
    val agg4 = createAggregate(mockScan(statistic3), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator4 = new AggCallSelectivityEstimator(agg4, mq)
    assertEquals(se.defaultEqualsSelectivity, estimator4.evaluate(predicate1))
  }

  @Test
  def testSumWithLessThan(): Unit = {
    // sum(amount), sum(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // sum(amount) < 50
    val predicate1 = createCall(LESS_THAN, createInputRef(1), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // sum(amount) < 5
    val predicate2 = createCall(LESS_THAN, createInputRef(1), createNumericLiteral(5))
    // no overlap
    assertEquals(estimator2.defaultAggCallSelectivity, estimator2.evaluate(predicate2))

    val predicate3 = createCall(LESS_THAN, createInputRef(1), createNumericLiteral(100))
    // complete overlap
    assertEquals(Some(1.0 - estimator1.defaultAggCallSelectivity.get),
      estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((50.0 - 40.0) / (80.0 - 40.0)), estimator2.evaluate(predicate1))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate1))
  }

  @Test
  def testSumWithLessThanOrEqualsTo(): Unit = {
    // sum(amount), sum(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // sum(amount) < 50
    val predicate1 = createCall(LESS_THAN_OR_EQUAL, createInputRef(1), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // sum(amount) < 5
    val predicate2 = createCall(LESS_THAN_OR_EQUAL, createInputRef(1), createNumericLiteral(5))
    // no overlap
    assertEquals(estimator2.defaultAggCallSelectivity, estimator2.evaluate(predicate2))

    val predicate3 = createCall(LESS_THAN_OR_EQUAL, createInputRef(1), createNumericLiteral(100))
    // complete overlap
    assertEquals(Some(1.0 - estimator1.defaultAggCallSelectivity.get),
      estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((50.0 - 40.0) / (80.0 - 40.0)), estimator2.evaluate(predicate1))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate1))
  }

  @Test
  def testSumWithGreaterThan(): Unit = {
    // sum(amount), sum(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // sum(amount) > 50
    val predicate1 = createCall(GREATER_THAN, createInputRef(1), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // sum(amount) > 100
    val predicate2 = createCall(GREATER_THAN, createInputRef(1), createNumericLiteral(100))
    // no overlap
    assertEquals(estimator2.defaultAggCallSelectivity, estimator2.evaluate(predicate2))

    val predicate3 = createCall(GREATER_THAN, createInputRef(1), createNumericLiteral(5))
    // complete overlap
    assertEquals(Some(1.0 - estimator1.defaultAggCallSelectivity.get),
      estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((80.0 - 50.0) / (80.0 - 40.0)), estimator2.evaluate(predicate1))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate1))
  }

  @Test
  def testSumWithGreaterThanOrEquals(): Unit = {
    // sum(amount), sum(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // sum(amount) > 50
    val predicate1 = createCall(GREATER_THAN_OR_EQUAL, createInputRef(1), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // sum(amount) > 100
    val predicate2 = createCall(GREATER_THAN_OR_EQUAL, createInputRef(1), createNumericLiteral(100))
    // no overlap
    assertEquals(estimator2.defaultAggCallSelectivity, estimator2.evaluate(predicate2))

    val predicate3 = createCall(GREATER_THAN_OR_EQUAL, createInputRef(1), createNumericLiteral(5))
    // complete overlap
    assertEquals(Some(1.0 - estimator1.defaultAggCallSelectivity.get),
      estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((80.0 - 50.0) / (80.0 - 40.0)), estimator2.evaluate(predicate1))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate1))
  }

  @Test
  def testMaxMin(): Unit = {
    // max(amount), min(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.MAX, amount_idx), (SqlStdOperatorTable.MIN, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // max(amount) > 15
    val predicate1 = createCall(GREATER_THAN, createInputRef(1), createNumericLiteral(15))
    // max(amount) < 10
    val predicate2 = createCall(LESS_THAN, createInputRef(2), createNumericLiteral(10))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate1))
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate2))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)),
      "price" -> createColumnStats(Some(20L), None, Some(8.0), Some(8), Some(1.0), Some(30.0)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.MAX, amount_idx), (SqlStdOperatorTable.MIN, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // max(amount) = 15
    val predicate3 = createCall(EQUALS, createInputRef(1), createNumericLiteral(15))
    // min(price) > 50
    val predicate4 = createCall(GREATER_THAN, createInputRef(2), createNumericLiteral(50))
    // [10, 20] contains 15
    assertEquals(Some(1.0 / (20.0 - 10.0)), estimator2.evaluate(predicate3))
    // no overlap
    assertEquals(estimator2.defaultAggCallSelectivity, estimator2.evaluate(predicate4))

    val predicate5 = createCall(LESS_THAN, createInputRef(1), createNumericLiteral(100))
    // complete overlap
    assertEquals(Some(1.0 - estimator1.defaultAggCallSelectivity.get),
      estimator2.evaluate(predicate5))

    // partial overlap
    assertEquals(Some((20.0 - 15.0) / (20.0 - 10.0)), estimator2.evaluate(predicate1))
    assertEquals(Some((10.0 - 1.0) / (30.0 - 1.0)), estimator2.evaluate(predicate2))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.MIN, amount_idx), (SqlStdOperatorTable.MAX, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate1))
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate2))
  }

  @Test
  def testAvg(): Unit = {
    // avg(amount), avg(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.AVG, amount_idx), (SqlStdOperatorTable.AVG, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // avg(amount) > 15
    val predicate1 = createCall(GREATER_THAN, createInputRef(1), createNumericLiteral(15))
    // avg(amount) < 10
    val predicate2 = createCall(LESS_THAN, createInputRef(2), createNumericLiteral(10))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate1))
    assertEquals(se.defaultComparisonSelectivity, estimator1.evaluate(predicate2))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)),
      "price" -> createColumnStats(Some(20L), None, Some(8.0), Some(8), Some(1.0), Some(30.0)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.AVG, amount_idx), (SqlStdOperatorTable.AVG, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // max(amount) = 15
    val predicate3 = createCall(EQUALS, createInputRef(1), createNumericLiteral(15))
    // min(price) > 50
    val predicate4 = createCall(GREATER_THAN, createInputRef(2), createNumericLiteral(50))
    // [10, 20] contains 15
    assertEquals(Some(1.0 / (20.0 - 10.0)), estimator2.evaluate(predicate3))
    // no overlap
    assertEquals(estimator2.defaultAggCallSelectivity, estimator2.evaluate(predicate4))

    val predicate5 = createCall(LESS_THAN, createInputRef(1), createNumericLiteral(100))
    // complete overlap
    assertEquals(Some(1.0 - estimator1.defaultAggCallSelectivity.get),
      estimator2.evaluate(predicate5))

    // partial overlap
    assertEquals(Some((20.0 - 15.0) / (20.0 - 10.0)), estimator2.evaluate(predicate1))
    assertEquals(Some((10.0 - 1.0) / (30.0 - 1.0)), estimator2.evaluate(predicate2))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.AVG, amount_idx), (SqlStdOperatorTable.AVG, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate1))
    assertEquals(se.defaultComparisonSelectivity, estimator3.evaluate(predicate2))
  }

  @Test
  def testCount(): Unit = {
    // count(amount), count(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.COUNT, amount_idx), (SqlStdOperatorTable.COUNT, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // count(amount) > 6
    val predicate1 = createCall(GREATER_THAN, createInputRef(1), createNumericLiteral(6))
    // count(amount) < 5
    val predicate2 = createCall(LESS_THAN, createInputRef(2), createNumericLiteral(5))
    // test without statistics
    val estimator1 = new AggCallSelectivityEstimator(agg1, mq)
    assertEquals(Some(0.9526830054771714), estimator1.evaluate(predicate1))
    assertEquals(estimator1.defaultAggCallSelectivity, estimator1.evaluate(predicate2))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)),
      "price" -> createColumnStats(Some(20L), None, Some(8.0), Some(8), Some(1.0), Some(30.0)))))
    val agg2 = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.COUNT, amount_idx), (SqlStdOperatorTable.COUNT, price_idx)))
    val estimator2 = new AggCallSelectivityEstimator(agg2, mq)
    // count(amount) = 6
    val predicate3 = createCall(EQUALS, createInputRef(1), createNumericLiteral(6))
    // count(price) > 10
    val predicate4 = createCall(GREATER_THAN, createInputRef(2), createNumericLiteral(10))
    // [2, 8] contains 6
    assertEquals(Some(1.0 / (8.0 - 2.0)), estimator2.evaluate(predicate3))
    // no overlap
    assertEquals(estimator2.defaultAggCallSelectivity, estimator2.evaluate(predicate4))

    val predicate5 = createCall(LESS_THAN, createInputRef(1), createNumericLiteral(10))
    // complete overlap
    assertEquals(Some(1.0 - estimator1.defaultAggCallSelectivity.get),
      estimator2.evaluate(predicate5))

    // partial overlap
    assertEquals(Some((8.0 - 6.0) / (8.0 - 2.0)), estimator2.evaluate(predicate1))
    assertEquals(Some((5.0 - 2.0) / (8.0 - 2.0)), estimator2.evaluate(predicate2))

    // min or max is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), None))))
    val agg3 = createAggregate(mockScan(statistic2), Array(name_idx),
      Seq((SqlStdOperatorTable.COUNT, amount_idx), (SqlStdOperatorTable.COUNT, price_idx)))
    val estimator3 = new AggCallSelectivityEstimator(agg3, mq)
    assertEquals(Some((8.0 - 6.0) / (8.0 - 2.0)), estimator3.evaluate(predicate1))
    assertEquals(Some((5.0 - 2.0) / (8.0 - 2.0)), estimator3.evaluate(predicate2))
  }

  @Test
  def testAnd(): Unit = {
    // sum(amount), sum(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)),
      "price" -> createColumnStats(Some(20L), None, Some(8.0), Some(8), Some(1.0), Some(30.0)))))
    val agg = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator = new AggCallSelectivityEstimator(agg, mq)
    // sum(amount) < 50 and sum(price) > 10
    val predicate = createCall(AND,
      createCall(LESS_THAN, createInputRef(1), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(2), createNumericLiteral(10)))

    assertEquals(Some(((50.0 - 40.0) / (80.0 - 40.0)) * ((120.0 - 10.0) / (120.0 - 4.0))),
      estimator.evaluate(predicate))
  }

  @Test
  def testOr(): Unit = {
    // sum(amount), sum(price) group by name
    val agg1 = createAggregate(Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val se = new SelectivityEstimator(agg1, mq)

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "name" -> createColumnStats(Some(25L), None, Some(16.0), Some(32), None, None),
      "amount" -> createColumnStats(Some(10L), None, Some(8.0), Some(8), Some(10), Some(20)),
      "price" -> createColumnStats(Some(20L), None, Some(8.0), Some(8), Some(1.0), Some(30.0)))))
    val agg = createAggregate(mockScan(statistic1), Array(name_idx),
      Seq((SqlStdOperatorTable.SUM, amount_idx), (SqlStdOperatorTable.SUM, price_idx)))
    val estimator = new AggCallSelectivityEstimator(agg, mq)
    // sum(amount) < 50 or sum(price) > 10
    val predicate = createCall(OR,
      createCall(LESS_THAN, createInputRef(1), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(2), createNumericLiteral(10)))

    val s1 = (50.0 - 40.0) / (80.0 - 40.0)
    val s2 = (120.0 - 10.0) / (120.0 - 4.0)
    assertEquals(Some(s1 + s2 - s1 * s2), estimator.evaluate(predicate))
  }

}

object AggCallSelectivityEstimatorTest {

  @BeforeClass
  def beforeAll(): Unit = {
    RelMetadataQuery
      .THREAD_PROVIDERS
      .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
  }

}

