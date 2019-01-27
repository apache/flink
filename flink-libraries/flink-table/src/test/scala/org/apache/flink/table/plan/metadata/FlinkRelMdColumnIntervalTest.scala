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
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.functions.sql.internal.SqlAuxiliaryGroupAggFunction
import org.apache.flink.table.plan.stats.{RightSemiInfiniteValueInterval, ValueInterval}
import org.apache.flink.table.plan.util.ColumnIntervalUtil.toBigDecimalInterval

import org.apache.calcite.rel._
import org.apache.calcite.rel.core.{AggregateCall, JoinRelType}
import org.apache.calcite.rel.logical.LogicalExchange
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlCountAggFunction
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.{DateString, TimeString, TimestampString}

import com.google.common.collect.Lists

import java.sql.{Date, Time, Timestamp}

import scala.collection.JavaConversions._

import org.junit.Assert._
import org.junit.Test

class FlinkRelMdColumnIntervalTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetColumnIntervalOnBinaryOperator(): Unit = {
    val ts1 = relBuilder.scan("t1")
      .project(
        relBuilder.call(
          MULTIPLY,
          relBuilder.literal(3),
          relBuilder.call(
            MINUS,
            relBuilder.field(0),
            relBuilder.literal(2)
          )
        )
      )
      .build()

    assertEquals(
      toBigDecimalInterval(ValueInterval(-21, 9)),
      toBigDecimalInterval(mq.getColumnInterval(ts1, 0))
    )
  }

  @Test
  def testGetColumnIntervalOnTableScan(): Unit = {
    val ts = relBuilder.scan("t1").build()
    assertEquals(ValueInterval(-5, 5), mq.getColumnInterval(ts, 0))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(ts, 1))
  }

  @Test
  def testGetColumnIntervalOnLogicalDimensionTableScan(): Unit = {
    assertEquals(null, mq.getColumnInterval(temporalTableSourceScanWithCalc, 0))
    assertEquals(null, mq.getColumnInterval(temporalTableSourceScanWithCalc, 1))
    assertEquals(null, mq.getColumnInterval(temporalTableSourceScanWithCalc, 2))
  }

  @Test
  def testGetColumnIntervalOnFilter(): Unit = {
    val ts = relBuilder.scan("t1").build()
    relBuilder.push(ts)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    val expr3 = relBuilder.call(GREATER_THAN,
      relBuilder.call(ScalarSqlFunctions.DIV, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.literal(3))
    val expr4 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    val expr5 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(90))
    val expr6 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(-1))
    val expr7 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(1.9D))
    // filter: $0 <= 2
    val filter1 = relBuilder.filter(expr1).build
    assertEquals(mq.getColumnInterval(filter1, 0), ValueInterval(-5, 2))
    // filter: $0 <= 2 and $0 > -1 and DIV($0, 2) > 3
    val filter2 = relBuilder.push(ts).filter(expr1, expr2, expr3).build
    assertEquals(mq.getColumnInterval(filter2, 0),
      ValueInterval(-1, 2, includeLower = false))
    // filter: $0 <= 2 and $0 > -1 and $1 < 1.1
    val filter3 = relBuilder.push(ts).filter(expr1, expr2, expr4).build
    assertEquals(mq.getColumnInterval(filter3, 0),
      ValueInterval(-1, 2, includeLower = false))
    // filter: $0 > 90 or $0 <= -1
    val filter4 = relBuilder.push(ts).filter(relBuilder.call(OR, expr5, expr6)).build
    assertEquals(mq.getColumnInterval(filter4, 0), ValueInterval(-5, -1))
    // filter: $0 > 90 or $0 <= -1 or $1 < 1.1
    val filter5 = relBuilder.push(ts).filter(relBuilder.call(OR, expr4, expr5, expr6)).build
    assertEquals(mq.getColumnInterval(filter5, 0), ValueInterval(-5, 5))
    // filter: ($0 <=2 and $1 < 1.1) or not(DIV($0, 2) > 3 or $1 > 1.9)
    val filter6 = relBuilder.push(ts).filter(relBuilder.call(OR,
      relBuilder.call(AND, expr1, expr4),
      relBuilder.call(NOT, relBuilder.call(OR, expr3, expr7)))).build
    assertEquals(mq.getColumnInterval(filter6, 0), ValueInterval(-5, 5))
    // filter: ($0 <=2 and $1 < 1.1) or not( $0>2 or $1 > 1.9)
    val filter7 = relBuilder.push(ts).filter(relBuilder.call(OR,
      relBuilder.call(AND, expr1, expr4),
      relBuilder.call(NOT,
        relBuilder.call(OR,
          RexUtil.negate(relBuilder.getRexBuilder, expr1.asInstanceOf[RexCall]),
          expr7)))).build
    assertEquals(mq.getColumnInterval(filter7, 0), ValueInterval(-5, 2))
  }

  @Test
  def testGetColumnIntervalOnCalc(): Unit = {
    val ts = relBuilder.scan("t1").build()
    relBuilder.push(ts)
    // projects: $0==1, $0, $1, true, 2.1, 2
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.getRexBuilder.makeLiteral(2.1D, typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = false), true),
      relBuilder.getRexBuilder.makeLiteral(2L, typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = false), true))
    val outputRowType = relBuilder.project(projects).build().getRowType
    relBuilder.push(ts)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(-1))
    val expr3 = relBuilder.call(GREATER_THAN,
      relBuilder.call(ScalarSqlFunctions.DIV, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.literal(3))
    val expr4 = relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))
    val expr5 = relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(90))
    val expr6 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(-1))
    val expr7 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(1.9D))
    // calc => projects + filter: $0 <= 2
    val calc1 = buildCalc(ts, outputRowType, projects, List(expr1))
    assertNull(mq.getColumnInterval(calc1, 0))
    assertEquals(mq.getColumnInterval(calc1, 1), ValueInterval(-5, 2))
    assertEquals(mq.getColumnInterval(calc1, 2), ValueInterval(0D, 6.1D))
    assertEquals(mq.getColumnInterval(calc1, 3), ValueInterval(true, true))
    assertEquals(mq.getColumnInterval(calc1, 4), ValueInterval(2.1D, 2.1D))
    assertEquals(mq.getColumnInterval(calc1, 5), ValueInterval(2L, 2L))
    // calc => project + filter: $0 <= 2 and $0 > -1 and DIV($0, 2) > 3
    val calc2 = buildCalc(ts, outputRowType, projects, List(expr1, expr2, expr3))
    assertEquals(mq.getColumnInterval(calc2, 1),
      ValueInterval(-1, 2, includeLower = false))
    // calc => project + filter: $0 <= 2 and $0 > -1 and $1 < 1.1
    val calc3 = buildCalc(ts, outputRowType, projects, List(expr1, expr2, expr4))
    assertEquals(mq.getColumnInterval(calc3, 1),
      ValueInterval(-1, 2, includeLower = false))
    assertEquals(mq.getColumnInterval(calc3, 2),
      ValueInterval(0D, 1.1D, includeUpper = false))
    // calc => project + filter: $0 > 90 or $0 <= -1
    val calc4 = buildCalc(ts, outputRowType, projects, List(relBuilder.call(OR, expr5, expr6)))
    assertEquals(mq.getColumnInterval(calc4, 1), ValueInterval(-5, -1))
    // calc => project + filter: $0 > 90 or $0 <= -1 or $1 < 1.1
    val calc5 = buildCalc(ts, outputRowType, projects,
      List(relBuilder.call(OR, expr4, expr5, expr6)))
    assertEquals(mq.getColumnInterval(calc5, 1), ValueInterval(-5, 5))
    // calc => project + filter: ($0 <=2 and $1 < 1.1) or not(DIV($0, 2) > 3 or $1 > 1.9)
    val calc6 = buildCalc(ts, outputRowType, projects,
      List(relBuilder.call(OR,
        relBuilder.call(AND, expr1, expr4),
        relBuilder.call(NOT, relBuilder.call(OR, expr3, expr7)))))
    assertEquals(mq.getColumnInterval(calc6, 1), ValueInterval(-5, 5))
    // calc => project + filter: ($0 <=2 and $1 < 1.1) or not( $0>2 or $1 > 1.9)
    val calc7 = buildCalc(ts, outputRowType, projects,
      List(relBuilder.call(OR,
        relBuilder.call(AND, expr1, expr4),
        relBuilder.call(NOT,
          relBuilder.call(OR,
            RexUtil.negate(relBuilder.getRexBuilder, expr1.asInstanceOf[RexCall]),
            expr7)))))
    assertEquals(mq.getColumnInterval(calc7, 1), ValueInterval(-5, 2))

    val expr8 = relBuilder.call(CASE, expr5, relBuilder.literal(1), relBuilder.literal(0))
    val expr9 = relBuilder.call(
      CASE, expr5, relBuilder.literal(11), expr7, relBuilder.literal(10), relBuilder.literal(12))
    val expr10 = relBuilder.call(CASE, expr2, expr9, expr4, expr8, relBuilder.literal(null))
    val expr11 = relBuilder.call(CASE, expr5, relBuilder.literal(1), relBuilder.field(1))
    val expr12 = relBuilder.call(
      ScalarSqlFunctions.IF, expr5, relBuilder.literal(1), relBuilder.literal(0))
    val expr13 = relBuilder.call(
      ScalarSqlFunctions.IF, expr2, expr12, relBuilder.literal(null))
    val expr14 = relBuilder.call(
      ScalarSqlFunctions.IF, expr2, relBuilder.literal(1), relBuilder.field(1))
    val rowtype = typeFactory.buildLogicalRowType(
      Array("f0", "f1", "f2", "f3", "f4", "f5", "f6"),
      Array(Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT))
    val calc8 = buildCalc(
      ts, rowtype, List(expr8, expr9, expr10, expr11, expr12, expr13, expr14), List())
    assertEquals(ValueInterval(0, 1), mq.getColumnInterval(calc8, 0))
    assertEquals(ValueInterval(10, 12), mq.getColumnInterval(calc8, 1))
    assertEquals(ValueInterval(0, 12), mq.getColumnInterval(calc8, 2))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(calc8, 3))
    assertEquals(ValueInterval(0, 1), mq.getColumnInterval(calc8, 4))
    assertEquals(ValueInterval(0, 1), mq.getColumnInterval(calc8, 5))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(calc8, 6))
  }

  @Test
  def testGetColumnIntervalOnJoin(): Unit = {
    val left = relBuilder.scan("t1").build()
    // right is $0 <= 2 and $1 < 1.1
    val right = relBuilder.push(left).filter(
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))).build()
    // join condition is left.$0=right.$0 and left.$0 > -1 and right.$1 > 0.1
    val join = relBuilder.push(left).push(right).join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.literal(-1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 1, 1), relBuilder.literal(0.1D))).build
    assertEquals(
      ValueInterval(-1, 5, includeLower = false),
      mq.getColumnInterval(join, 0))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(join, 1))
    assertEquals(ValueInterval(-5, 2), mq.getColumnInterval(join, 2))
    assertEquals(ValueInterval(0.1D, 1.1D, false, false), mq.getColumnInterval(join, 3))
  }

  @Test
  def testGetColumnIntervalOnAggregate(): Unit = {
    val agg = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("score")),
      relBuilder.count(false, "c", relBuilder.field("id"))).build()
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(agg, 0))
    assertEquals(RightSemiInfiniteValueInterval(0), mq.getColumnInterval(agg, 1))

    val agg2 = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("id")),
      relBuilder.avg(false, "avg_score", relBuilder.field("score"))).build()
    assertEquals(ValueInterval(-5, 5), mq.getColumnInterval(agg2, 0))
    assertNull(mq.getColumnInterval(agg2, 1))

    val ts = relBuilder.scan("student").build()
    val agg3 = relBuilder.push(ts).aggregate(
      relBuilder.groupKey(relBuilder.field("id")), Lists.newArrayList(
        AggregateCall.create(SqlAuxiliaryGroupAggFunction, false, false,
          List[Integer](1), -1, 1, ts, null, "score"),
        AggregateCall.create(SqlAuxiliaryGroupAggFunction, false, false,
          List[Integer](2), -1, 1, ts, null, "age"),
        AggregateCall.create(
          new SqlCountAggFunction("COUNT"), false, false, List[Integer](3), -1, 2, ts, null, "h")
      )).build()
    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(agg3, 0))
    assertEquals(ValueInterval(0D, 5.1D), mq.getColumnInterval(agg3, 1))
    assertEquals(ValueInterval(0, 46), mq.getColumnInterval(agg3, 2))
    assertNull(mq.getColumnInterval(agg3, 3))

    val agg4 = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("id")),
      relBuilder.sum(false, "sum", relBuilder.field("score"))
    ).build()
    assertEquals(RightSemiInfiniteValueInterval(0.0, true), mq.getColumnInterval(agg4, 1))

    val agg5 = relBuilder.scan("t5")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("score")),
        relBuilder.sum(false, "sum", relBuilder.field("id"))
      ).build()
    assertEquals(RightSemiInfiniteValueInterval(1, true), mq.getColumnInterval(agg5, 1))
  }

  @Test
  def testGetColumnIntervalOnOverWindowAgg(): Unit = {
    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(overWindowAgg, 0))
    assertEquals(ValueInterval(0D, 5.1D), mq.getColumnInterval(overWindowAgg, 1))
    assertEquals(ValueInterval(0, 46), mq.getColumnInterval(overWindowAgg, 2))
    assertEquals(ValueInterval(161.0D, 172.1D), mq.getColumnInterval(overWindowAgg, 3))
    // cannot estimate valueInterval on aggCall
    assertNull(mq.getColumnInterval(overWindowAgg, 4))
    assertNull(mq.getColumnInterval(overWindowAgg, 5))
  }

  @Test
  def testGetColumnIntervalOnLogicalOverWindow(): Unit = {
    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(logicalOverWindow, 0))
    assertEquals(ValueInterval(0D, 5.1D), mq.getColumnInterval(logicalOverWindow, 1))
    assertEquals(ValueInterval(0, 46), mq.getColumnInterval(logicalOverWindow, 2))
    assertEquals(ValueInterval(161.0D, 172.1D), mq.getColumnInterval(logicalOverWindow, 3))
    // cannot estimate valueInterval on aggCall
    assertNull(mq.getColumnInterval(logicalOverWindow, 4))
    assertNull(mq.getColumnInterval(logicalOverWindow, 5))
  }

  @Test
  def testGetColumnIntervalOnAggregateBatchExec(): Unit = {
    assertEquals(ValueInterval(0D, 6.1D),
      mq.getColumnInterval(unSplittableGlobalAggWithLocalAgg, 0))
    assertEquals(RightSemiInfiniteValueInterval(0),
      mq.getColumnInterval(unSplittableGlobalAggWithLocalAgg, 1))

    assertEquals(ValueInterval(-5, 5), mq.getColumnInterval(splittableGlobalAggWithLocalAgg, 0))
    assertNull(mq.getColumnInterval(splittableGlobalAggWithLocalAgg, 1))

    assertEquals(ValueInterval(0, 10),
      mq.getColumnInterval(unSplittableGlobalAggWithLocalAggAndAuxGrouping, 0))
    assertEquals(ValueInterval(0, 46),
      mq.getColumnInterval(unSplittableGlobalAggWithLocalAggAndAuxGrouping, 1))
    assertEquals(null, mq.getColumnInterval(unSplittableGlobalAggWithLocalAggAndAuxGrouping, 2))
  }

  @Test
  def testGetColumnIntervalOnSort(): Unit = {
    // select * from t1 order by score desc, id
    val sort = relBuilder.scan("t1").sort(
      relBuilder.desc(relBuilder.field("score")),
      relBuilder.field("id")).build
    assertEquals(ValueInterval(-5, 5), mq.getColumnInterval(sort, 0))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(sort, 1))
  }

  @Test
  def testGetColumnIntervalOnSortBatchExec(): Unit = {
    assertEquals(ValueInterval(-5, 5), mq.getColumnInterval(sortBatchExec, 0))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(sortBatchExec, 1))
  }

  @Test
  def testGetColumnIntervalOnSortLimitExec(): Unit = {
    assertEquals(ValueInterval(-5, 5), mq.getColumnInterval(sortLimitBatchExec, 0))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(sortLimitBatchExec, 1))
  }

  @Test
  def testGetColumnIntervalOnLimitExec(): Unit = {
    assertEquals(ValueInterval(-5, 5), mq.getColumnInterval(limitBatchExec, 0))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(limitBatchExec, 1))
  }

  @Test
  def testGetColumnIntervalOnProject(): Unit = {
    assertEquals(mq.getColumnInterval(project, 1), ValueInterval(-5, 5))
    assertNull(mq.getColumnInterval(project, 0))
    assertEquals(mq.getColumnInterval(project, 2), ValueInterval(0D, 6.1D))
    assertEquals(mq.getColumnInterval(project, 3), ValueInterval(true, true))
    assertEquals(mq.getColumnInterval(project, 4), ValueInterval(2.1D, 2.1D))
    assertEquals(mq.getColumnInterval(project, 5), ValueInterval(2L, 2L))
  }

  @Test
  def testGetColumnIntervalOnExchange(): Unit = {
    val ts = relBuilder.scan("t1").build()
    val exchange = LogicalExchange.create(ts, RelDistributions.SINGLETON)
    assertEquals(mq.getColumnInterval(exchange, 0), ValueInterval(-5, 5))
    assertEquals(mq.getColumnInterval(exchange, 1), ValueInterval(0D, 6.1D))
  }

  @Test
  def testGetColumnIntervalOnUnion(): Unit = {
    val ts1 = relBuilder.scan("t1").build()
    val ts2 = relBuilder.scan("t2").build()
    val union = relBuilder.push(ts1).push(ts2).union(true).build()
    assertEquals(mq.getColumnInterval(union, 0), ValueInterval(-5, 10))
    assertEquals(mq.getColumnInterval(union, 1), ValueInterval(0D, 6.1D))
  }

  @Test
  def testGetColumnIntervalOnValues(): Unit = {
    assertEquals(mq.getColumnInterval(emptyValues, 0), ValueInterval.empty)

    assertEquals(mq.getColumnInterval(values, 0), ValueInterval(1L, 3L))
    assertEquals(mq.getColumnInterval(values, 1), ValueInterval(false, true))
    assertEquals(mq.getColumnInterval(values, 2),
      ValueInterval(
        new Date(new DateString(2017, 9, 1).getMillisSinceEpoch),
        new Date(new DateString(2017, 10, 2).getMillisSinceEpoch)))
    assertEquals(mq.getColumnInterval(values, 3),
      ValueInterval(
        new Time(new TimeString(9, 59, 59).toCalendar.getTimeInMillis),
        new Time(new TimeString(10, 0, 2).toCalendar.getTimeInMillis)))
    assertEquals(mq.getColumnInterval(values, 4),
      ValueInterval(
        new Timestamp(new TimestampString(2017, 7, 1, 1, 0, 0).getMillisSinceEpoch),
        new Timestamp(new TimestampString(2017, 10, 1, 1, 0, 0).getMillisSinceEpoch)))
    assertEquals(mq.getColumnInterval(values, 5), ValueInterval(-1D, 3.12D))
    assertEquals(mq.getColumnInterval(values, 6), ValueInterval.empty)
  }

  @Test
  def testGetColumnIntervalOnExpand(): Unit = {
    assertEquals(ValueInterval(-5D, 5D), mq.getColumnInterval(aggWithExpand, 0))
    assertEquals(ValueInterval(0D, 6.1D), mq.getColumnInterval(aggWithExpand, 1))
    assertEquals(ValueInterval(1L, 2L), mq.getColumnInterval(aggWithExpand, 2))

    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(aggWithAuxGroupAndExpand, 0))
    assertEquals(ValueInterval(1L, 2L), mq.getColumnInterval(aggWithAuxGroupAndExpand, 1))
    assertEquals(ValueInterval(0D, 5.1D), mq.getColumnInterval(aggWithAuxGroupAndExpand, 2))
    assertEquals(ValueInterval(0, 46), mq.getColumnInterval(aggWithAuxGroupAndExpand, 3))
    assertEquals(null, mq.getColumnInterval(aggWithAuxGroupAndExpand, 4))
  }

  @Test
  def testGetColumnIntervalOnFlinkLogicalWindowAggregate(): Unit = {
    assertEquals(ValueInterval(5, 45), mq.getColumnInterval(flinkLogicalWindowAgg, 0))
    assertEquals(null, mq.getColumnInterval(flinkLogicalWindowAgg, 1))
    assertEquals(RightSemiInfiniteValueInterval(0), mq.getColumnInterval(flinkLogicalWindowAgg, 2))
    assertEquals(null, mq.getColumnInterval(flinkLogicalWindowAgg, 3))

    assertEquals(ValueInterval(5, 55), mq.getColumnInterval(flinkLogicalWindowAggWithAuxGroup, 0))
    assertEquals(ValueInterval(0, 50), mq.getColumnInterval(flinkLogicalWindowAggWithAuxGroup, 1))
    assertEquals(null, mq.getColumnInterval(flinkLogicalWindowAggWithAuxGroup, 2))
    assertEquals(RightSemiInfiniteValueInterval(0),
      mq.getColumnInterval(flinkLogicalWindowAggWithAuxGroup, 3))
  }

  @Test
  def testGetColumnIntervalOnLogicalWindowAggregate(): Unit = {
    assertEquals(ValueInterval(5, 45), mq.getColumnInterval(logicalWindowAgg, 0))
    assertEquals(null, mq.getColumnInterval(logicalWindowAgg, 1))
    assertEquals(RightSemiInfiniteValueInterval(0), mq.getColumnInterval(logicalWindowAgg, 2))
    assertEquals(null, mq.getColumnInterval(logicalWindowAgg, 3))

    assertEquals(ValueInterval(5, 55), mq.getColumnInterval(logicalWindowAggWithAuxGroup, 0))
    assertEquals(ValueInterval(0, 50), mq.getColumnInterval(logicalWindowAggWithAuxGroup, 1))
    assertEquals(null, mq.getColumnInterval(logicalWindowAggWithAuxGroup, 2))
    assertEquals(RightSemiInfiniteValueInterval(0),
      mq.getColumnInterval(logicalWindowAggWithAuxGroup, 3))
  }

  @Test
  def testGetColumnIntervalOnWindowAggregateBatchExec(): Unit = {
    assertEquals(ValueInterval(5, 45), mq.getColumnInterval(globalWindowAggWithLocalAgg, 0))
    assertEquals(null, mq.getColumnInterval(globalWindowAggWithLocalAgg, 1))
    assertEquals(RightSemiInfiniteValueInterval(0),
      mq.getColumnInterval(globalWindowAggWithLocalAgg, 2))
    assertEquals(null, mq.getColumnInterval(globalWindowAggWithLocalAgg, 3))
    assertEquals(ValueInterval(5, 45), mq.getColumnInterval(globalWindowAggWithoutLocalAgg, 0))
    assertEquals(null, mq.getColumnInterval(globalWindowAggWithoutLocalAgg, 1))
    assertEquals(RightSemiInfiniteValueInterval(0),
                 mq.getColumnInterval(globalWindowAggWithoutLocalAgg, 2))
    assertEquals(null, mq.getColumnInterval(globalWindowAggWithoutLocalAgg, 3))

    assertEquals(ValueInterval(5, 55),
      mq.getColumnInterval(globalWindowAggWithLocalAggWithAuxGrouping, 0))
    assertEquals(ValueInterval(0, 50),
      mq.getColumnInterval(globalWindowAggWithLocalAggWithAuxGrouping, 1))
    assertEquals(RightSemiInfiniteValueInterval(0),
      mq.getColumnInterval(globalWindowAggWithLocalAggWithAuxGrouping, 2))
    assertEquals(null, mq.getColumnInterval(globalWindowAggWithLocalAggWithAuxGrouping, 3))
    assertEquals(ValueInterval(5, 55),
      mq.getColumnInterval(globalWindowAggWithoutLocalAggWithAuxGrouping, 0))
    assertEquals(ValueInterval(0, 50),
      mq.getColumnInterval(globalWindowAggWithoutLocalAggWithAuxGrouping, 1))
    assertEquals(RightSemiInfiniteValueInterval(0),
                 mq.getColumnInterval(globalWindowAggWithoutLocalAggWithAuxGrouping, 2))
    assertEquals(null, mq.getColumnInterval(globalWindowAggWithoutLocalAggWithAuxGrouping, 3))
  }

  @Test
  def testGetColumnIntervalOnRank(): Unit = {
    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(flinkLogicalRank, 0))
    assertEquals(ValueInterval(0D, 5.1D), mq.getColumnInterval(flinkLogicalRank, 1))
    assertEquals(ValueInterval(0, 46), mq.getColumnInterval(flinkLogicalRank, 2))
    assertEquals(ValueInterval(1, 5), mq.getColumnInterval(flinkLogicalRank, 4))

    assertEquals(ValueInterval(161.0D, 172.1D),
      mq.getColumnInterval(flinkLogicalRankWithVariableRankRange, 3))
    assertEquals(ValueInterval(1D,172.1D),
      mq.getColumnInterval(flinkLogicalRankWithVariableRankRange, 4))

    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(flinkLogicalRowNumber, 0))
    assertEquals(ValueInterval(0D, 5.1D), mq.getColumnInterval(flinkLogicalRowNumber, 1))

    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(globalBatchExecRank, 0))
    assertEquals(ValueInterval(3, 5), mq.getColumnInterval(globalBatchExecRank, 4))

    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(localBatchExecRank, 0))

    assertEquals(ValueInterval(0, 10), mq.getColumnInterval(streamExecRowNumber, 0))
    assertEquals(ValueInterval(0D, 5.1D), mq.getColumnInterval(streamExecRowNumber, 1))
  }
}

