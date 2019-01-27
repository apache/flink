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

import org.apache.flink.table.plan.nodes.logical.FlinkLogicalExpand
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule.{buildExpandRowType, createExpandProjects}

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.logical.LogicalExchange
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdUniqueGroupsTest extends FlinkRelMdHandlerTestBase {

  @Test(expected = classOf[IllegalArgumentException])
  def testGetUniqueGroupsOnTableScanWithNullColumns(): Unit = {
    val ts1 = relBuilder.scan("t1").build()
    mq.getUniqueGroups(ts1, null)
  }

  @Test
  def testGetUniqueGroupsOnTableScan(): Unit = {
    val ts1 = relBuilder.scan("t1").build()
    val ts2 = relBuilder.scan("t2").build()
    val ts3 = relBuilder.scan("t3").build()
    val ts7 = relBuilder.scan("t7").build()

    val empty = ImmutableBitSet.of()
    assertEquals(empty, mq.getUniqueGroups(ts1, empty))
    assertEquals(empty, mq.getUniqueGroups(ts2, empty))
    assertEquals(empty, mq.getUniqueGroups(ts3, empty))

    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts3, ImmutableBitSet.of(0)))

    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(ts2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts3, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts7, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts7, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts7, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(ts7, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testGetUniqueGroupsOnFilter(): Unit = {
    relBuilder.scan("t1")
    // filter: $0 <= 2 and $1 > 10
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10D))
    val filter1 = relBuilder.filter(List(expr1, expr2)).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(filter1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(filter1, ImmutableBitSet.of(0, 1)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // filter: $0 <= 2 and $1 > 10
    val expr3 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr4 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10D))
    val filter2 = relBuilder.filter(List(expr3, expr4)).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(filter2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(filter2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(filter2, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(filter2, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testGetUniqueGroupsOnProject(): Unit = {
    relBuilder.scan("t7")
    // project: $0, $1, $2
    val proj1 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.field(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj1, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj1, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(proj1, ImmutableBitSet.of(1, 2)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: $0 + $1, $1 * 2, 1
    val proj2 = relBuilder.project(List(
      relBuilder.call(PLUS, relBuilder.field(0), relBuilder.field(1)),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.literal(1))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(proj2, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(proj2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(proj2, ImmutableBitSet.of(0, 1, 2)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: $0, $1 * 2, $2, 1, 2
    val proj3 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.field(2),
      relBuilder.literal(1),
      relBuilder.literal(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj3, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(proj3, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(proj3, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(proj3, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj3, ImmutableBitSet.of(0, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(proj3, ImmutableBitSet.of(3, 4)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: $0, $0 as a1, $2
    val proj4 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.alias(relBuilder.field(0), "a1"),
      relBuilder.field(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj4, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj4, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj4, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(proj4, ImmutableBitSet.of(1, 2)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: true, 1
    val proj5 = relBuilder.project(List(
      relBuilder.literal(true),
      relBuilder.literal(1))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj5, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(proj5, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj5, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueGroupsOnCalc(): Unit = {
    val ts = relBuilder.scan("t7").build()
    // project: $0, $1 * 2, $2, 1, 2  condition: a > 1
    relBuilder.push(ts)
    val projects = List(
      relBuilder.field(0),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.field(2),
      relBuilder.literal(1),
      relBuilder.literal(2))
    val condition = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val outputRowType = relBuilder.push(ts).project(projects).build().getRowType
    val calc = buildCalc(ts, outputRowType, projects, List(condition))

    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(calc, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(calc, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(calc, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(calc, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(calc, ImmutableBitSet.of(0, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(calc, ImmutableBitSet.of(3, 4)))
  }

  @Test
  def testGetUniqueGroupsOnUnion(): Unit = {
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(union, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(union, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(union, ImmutableBitSet.of(0, 1)))

    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(unionAll, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(unionAll, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(unionAll, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueGroupsOnExchange(): Unit = {
    val ts = relBuilder.scan("t3").build()
    val exchange = LogicalExchange.create(ts, RelDistributions.SINGLETON)
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(exchange, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueGroupsOnSort(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(sort, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(limitBatchExec, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(sortBatchExec, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(sortLimitBatchExec, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueGroupsOnJoin(): Unit = {
    // inner join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(innerJoin, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(innerJoin, ImmutableBitSet.of(2, 3, 4)))
    assertEquals(ImmutableBitSet.of(3, 4), mq.getUniqueGroups(innerJoin, ImmutableBitSet.of(3, 4)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(innerJoin, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(innerJoin, ImmutableBitSet.of(0, 1, 3, 4, 5)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(innerJoinOnOneSideUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueGroups(innerJoinOnOneSideUniqueKeys, ImmutableBitSet.of(2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(innerJoinOnOneSideUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(innerJoinOnScore, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueGroups(innerJoinOnScore, ImmutableBitSet.of(2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(innerJoinOnScore, ImmutableBitSet.of(1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(innerJoinOnScore, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (no unique keys)
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(innerJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueGroups(innerJoinOnNoneUniqueKeys, ImmutableBitSet.of(2, 3)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(innerJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(innerJoinWithNonEquiCond, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 3, 4),
      mq.getUniqueGroups(innerJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 3, 4)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(innerJoinWithoutEquiCond, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueGroups(innerJoinWithoutEquiCond, ImmutableBitSet.of(2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(innerJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // left outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(leftJoin, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(leftJoin, ImmutableBitSet.of(1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(leftJoinOnRHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(0, 4, 5),
      mq.getUniqueGroups(leftJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(leftJoinOnScore, ImmutableBitSet.of(1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(leftJoinOnScore, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (no unique keys)
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(leftJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(leftJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(0, 2, 3),
      mq.getUniqueGroups(leftJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 2, 3)))

    // right outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueGroups(rightJoin, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueGroups(rightJoin, ImmutableBitSet.of(1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(rightJoinOnRHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(4, 5),
      mq.getUniqueGroups(rightJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(rightJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueGroups(rightJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 2, 3)))

    // full outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(fullJoin, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(fullJoin, ImmutableBitSet.of(1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(fullJoinOnOneSideUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(fullJoinOnScore, ImmutableBitSet.of(1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(fullJoinOnScore, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (no unique keys)
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(fullJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(0, 3, 4),
      mq.getUniqueGroups(fullJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 3, 4)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueGroups(fullJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
  }

  @Test
  def testGetUniqueGroupsOnSemiJoin(): Unit = {
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(semiJoin, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(antiJoin, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueGroupsOnAggregate(): Unit = {
    val agg1 = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(relBuilder.field("score")),
      relBuilder.count(false, "c", relBuilder.field("id")),
      relBuilder.avg(false, "a", relBuilder.field("age"))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg1, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(agg1, ImmutableBitSet.of(1, 2)))

    val agg2 = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("age")),
      relBuilder.avg(false, "s", relBuilder.field("score")),
      relBuilder.avg(false, "h", relBuilder.field("height"))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg2, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 2, 3),
      mq.getUniqueGroups(agg2, ImmutableBitSet.of(0, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(agg2, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(2, 3), mq.getUniqueGroups(agg2, ImmutableBitSet.of(2, 3)))

    val agg3 = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(),
      relBuilder.count(false, "c", relBuilder.field("id")),
      relBuilder.avg(false, "a", relBuilder.field("age"))).build()
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(agg3, ImmutableBitSet.of(0, 1)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(aggWithAuxGroup, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(aggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(aggWithAuxGroup, ImmutableBitSet.of(0, 2)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(aggWithAuxGroup, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(aggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
  }

  @Test
  def testGetUniqueGroupsOnLogicalOverWindow(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 4, 5),
      mq.getUniqueGroups(logicalOverWindow, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(logicalOverWindow, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueGroups(logicalOverWindow, ImmutableBitSet.of(0, 1, 2, 4)))
  }

  @Test
  def testGetUniqueGroupsOnBatchExecOverWindow(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 4, 5),
      mq.getUniqueGroups(overWindowAgg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(overWindowAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueGroups(overWindowAgg, ImmutableBitSet.of(0, 1, 2, 4)))
  }

  @Test
  def testGetUniqueGroupsOnValues(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
      mq.getUniqueGroups(values, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(1, 3, 6),
      mq.getUniqueGroups(values, ImmutableBitSet.of(1, 3, 6)))
  }

  @Test
  def testGetUniqueGroupsOnExpand(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(aggWithExpand, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(aggWithExpand, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(aggWithExpand, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueGroups(aggWithExpand, ImmutableBitSet.of(2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(aggWithExpand, ImmutableBitSet.of(1, 2)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2, 3)))
    assertEquals(ImmutableBitSet.of(2, 3, 4),
      mq.getUniqueGroups(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2, 3, 4)))

    // column 0 is unique key
    val ts = relBuilder.scan("student").build()
    val expandOutputType = buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects1 = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(2),
        ImmutableBitSet.of(3)
      ), Array.empty[Integer])
    val expand1 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects1, 4)
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(expand1, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(expand1, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(expand1, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(4), mq.getUniqueGroups(expand1, ImmutableBitSet.of(4)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(expand1, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 4), mq.getUniqueGroups(expand1, ImmutableBitSet.of(0, 4)))
    assertEquals(ImmutableBitSet.of(0, 1, 4),
      mq.getUniqueGroups(expand1, ImmutableBitSet.of(0, 1, 4)))

    val expandProjects2 = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0, 1),
        ImmutableBitSet.of(0, 2),
        ImmutableBitSet.of(0, 3)
      ), Array.empty[Integer])
    val expand2 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects2, 4)
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 4)), mq.getUniqueKeys(expand2))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(expand2, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(expand2, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(expand2, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(4), mq.getUniqueGroups(expand2, ImmutableBitSet.of(4)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 4), mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 4)))
    assertEquals(ImmutableBitSet.of(0, 1, 4),
      mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 1, 4)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    val expandProjects3 = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0, 1, 2),
        ImmutableBitSet.of(0, 1, 3)
      ), Array.empty[Integer])
    val expand3 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects3, 4)
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 4)), mq.getUniqueKeys(expand2))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(expand3, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(expand3, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(expand3, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(4), mq.getUniqueGroups(expand3, ImmutableBitSet.of(4)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 2), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 2)))
    assertEquals(ImmutableBitSet.of(0, 4), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 4)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 1, 4)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4),
      mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 1, 2, 3, 4)))
  }

  @Test
  def testGetUniqueGroupsOnFlinkLogicalWindowAggregate(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
      mq.getUniqueGroups(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(3, 4, 5, 6),
      mq.getUniqueGroups(flinkLogicalWindowAgg, ImmutableBitSet.of(3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 3, 4, 5, 6),
      mq.getUniqueGroups(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1, 2)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2, 3)))
  }

  @Test
  def testGetUniqueGroupsOnLogicalWindowAggregate(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
      mq.getUniqueGroups(logicalWindowAgg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(3, 4, 5, 6),
      mq.getUniqueGroups(logicalWindowAgg, ImmutableBitSet.of(3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 3, 4, 5, 6),
      mq.getUniqueGroups(logicalWindowAgg, ImmutableBitSet.of(0, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(logicalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(logicalWindowAgg, ImmutableBitSet.of(0, 1, 2)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2, 3)))
  }

  @Test
  def testGetUniqueGroupsOnBatchExecWindowAggregate(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(localWindowAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(localWindowAgg, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(localWindowAgg, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 3),
      mq.getUniqueGroups(localWindowAgg, ImmutableBitSet.of(1, 3)))

    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(globalWindowAggWithLocalAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 3),
      mq.getUniqueGroups(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1, 3)))

    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 3),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1, 3)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(localWindowAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(localWindowAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(localWindowAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(localWindowAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2, 3)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2), mq.getUniqueGroups(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3), mq.getUniqueGroups(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2, 3)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2), mq.getUniqueGroups(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3), mq.getUniqueGroups(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2, 3)))
  }

  @Test
  def testGetUniqueGroupsOnRank(): Unit = {
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(flinkLogicalRank, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(flinkLogicalRank, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueGroups(flinkLogicalRank, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueGroups(flinkLogicalRank, ImmutableBitSet.of(1, 2, 3, 4)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueGroups(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueGroups(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(1, 2, 3, 4)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(globalBatchExecRank, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(globalBatchExecRank, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueGroups(globalBatchExecRank, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueGroups(globalBatchExecRank, ImmutableBitSet.of(1, 2, 3, 4)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(localBatchExecRank, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(localBatchExecRank, ImmutableBitSet.of(1, 2, 3)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(streamExecRowNumber, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueGroups(streamExecRowNumber, ImmutableBitSet.of(1, 2, 3)))
  }
}

