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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalExpand
import org.apache.flink.table.planner.plan.utils.ExpandUtil

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{GREATER_THAN, LESS_THAN_OR_EQUAL, MULTIPLY, PLUS}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdUniqueGroupsTest extends FlinkRelMdHandlerTestBase {

  @Test(expected = classOf[IllegalArgumentException])
  def testGetUniqueGroupsOnTableScanWithNullColumns(): Unit = {
    mq.getUniqueGroups(studentLogicalScan, null)
  }

  @Test
  def testGetUniqueGroupsOnTableScan(): Unit = {
    Array(studentLogicalScan, empFlinkLogicalScan, studentBatchScan, studentStreamScan,
      empFlinkLogicalScan, empFlinkLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(ImmutableBitSet.of(), mq.getUniqueGroups(scan, ImmutableBitSet.of()))
      (0 until scan.getRowType.getFieldCount).foreach { idx =>
        assertEquals(ImmutableBitSet.of(idx), mq.getUniqueGroups(scan, ImmutableBitSet.of(idx)))
      }
    }

    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(scan, ImmutableBitSet.of(0, 1)))
        assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(scan, ImmutableBitSet.of(0, 1, 2)))
        assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(scan, ImmutableBitSet.of(1, 2)))
      }

    Array(empFlinkLogicalScan, empFlinkLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(scan, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0, 1, 2),
        mq.getUniqueGroups(scan, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(scan, ImmutableBitSet.of(1, 2)))
    }

    val ts = relBuilder.scan("MyTable4").build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(ts, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(ts, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testGetUniqueGroupsOnFilter(): Unit = {
    relBuilder.scan("MyTable3")
    // a <= 2 and b > 10
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10D))
    val filter1 = relBuilder.filter(List(expr1, expr2)).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(filter1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(filter1, ImmutableBitSet.of(0, 1)))

    relBuilder.clear()
    relBuilder.scan("MyTable4")
    // a <= 2 and b > 10
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
    relBuilder.scan("MyTable4")
    // a, b, c
    val proj1 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.field(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj1, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj1, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(proj1, ImmutableBitSet.of(1, 2)))

    relBuilder.clear()
    relBuilder.scan("MyTable4")
    // a + b, b * 2, 1
    val proj2 = relBuilder.project(List(
      relBuilder.call(PLUS, relBuilder.field(0), relBuilder.field(1)),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.literal(1))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(proj2, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(proj2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(proj2, ImmutableBitSet.of(0, 1, 2)))

    relBuilder.clear()
    relBuilder.scan("MyTable4")
    // a, b * 2, c, 1, 2
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
    relBuilder.scan("MyTable4")
    // a, a as a1, $2
    val proj4 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.alias(relBuilder.field(0), "a1"),
      relBuilder.field(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj4, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj4, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj4, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(proj4, ImmutableBitSet.of(1, 2)))

    relBuilder.clear()
    relBuilder.scan("MyTable4")
    // true, 1
    val proj5 = relBuilder.project(List(
      relBuilder.literal(true),
      relBuilder.literal(1))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj5, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(proj5, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(proj5, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueGroupsOnCalc(): Unit = {
    val ts = relBuilder.scan("MyTable4").build()
    // project: a, b * 2, c, 1, 2
    // filter: a > 1
    relBuilder.push(ts)
    val projects = List(
      relBuilder.field(0),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.field(2),
      relBuilder.literal(1),
      relBuilder.literal(2))
    val condition = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val outputRowType = relBuilder.push(ts).project(projects).build().getRowType
    val calc = createLogicalCalc(ts, outputRowType, projects, List(condition))

    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(calc, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(calc, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(calc, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueGroups(calc, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(calc, ImmutableBitSet.of(0, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(calc, ImmutableBitSet.of(3, 4)))
  }

  @Test
  def testGetUniqueGroupsOnExpand(): Unit = {
    // column 0 is unique key
    val ts = studentLogicalScan
    val expandOutputType = ExpandUtil.buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects1 = ExpandUtil.createExpandProjects(
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
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects1, 7)
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(expand1, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(expand1, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(expand1, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(7), mq.getUniqueGroups(expand1, ImmutableBitSet.of(7)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(expand1, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 7), mq.getUniqueGroups(expand1, ImmutableBitSet.of(0, 7)))
    assertEquals(ImmutableBitSet.of(0, 1, 7),
      mq.getUniqueGroups(expand1, ImmutableBitSet.of(0, 1, 7)))

    val expandProjects2 = ExpandUtil.createExpandProjects(
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
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects2, 7)
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 7)), mq.getUniqueKeys(expand2))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(expand2, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(expand2, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(expand2, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(7), mq.getUniqueGroups(expand2, ImmutableBitSet.of(7)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 7), mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 7)))
    assertEquals(ImmutableBitSet.of(0, 1, 7),
      mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 1, 7)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 7),
      mq.getUniqueGroups(expand2, ImmutableBitSet.of(0, 1, 2, 3, 7)))

    val expandProjects3 = ExpandUtil.createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0, 1, 2),
        ImmutableBitSet.of(0, 1, 3)
      ), Array.empty[Integer])
    val expand3 = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects3, 7)
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 7)), mq.getUniqueKeys(expand2))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueGroups(expand3, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueGroups(expand3, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueGroups(expand3, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(7), mq.getUniqueGroups(expand3, ImmutableBitSet.of(7)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 2), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 2)))
    assertEquals(ImmutableBitSet.of(0, 7), mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 7)))
    assertEquals(ImmutableBitSet.of(0, 7),
      mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 1, 7)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 7),
      mq.getUniqueGroups(expand3, ImmutableBitSet.of(0, 1, 2, 3, 7)))
  }

  @Test
  def testGetUniqueGroupsOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach { exchange =>
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(exchange, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(exchange, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(exchange, ImmutableBitSet.of(1, 2)))
    }
  }

  @Test
  def testGetUniqueGroupsOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalLimit, flinkLogicalLimit, batchLimit, batchLocalLimit, batchGlobalLimit, streamLimit,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, batchLocalSortLimit,
      batchGlobalSortLimit, streamSortLimit).foreach { sort =>
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(sort, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(sort, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(sort, ImmutableBitSet.of(1, 2)))
    }
  }

  @Test
  def testGetUniqueGroupsOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithoutLocal, batchGlobalAggWithLocal,
      batchLocalAgg).foreach { agg =>
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(agg, ImmutableBitSet.of(1, 2)))
    }

    Array(logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup, batchGlobalAggWithLocalWithAuxGroup,
      batchLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2, 3)))
      assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(agg, ImmutableBitSet.of(1, 2)))
      assertEquals(ImmutableBitSet.of(1, 2, 3),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(1, 2, 3)))
    }

    val agg = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(),
      relBuilder.count(false, "c", relBuilder.field("id")),
      relBuilder.avg(false, "a", relBuilder.field("age"))).build()
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueGroupsOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg,
      batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
      assertEquals(ImmutableBitSet.of(3, 4, 5, 6),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(3, 4, 5, 6)))
      assertEquals(ImmutableBitSet.of(0, 3, 4, 5, 6),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 3, 4, 5, 6)))
      assertEquals(ImmutableBitSet.of(0, 1),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0, 1, 2),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2)))
    }
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueGroups(batchLocalWindowAgg, ImmutableBitSet.of(0, 1, 2, 3)))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(ImmutableBitSet.of(1),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(1)))
      assertEquals(ImmutableBitSet.of(0),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0, 1, 2),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2, 3)))
      assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
    }
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueGroups(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueGroups(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
  }

  @Test
  def testGetUniqueGroupsOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach { agg =>
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1)))
      assertEquals(ImmutableBitSet.of(0), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2)))
      assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueGroups(agg, ImmutableBitSet.of(1, 2)))
      assertEquals(ImmutableBitSet.of(0, 6), mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 6)))
      assertEquals(ImmutableBitSet.of(0, 6, 7, 8),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(0, 1, 2, 6, 7, 8)))
      assertEquals(ImmutableBitSet.of(1, 6, 7, 8),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(1, 6, 7, 8)))
      assertEquals(ImmutableBitSet.of(6, 7, 8),
        mq.getUniqueGroups(agg, ImmutableBitSet.of(6, 7, 8)))
    }
  }

  @Test
  def testGetUniqueGroupsOnJoin(): Unit = {
    // inner join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(3, 4),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(3, 4)))
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1, 5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(5),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 6, 7, 8),
      mq.getUniqueGroups(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0, 2, 3, 4, 6, 7, 8)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinOnLHSUniqueKeys, ImmutableBitSet.of(5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinOnLHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8),
      mq.getUniqueGroups(logicalInnerJoinOnLHSUniqueKeys,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(logicalInnerJoinOnRHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(6),
      mq.getUniqueGroups(logicalInnerJoinOnRHSUniqueKeys, ImmutableBitSet.of(5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(logicalInnerJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(logicalInnerJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinNotOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinNotOnUniqueKeys, ImmutableBitSet.of(5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinNotOnUniqueKeys,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinNotOnUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // with equi join condition and non-equi join condition
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinWithEquiAndNonEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinWithEquiAndNonEquiCond, ImmutableBitSet.of(5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinWithEquiAndNonEquiCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8),
      mq.getUniqueGroups(logicalInnerJoinWithEquiAndNonEquiCond,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalInnerJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinWithoutEquiCond, ImmutableBitSet.of(5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalInnerJoinWithoutEquiCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // left outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5),
      mq.getUniqueGroups(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalLeftJoinOnUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 5),
      mq.getUniqueGroups(logicalLeftJoinOnUniqueKeys,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(logicalLeftJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueGroups(logicalLeftJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(1, 2, 3, 4, 5, 7, 8, 9)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalLeftJoinOnLHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalLeftJoinNotOnUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalLeftJoinNotOnUniqueKeys,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalLeftJoinWithoutEquiCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalLeftJoinWithoutEquiCond,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalLeftJoinWithEquiAndNonEquiCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalLeftJoinWithEquiAndNonEquiCond,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9)))

    // right outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5),
      mq.getUniqueGroups(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(5),
      mq.getUniqueGroups(logicalRightJoinOnUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(1, 6, 7, 8),
      mq.getUniqueGroups(logicalRightJoinOnUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 6, 7, 8)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 6),
      mq.getUniqueGroups(logicalRightJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 7, 8, 9),
      mq.getUniqueGroups(logicalRightJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 7, 8, 9)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalRightJoinOnLHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalRightJoinOnLHSUniqueKeys,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalRightJoinWithoutEquiCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalRightJoinWithEquiAndNonEquiCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // full outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(5),
      mq.getUniqueGroups(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(1, 5),
      mq.getUniqueGroups(logicalFullJoinOnUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 6, 7, 8),
      mq.getUniqueGroups(logicalFullJoinOnUniqueKeys,
        ImmutableBitSet.of(0, 2, 3, 4, 6, 7, 8)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 6),
      mq.getUniqueGroups(logicalFullJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 7, 8, 9),
      mq.getUniqueGroups(logicalFullJoinOnRHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 7, 8, 9)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalFullJoinOnLHSUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalFullJoinOnLHSUniqueKeys,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalFullJoinNotOnUniqueKeys,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    assertEquals(ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalFullJoinWithEquiAndNonEquiCond,
        ImmutableBitSet.of(0, 2, 3, 4, 5, 6, 7, 8, 9)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalFullJoinWithEquiAndNonEquiCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(1, 5, 6, 7, 8, 9),
      mq.getUniqueGroups(logicalFullJoinWithoutCond,
        ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))

    // semi join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalSemiJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(logicalSemiJoinOnRHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalSemiJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalSemiJoinNotOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalSemiJoinWithEquiAndNonEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // anti join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalAntiJoinOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(logicalAntiJoinOnRHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalAntiJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalAntiJoinNotOnUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueGroups(logicalAntiJoinWithEquiAndNonEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))
  }

  @Test
  def testGetUniqueGroupsOnUnion(): Unit = {
    Array(logicalUnion, logicalUnionAll, logicalIntersect, logicalIntersectAll, logicalMinus,
      logicalMinusAll).foreach { setOp =>
      assertEquals(ImmutableBitSet.of(0, 1, 3, 4),
        mq.getUniqueGroups(setOp, ImmutableBitSet.of(0, 1, 3, 4)))
      assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
        mq.getUniqueGroups(setOp, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    }
  }

  @Test
  def testGetUniqueGroupsOnDefault(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 3, 4),
      mq.getUniqueGroups(testRel, ImmutableBitSet.of(0, 1, 3, 4)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4),
      mq.getUniqueGroups(testRel, ImmutableBitSet.of(0, 1, 2, 3, 4)))
  }

}
