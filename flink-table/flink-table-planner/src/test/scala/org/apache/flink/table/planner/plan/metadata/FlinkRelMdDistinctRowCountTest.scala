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

import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRank
import org.apache.flink.table.planner.plan.utils.FlinkRelMdUtil

import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdDistinctRowCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetDistinctRowCountOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(), null))
      assertEquals(50.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), null))
      assertEquals(48.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), null))
      assertEquals(20.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2), null))
      assertEquals(7.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(3), null))
      assertEquals(35.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(4), null))
      assertEquals(2.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(5), null))
      assertEquals(null, mq.getDistinctRowCount(scan, ImmutableBitSet.of(6), null))
      assertEquals(50.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 3), null))
      assertEquals(40.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 5), null))

      // age = 16
      val expr = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
      assertEquals(1.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(), expr))
      assertEquals(7.14, mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), expr), 1e-2)
      assertEquals(7.12, mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), expr), 1e-2)
      assertEquals(6.39, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2), expr), 1e-2)
      assertEquals(4.67, mq.getDistinctRowCount(scan, ImmutableBitSet.of(3), expr), 1e-2)
      assertEquals(6.92, mq.getDistinctRowCount(scan, ImmutableBitSet.of(4), expr), 1e-2)
      assertEquals(1.96, mq.getDistinctRowCount(scan, ImmutableBitSet.of(5), expr), 1e-2)
      assertEquals(null, mq.getDistinctRowCount(scan, ImmutableBitSet.of(6), expr))
      assertEquals(7.14, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 3), expr), 1e-2)
      assertEquals(7.01, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 5), expr), 1e-2)
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(), null))
      assertNull(mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), null))
      // empno = 1
      val condition = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(0), relBuilder.literal(1))
      assertEquals(1.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(), condition))
      assertNull(mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), condition))
    }
  }

  @Test
  def testGetDistinctRowCountOnValues(): Unit = {
    assertEquals(1.0, mq.getDistinctRowCount(logicalValues, ImmutableBitSet.of(), null))
    (0 until logicalValues.getRowType.getFieldCount).foreach { idx =>
      assertEquals(FlinkRelMdUtil.numDistinctVals(2.0, 2.0),
        mq.getDistinctRowCount(logicalValues, ImmutableBitSet.of(idx), null))
    }
    assertEquals(FlinkRelMdUtil.numDistinctVals(2.0, 2.0),
      mq.getDistinctRowCount(logicalValues, ImmutableBitSet.of(0, 1), null))

    (0 until logicalValues.getRowType.getFieldCount).foreach { idx =>
      assertEquals(1.0, mq.getDistinctRowCount(emptyValues, ImmutableBitSet.of(idx), null))
    }
  }

  @Test
  def testGetDistinctRowCountOnProject(): Unit = {
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(), null))
    assertEquals(50.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(0), null))
    assertEquals(48.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(1), null))
    assertEquals(17.13,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(2), null), 1e-2)
    assertEquals(6.99,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(3), null), 1e-2)
    assertEquals(21.90,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(4), null), 1e-2)
    assertEquals(21.90,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(5), null), 1e-2)
    assertEquals(35.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(6), null))
    // TODO check result ??
    assertEquals(5.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(7), null), 1e-2)
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(8), null))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(9), null))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(10), null))
    assertEquals(17.13,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(11), null), 1e-2)

    // id > 10
    val expr1 = relBuilder.push(logicalProject)
      .call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(10))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(), expr1))
    assertEquals(25.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(0), expr1))
    assertEquals(24.68, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(1), expr1), 1e-2)
    assertEquals(17.13,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(2), expr1), 1e-2)
    assertEquals(6.99, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(3), expr1), 1e-2)
    assertEquals(21.90,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(4), expr1), 1e-2)
    assertEquals(21.90,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(5), expr1), 1e-2)
    assertEquals(22.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(6), expr1), 1e-2)
    // TODO check result ??
    assertEquals(5.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(7), expr1), 1e-2)
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(8), expr1))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(9), expr1))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(10), expr1))
    assertEquals(17.13,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(11), expr1), 1e-2)

    // age > 15 and class = 5
    val expr2 = relBuilder.push(logicalProject)
      .call(AND,
        relBuilder.call(GREATER_THAN, relBuilder.field(4), relBuilder.literal(15)),
        relBuilder.call(EQUALS, relBuilder.field(6), relBuilder.literal(5)))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(), expr2))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(0), expr2))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(1), expr2), 1e-2)
    assertEquals(17.13,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(2), expr2), 1e-2)
    assertEquals(6.99, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(3), expr2), 1e-2)
    assertEquals(21.90,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(4), expr2), 1e-2)
    assertEquals(21.90,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(5), expr2), 1e-2)
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(6), expr2), 1e-2)
    // TODO check result ??
    assertEquals(5.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(7), expr2), 1e-2)
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(8), expr2))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(9), expr2))
    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(10), expr2))
    assertEquals(17.13,
      mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(11), expr2), 1e-2)

    assertEquals(1.0, mq.getDistinctRowCount(logicalProject, ImmutableBitSet.of(0, 1), expr2))
  }

  @Test
  def testGetDistinctRowCountOnFilter(): Unit = {
    assertEquals(1.0, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(), null))
    assertEquals(25.0, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(0), null))
    assertEquals(24.68, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(1), null), 1e-2)
    assertEquals(16.46, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(2), null), 1e-2)
    assertEquals(6.95, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(3), null), 1e-2)
    assertEquals(21.99, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(4), null), 1e-2)
    assertEquals(2, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(5), null), 1e-2)
    assertNull(mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(6), null))
    assertEquals(25.0, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(0, 1), null))

    // class = 5
    relBuilder.push(logicalFilter)
    val expr1 = relBuilder.call(EQUALS, relBuilder.field(6), relBuilder.literal(5))
    assertEquals(1.0, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(), expr1))
    assertEquals(3.75, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(0), expr1), 1e-2)
    assertEquals(3.74, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(1), expr1), 1e-2)
    assertEquals(3.54, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(2), expr1), 1e-2)
    assertEquals(2.99, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(3), expr1), 1e-2)
    assertEquals(3.69, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(4), expr1), 1e-2)
    assertEquals(1.71, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(5), expr1), 1e-2)
    assertNull(mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(6), expr1))
    assertEquals(3.75, mq.getDistinctRowCount(logicalFilter, ImmutableBitSet.of(0, 1), expr1), 1e-2)
  }

  @Test
  def testGetDistinctRowCountOnCalc(): Unit = {
    relBuilder.push(studentLogicalScan)
    // id <= 10
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(10))
    val calc = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, List(expr1))

    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(), null))
    assertEquals(25.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(0), null))
    assertEquals(24.68, mq.getDistinctRowCount(calc, ImmutableBitSet.of(1), null), 1e-2)
    assertEquals(11.22,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(2), null), 1e-2)
    assertEquals(6.67,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(3), null), 1e-2)
    assertEquals(12.30,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(4), null), 1e-2)
    assertEquals(12.30,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(5), null), 1e-2)
    assertEquals(22.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(6), null), 1e-2)
    assertEquals(2.5, mq.getDistinctRowCount(calc, ImmutableBitSet.of(7), null), 1e-2)
    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(8), null))
    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(9), null))
    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(10), null))
    assertEquals(11.22,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(11), null), 1e-2)

    // class = 5
    relBuilder.push(calc)
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(11), relBuilder.literal(170))
    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(), expr2))
    assertEquals(12.5, mq.getDistinctRowCount(calc, ImmutableBitSet.of(0), expr2))
    assertEquals(12.43, mq.getDistinctRowCount(calc, ImmutableBitSet.of(1), expr2), 1e-2)
    assertEquals(11.22,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(2), expr2), 1e-2)
    assertEquals(6.67,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(3), expr2), 1e-2)
    assertEquals(12.30,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(4), expr2), 1e-2)
    assertEquals(12.30,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(5), expr2), 1e-2)
    assertEquals(11.79, mq.getDistinctRowCount(calc, ImmutableBitSet.of(6), expr2), 1e-2)
    assertEquals(2.5, mq.getDistinctRowCount(calc, ImmutableBitSet.of(7), expr2), 1e-2)
    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(8), expr2))
    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(9), expr2))
    assertEquals(1.0, mq.getDistinctRowCount(calc, ImmutableBitSet.of(10), expr2))
    assertEquals(11.22,
      mq.getDistinctRowCount(calc, ImmutableBitSet.of(11), expr2), 1e-2)
  }

  @Test
  def testGetDistinctRowCountOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand =>
        assertEquals(1.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(), null))
        assertEquals(50.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(0), null))
        assertEquals(48.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(1), null))
        assertEquals(20.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(2), null))
        assertEquals(7.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(3), null))
        assertEquals(35.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(4), null))
        assertEquals(2.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(5), null))
        assertEquals(null, mq.getDistinctRowCount(expand, ImmutableBitSet.of(6), null))
        assertEquals(3.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(7), null))

        // class = 5
        relBuilder.clear()
        relBuilder.push(expand)
        val expr = relBuilder.call(EQUALS, relBuilder.field(6), relBuilder.literal(5))
        assertEquals(1.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(), expr))
        assertEquals(7.5, mq.getDistinctRowCount(expand, ImmutableBitSet.of(0), expr))
        assertEquals(7.47, mq.getDistinctRowCount(expand, ImmutableBitSet.of(1), expr), 1e-2)
        assertEquals(6.67, mq.getDistinctRowCount(expand, ImmutableBitSet.of(2), expr), 1e-2)
        assertEquals(4.80, mq.getDistinctRowCount(expand, ImmutableBitSet.of(3), expr), 1e-2)
        assertEquals(7.25, mq.getDistinctRowCount(expand, ImmutableBitSet.of(4), expr), 1e-2)
        assertEquals(1.97, mq.getDistinctRowCount(expand, ImmutableBitSet.of(5), expr), 1e-2)
        assertEquals(null, mq.getDistinctRowCount(expand, ImmutableBitSet.of(6), expr))
        assertEquals(3.0, mq.getDistinctRowCount(expand, ImmutableBitSet.of(7), expr))
    }
  }

  @Test
  def testGetDistinctRowCountOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach { exchange =>
      assertEquals(1.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(), null))
      assertEquals(50.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(0), null))
      assertEquals(48.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(1), null))
      assertEquals(20.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(2), null))
      assertEquals(7.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(3), null))
      assertEquals(35.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(4), null))
      assertEquals(2.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(5), null))
      assertEquals(null, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(6), null))
      assertEquals(50.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(2, 3), null))
      assertEquals(40.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(2, 5), null))

      // age = 16
      val expr = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
      assertEquals(1.0, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(), expr))
      assertEquals(7.14, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(0), expr), 1e-2)
      assertEquals(7.12, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(1), expr), 1e-2)
      assertEquals(6.39, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(2), expr), 1e-2)
      assertEquals(4.67, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(3), expr), 1e-2)
      assertEquals(6.92, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(4), expr), 1e-2)
      assertEquals(1.96, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(5), expr), 1e-2)
      assertEquals(null, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(6), expr))
      assertEquals(7.14, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(2, 3), expr), 1e-2)
      assertEquals(7.01, mq.getDistinctRowCount(exchange, ImmutableBitSet.of(2, 5), expr), 1e-2)
    }
  }

  @Test
  def testGetDistinctRowCountOnRank(): Unit = {
    // no ndv on partition key
    Array(logicalRank, flinkLogicalRank, batchLocalRank, streamRank).foreach {
      rank =>
        assertEquals(1.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(), null))
        assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(0), null))
        assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(1), null))
        assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(2), null))
        assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(3), null))
        assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(4), null))
        assertEquals(2.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(5), null))
        assertEquals(null, mq.getDistinctRowCount(rank, ImmutableBitSet.of(6), null))
        rank match {
          case r: BatchPhysicalRank if !r.isGlobal => // local rank does not output rank func
          case _ =>
            assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(7), null))
        }
        assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(0, 1), null))
        assertEquals(5.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(3, 5), null))
    }

    // TODO FLINK-12282
    assertEquals(1.0, mq.getDistinctRowCount(batchGlobalRank, ImmutableBitSet.of(), null))
    assertEquals(1.0, mq.getDistinctRowCount(batchGlobalRank, ImmutableBitSet.of(0), null))
    assertEquals(1.0, mq.getDistinctRowCount(batchGlobalRank, ImmutableBitSet.of(1), null))
    assertEquals(null, mq.getDistinctRowCount(batchGlobalRank, ImmutableBitSet.of(6), null))
    assertEquals(1.0, mq.getDistinctRowCount(batchGlobalRank, ImmutableBitSet.of(7), null))

    // age has ndv
    Array(logicalRank2, flinkLogicalRank2, batchGlobalRank2, streamRank2).foreach {
      rank =>
        assertEquals(1.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(), null))
        assertEquals(21.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(0), null))
        assertEquals(21.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(1), null))
        assertEquals(20.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(2), null))
        assertEquals(7.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(3), null))
        assertEquals(21.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(4), null))
        assertEquals(2.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(5), null))
        assertEquals(null, mq.getDistinctRowCount(rank, ImmutableBitSet.of(6), null))
        assertEquals(3.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(7), null))
        assertEquals(21.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(0, 1), null))
        assertEquals(14.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(3, 5), null))
    }

    assertEquals(1.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(), null))
    assertEquals(35.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(0), null))
    assertEquals(35.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(1), null))
    assertEquals(20.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(2), null))
    assertEquals(7.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(3), null))
    assertEquals(35.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(4), null))
    assertEquals(2.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(5), null))
    assertEquals(null, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(6), null))
    assertEquals(35.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(0, 1), null))
    assertEquals(14.0, mq.getDistinctRowCount(batchLocalRank2, ImmutableBitSet.of(3, 5), null))

    // height > 170
    val expr = relBuilder.push(studentLogicalScan)
      .call(GREATER_THAN, relBuilder.field(4), relBuilder.literal(170.0))
    Array(logicalRank2, flinkLogicalRank2, batchGlobalRank2, streamRank2).foreach {
      rank =>
        assertEquals(1.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(), expr))
        assertEquals(9.46, mq.getDistinctRowCount(rank, ImmutableBitSet.of(0), expr), 1e-2)
        assertEquals(9.42, mq.getDistinctRowCount(rank, ImmutableBitSet.of(1), expr), 1e-2)
        assertEquals(8.16, mq.getDistinctRowCount(rank, ImmutableBitSet.of(2), expr), 1e-2)
        assertEquals(5.43, mq.getDistinctRowCount(rank, ImmutableBitSet.of(3), expr), 1e-2)
        assertEquals(9.06, mq.getDistinctRowCount(rank, ImmutableBitSet.of(4), expr), 1e-2)
        assertEquals(1.99, mq.getDistinctRowCount(rank, ImmutableBitSet.of(5), expr), 1e-2)
        assertEquals(null, mq.getDistinctRowCount(rank, ImmutableBitSet.of(6), expr))
        assertEquals(3.0, mq.getDistinctRowCount(rank, ImmutableBitSet.of(7), expr))
        assertEquals(9.46, mq.getDistinctRowCount(rank, ImmutableBitSet.of(0, 1), expr), 1e-2)
    }
  }

  @Test
  def testGetDistinctRowCountOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort).foreach {
      sort =>
        assertEquals(1.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(), null))
        assertEquals(50.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(0), null))
        assertEquals(48.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(1), null))
        assertEquals(20.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(2), null))
        assertEquals(7.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(3), null))
        assertEquals(35.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(4), null))
        assertEquals(2.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(5), null))
        assertEquals(null, mq.getDistinctRowCount(sort, ImmutableBitSet.of(6), null))
        assertEquals(50.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(2, 3), null))
        assertEquals(40.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(2, 5), null))
    }
    Array(logicalSortLimit, flinkLogicalSortLimit, batchGlobalSortLimit, streamSortLimit,
      logicalLimit, flinkLogicalLimit, batchLimit, streamLimit).foreach {
      sort =>
        assertEquals(1.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(), null))
        assertEquals(20.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(0), null))
        assertEquals(20.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(1), null))
        assertEquals(20.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(2), null))
        assertEquals(7.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(3), null))
        assertEquals(20.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(4), null))
        assertEquals(2.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(5), null))
        assertEquals(null, mq.getDistinctRowCount(sort, ImmutableBitSet.of(6), null))
        assertEquals(20.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(2, 3), null))
        assertEquals(20.0, mq.getDistinctRowCount(sort, ImmutableBitSet.of(2, 5), null))
    }
  }

  @Test
  def testGetDistinctRowCountOnAggreate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithoutLocal, batchGlobalAggWithLocal)
      .foreach { agg =>
        assertEquals(1.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(), null))
        assertEquals(7.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), null))
        assertEquals(2.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), null))
        assertEquals(2.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(2), null))
        assertEquals(3.5, mq.getDistinctRowCount(agg, ImmutableBitSet.of(3), null))
        assertEquals(3.5, mq.getDistinctRowCount(agg, ImmutableBitSet.of(4), null))
        assertEquals(10.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(5), null))
        assertEquals(7.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), null))
        assertEquals(7.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 5), null))
        assertEquals(20.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1, 5), null))
      }

    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithoutLocal, batchGlobalAggWithLocal)
      .foreach { agg =>
        // avg_score > 3.5
        relBuilder.clear()
        val expr1 = relBuilder.push(agg)
          .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(3.5))
        assertEquals(1.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(), expr1))
        assertEquals(4.33, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), expr1), 1e-2)
        assertEquals(1.93, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), expr1), 1e-2)
        assertEquals(1.93, mq.getDistinctRowCount(agg, ImmutableBitSet.of(2), expr1), 1e-2)
        assertEquals(2.99, mq.getDistinctRowCount(agg, ImmutableBitSet.of(3), expr1), 1e-2)
        assertEquals(2.99, mq.getDistinctRowCount(agg, ImmutableBitSet.of(4), expr1), 1e-2)
        assertEquals(4.33, mq.getDistinctRowCount(agg, ImmutableBitSet.of(5), expr1), 1e-2)
        assertEquals(4.33, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), expr1), 1e-2)
        assertEquals(4.33, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 5), expr1), 1e-2)
        assertEquals(4.33, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1, 5), expr1), 1e-2)

        // age = 15
        val expr2 = relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(15))
        assertEquals(4.67, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), expr2), 1e-2)
        assertEquals(1.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), expr2), 1e-2)
        assertEquals(10.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(5), expr2), 1e-2)
        assertEquals(4.67, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), expr2), 1e-2)
        assertEquals(10.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1, 5), expr2), 1e-2)

        // age > 15 or max_height > 170.0
        val expr3 = relBuilder.call(OR,
          relBuilder.call(GREATER_THAN, relBuilder.field(0), relBuilder.literal(15)),
          relBuilder.call(GREATER_THAN, relBuilder.field(3), relBuilder.literal(170.0)))
        assertEquals(1.75, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), expr3), 1e-2)
        assertEquals(1.53, mq.getDistinctRowCount(agg, ImmutableBitSet.of(3), expr3), 1e-2)
        assertEquals(1.75, mq.getDistinctRowCount(agg, ImmutableBitSet.of(5), expr3), 1e-2)
        assertEquals(1.75, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 3), expr3), 1e-2)
        assertEquals(1.75, mq.getDistinctRowCount(agg, ImmutableBitSet.of(3, 5), expr3), 1e-2)
      }
  }

  @Test
  def testGetDistinctRowCountOnWindowAgg(): Unit = {
    Array(logicalWindowAgg, flinkLogicalWindowAgg, batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach { agg =>
      assertEquals(30D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), null))
      assertEquals(5D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), null))
      assertEquals(50D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), null))
      assertEquals(50D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 2), null))
      assertEquals(null, mq.getDistinctRowCount(agg, ImmutableBitSet.of(3), null))
      assertEquals(null, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 3), null))
      assertEquals(null, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1, 3), null))
      assertEquals(null, mq.getDistinctRowCount(agg, ImmutableBitSet.of(2, 3), null))

      relBuilder.clear()
      // $1 > 10
      val pred = relBuilder
        .push(agg)
        .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10))
      assertEquals(
        FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 5.0D, 0.5D),
        mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), pred), 1e-6)
      assertEquals(25D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), pred))

      // b > 10 and count(c) > 1 and w$end = 100000
      val pred1 = relBuilder
        .push(agg)
        .and(
          relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)),
          relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(1)),
          relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(100000))
        )
      assertEquals(
        FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 5.0D, 0.075D),
        mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), pred1), 1e-6)
      assertEquals(25D * 0.15D * 1.0D,
        mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), pred1), 1e-2)
    }
    assertEquals(30D, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(0), null))
    assertEquals(5D, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(1), null))
    assertEquals(50D, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(0, 1), null))
    assertEquals(null, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(0, 2), null))
    assertEquals(10D, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(3), null))
    assertEquals(50D, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(0, 3), null))
    assertEquals(50.0, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(1, 3), null))
    assertEquals(null, mq.getDistinctRowCount(batchLocalWindowAgg, ImmutableBitSet.of(2, 3), null))

    Array(logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup).foreach { agg =>
      assertEquals(50D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), null))
      assertEquals(48D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), null))
      assertEquals(50D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), null))
      assertEquals(50D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 2), null))
      assertEquals(50D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1, 2), null))
      assertEquals(null, mq.getDistinctRowCount(agg, ImmutableBitSet.of(3), null))

      relBuilder.clear()
      // $1 > 10
      val pred = relBuilder
        .push(agg)
        .call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10))
      assertEquals(
        FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.8D),
        mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), pred), 1e-6)
      assertEquals(40D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), pred))

      // b > 10 and count(c) > 1 and w$end = 100000
      val pred1 = relBuilder
        .push(agg)
        .and(
          relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10)),
          relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(1)),
          relBuilder.call(EQUALS, relBuilder.field(4), relBuilder.literal(100000))
        )
      assertEquals(
        FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0D, 48.0D, 0.12D),
        mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), pred1), 1e-6)
      assertEquals(40D * 0.15D * 1.0D, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 1), pred1))
    }
    assertEquals(50D,
      mq.getDistinctRowCount(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0), null))
    assertNull(mq.getDistinctRowCount(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(1), null))
    assertNull(
      mq.getDistinctRowCount(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1), null))
    assertEquals(50D,
      mq.getDistinctRowCount(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 2), null))
    assertNull(
      mq.getDistinctRowCount(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(1, 2), null))
    assertEquals(10D,
      mq.getDistinctRowCount(batchLocalWindowAggWithAuxGroup, ImmutableBitSet.of(3), null))
  }

  @Test
  def testGetDistinctRowCountOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach { agg =>
      assertEquals(1.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(), null))
      assertEquals(50.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), null))
      assertEquals(48.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), null))
      assertEquals(20.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(2), null))
      assertEquals(7.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(3), null))
      (4 until agg.getRowType.getFieldCount).foreach { idx =>
        assertNull(mq.getDistinctRowCount(agg, ImmutableBitSet.of(idx), null))
      }
      assertNull(mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 5), null))

      // avg_score > 3.5
      relBuilder.clear()
      val expr1 = relBuilder.push(agg)
        .call(GREATER_THAN, relBuilder.field(8), relBuilder.literal(3.5))
      assertEquals(1.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(), expr1))
      assertEquals(25.0, mq.getDistinctRowCount(agg, ImmutableBitSet.of(0), expr1))
      assertEquals(24.68, mq.getDistinctRowCount(agg, ImmutableBitSet.of(1), expr1), 1e-2)
      assertNull(mq.getDistinctRowCount(agg, ImmutableBitSet.of(5), expr1))
      assertNull(mq.getDistinctRowCount(agg, ImmutableBitSet.of(0, 5), expr1))
    }
  }

  @Test
  def testGetDistinctRowCountOnJoin(): Unit = {
    assertEquals(1.0,
      mq.getDistinctRowCount(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(), null))
    assertEquals(1.0,
      mq.getDistinctRowCount(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of(), null))

    assertEquals(49.999938,
      mq.getDistinctRowCount(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(0), null), 1e-6)
    assertEquals(49.999998,
      mq.getDistinctRowCount(logicalInnerJoinOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-6)
    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalInnerJoinNotOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(5.0569644545E8,
      mq.getDistinctRowCount(logicalInnerJoinNotOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-2)

    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(5.0569644545E8,
      mq.getDistinctRowCount(logicalLeftJoinOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-2)
    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(5.0569644545E8,
      mq.getDistinctRowCount(logicalLeftJoinNotOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-2)

    assertEquals(49.999938,
      mq.getDistinctRowCount(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(0), null), 1e-6)
    assertEquals(49.999998,
      mq.getDistinctRowCount(logicalRightJoinOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-6)
    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalRightJoinNotOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(5.0569644545E8,
      mq.getDistinctRowCount(logicalRightJoinNotOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-2)

    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(5.0569644545E8,
      mq.getDistinctRowCount(logicalFullJoinOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-2)
    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalFullJoinNotOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(5.0569644545E8,
      mq.getDistinctRowCount(logicalFullJoinNotOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-2)

    assertEquals(50,
      mq.getDistinctRowCount(logicalSemiJoinOnUniqueKeys, ImmutableBitSet.of(0), null), 1e-2)
    assertEquals(50,
      mq.getDistinctRowCount(logicalSemiJoinOnUniqueKeys, ImmutableBitSet.of(1), null), 1e-2)
    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalSemiJoinNotOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(8.0E8,
      mq.getDistinctRowCount(logicalSemiJoinNotOnUniqueKeys, ImmutableBitSet.of(1), null))

    assertEquals(2.0E7,
      mq.getDistinctRowCount(logicalAntiJoinOnUniqueKeys, ImmutableBitSet.of(0), null))
    assertEquals(7.9999995E8,
      mq.getDistinctRowCount(logicalAntiJoinOnUniqueKeys, ImmutableBitSet.of(1), null))
    assertEquals(1.970438234E7,
      mq.getDistinctRowCount(logicalAntiJoinNotOnUniqueKeys, ImmutableBitSet.of(0), null), 1e-2)
    assertEquals(8.0E7,
      mq.getDistinctRowCount(logicalAntiJoinNotOnUniqueKeys, ImmutableBitSet.of(1), null))
  }

  @Test
  def testGetDistinctRowCountOnUnion(): Unit = {
    Array(logicalUnion, logicalUnionAll).foreach {
      union =>
        assertEquals(2.0, mq.getDistinctRowCount(union, ImmutableBitSet.of(), null))
        assertEquals(4.0E7, mq.getDistinctRowCount(union, ImmutableBitSet.of(0), null))
        assertEquals(8.00002556E8, mq.getDistinctRowCount(union, ImmutableBitSet.of(1), null))
        assertEquals(2263.0, mq.getDistinctRowCount(union, ImmutableBitSet.of(2), null))
        assertEquals(2.45748586E8, mq.getDistinctRowCount(union, ImmutableBitSet.of(3), null))
        assertEquals(null, mq.getDistinctRowCount(union, ImmutableBitSet.of(4), null))

        relBuilder.clear()
        val expr1 = relBuilder.push(union)
          .call(GREATER_THAN, relBuilder.field(4), relBuilder.literal(20))
        assertEquals(4.0E7, mq.getDistinctRowCount(union, ImmutableBitSet.of(0), expr1))
        assertEquals(646467202.46,
          mq.getDistinctRowCount(union, ImmutableBitSet.of(1), expr1), 1e-2)
        assertEquals(2263.0, mq.getDistinctRowCount(union, ImmutableBitSet.of(2), expr1))
        assertEquals(244612601.35,
          mq.getDistinctRowCount(union, ImmutableBitSet.of(3), expr1), 1e-2)
        assertEquals(null, mq.getDistinctRowCount(union, ImmutableBitSet.of(4), expr1))
    }
  }

  @Test
  def testGetDistinctRowCountOnDefault(): Unit = {
    assertEquals(null, mq.getDistinctRowCount(testRel, ImmutableBitSet.of(), null))
    assertEquals(null, mq.getDistinctRowCount(testRel, ImmutableBitSet.of(0), null))
  }

  @Test
  def testGetDistinctRowCountOnLargeDomainSize(): Unit = {
    relBuilder.clear()
    val rel = relBuilder
      .scan("MyTable1")
      .project(
        relBuilder.field(0),
        relBuilder.field(1),
        relBuilder.call(SUBSTRING, relBuilder.field(3), relBuilder.literal(10)))
      .build()
    assertEquals(
      7.999999964933156E8,
      mq.getDistinctRowCount(rel, ImmutableBitSet.of(0, 1, 2), null),
      1e-2)
  }
}
