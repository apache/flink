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

import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdColumnNullCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetColumnNullCountOnTableScan(): Unit = {
    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertEquals(0.0, mq.getColumnNullCount(scan, 0))
        assertEquals(0.0, mq.getColumnNullCount(scan, 1))
        assertEquals(6.0, mq.getColumnNullCount(scan, 2))
        assertEquals(0.0, mq.getColumnNullCount(scan, 3))
        assertNull(mq.getColumnNullCount(scan, 4))
        assertEquals(0.0, mq.getColumnNullCount(scan, 5))
        assertNull(mq.getColumnNullCount(scan, 6))
      }
  }

  @Test
  def testGetColumnIntervalOnValues(): Unit = {
    (0 until emptyValues.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnNullCount(emptyValues, idx))
    }
    (0 until logicalValues.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnNullCount(logicalValues, idx))
    }
  }

  @Test
  def testGetColumnIntervalOnSnapshot(): Unit = {
    (0 until flinkLogicalSnapshot.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnNullCount(flinkLogicalSnapshot, idx))
    }
  }

  @Test
  def testGetColumnNullCountOnProject(): Unit = {
    assertEquals(0.0, mq.getColumnNullCount(logicalProject, 0))
    assertEquals(0.0, mq.getColumnNullCount(logicalProject, 1))
    assertNull(mq.getColumnNullCount(logicalProject, 2))
    assertNull(mq.getColumnNullCount(logicalProject, 3))
    assertNull(mq.getColumnNullCount(logicalProject, 4))
    assertNull(mq.getColumnNullCount(logicalProject, 5))
    assertNull(mq.getColumnNullCount(logicalProject, 6))
    assertNull(mq.getColumnNullCount(logicalProject, 7))
    assertEquals(0.0, mq.getColumnNullCount(logicalProject, 8))
    assertEquals(0.0, mq.getColumnNullCount(logicalProject, 9))
    assertEquals(0.0, mq.getColumnNullCount(logicalProject, 10))
    assertNull(mq.getColumnNullCount(logicalProject, 11))
  }

  @Test
  def testGetColumnNullCountOnFilter(): Unit = {
    assertEquals(0.0, mq.getColumnNullCount(logicalFilter, 0))
    assertEquals(0.0, mq.getColumnNullCount(logicalFilter, 1))
    assertNull(mq.getColumnNullCount(logicalFilter, 2))
    assertEquals(0.0, mq.getColumnNullCount(logicalFilter, 3))
    assertNull(mq.getColumnNullCount(logicalFilter, 4))
    assertEquals(0.0, mq.getColumnNullCount(logicalFilter, 5))
    assertNull(mq.getColumnNullCount(logicalFilter, 6))

    relBuilder.push(studentLogicalScan)
    // height is not null
    val expr2 = relBuilder.isNotNull(relBuilder.field(4))
    val filter2 = relBuilder.filter(expr2).build
    assertEquals(0.0, mq.getColumnNullCount(filter2, 0))
    assertEquals(0.0, mq.getColumnNullCount(filter2, 1))
    assertNull(mq.getColumnNullCount(filter2, 2))
    assertEquals(0.0, mq.getColumnNullCount(filter2, 3))
    assertEquals(0.0, mq.getColumnNullCount(filter2, 4))
    assertEquals(0.0, mq.getColumnNullCount(filter2, 5))
    assertNull(mq.getColumnNullCount(filter2, 6))
  }

  @Test
  def testGetColumnNullCountOnCalc(): Unit = {
    // only project
    val calc1 = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, null)
    assertEquals(0.0, mq.getColumnNullCount(calc1, 0))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 1))
    assertNull(mq.getColumnNullCount(calc1, 2))
    assertNull(mq.getColumnNullCount(calc1, 3))
    assertNull(mq.getColumnNullCount(calc1, 4))
    assertNull(mq.getColumnNullCount(calc1, 5))
    assertNull(mq.getColumnNullCount(calc1, 6))
    assertNull(mq.getColumnNullCount(calc1, 7))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 8))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 9))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 10))
    assertNull(mq.getColumnNullCount(calc1, 11))

    // only filter
    relBuilder.push(studentLogicalScan)
    // id <= 2
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val calc2 = createLogicalCalc(
      studentLogicalScan, studentLogicalScan.getRowType, relBuilder.fields(), List(expr1))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 0))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 1))
    assertNull(mq.getColumnNullCount(calc2, 2))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 3))
    assertNull(mq.getColumnNullCount(calc2, 4))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 5))
    assertNull(mq.getColumnNullCount(calc2, 6))

    // height is not null
    val expr2 = relBuilder.isNotNull(relBuilder.field(4))
    val calc3 = createLogicalCalc(
      studentLogicalScan, studentLogicalScan.getRowType, relBuilder.fields(), List(expr2))
    assertEquals(0.0, mq.getColumnNullCount(calc3, 0))
    assertEquals(0.0, mq.getColumnNullCount(calc3, 1))
    assertNull(mq.getColumnNullCount(calc3, 2))
    assertEquals(0.0, mq.getColumnNullCount(calc3, 3))
    assertEquals(0.0, mq.getColumnNullCount(calc3, 4))
    assertEquals(0.0, mq.getColumnNullCount(calc3, 5))
    assertNull(mq.getColumnNullCount(calc3, 6))

    // project + filter
    // id <= 2
    val calc4 = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, List(expr1))
    assertEquals(0.0, mq.getColumnNullCount(calc4, 0))
    assertEquals(0.0, mq.getColumnNullCount(calc4, 1))
    assertNull(mq.getColumnNullCount(calc4, 2))
    assertNull(mq.getColumnNullCount(calc4, 3))
    assertNull(mq.getColumnNullCount(calc4, 4))
    assertNull(mq.getColumnNullCount(calc4, 5))
    assertNull(mq.getColumnNullCount(calc4, 6))
    assertNull(mq.getColumnNullCount(calc4, 7))
    assertEquals(0.0, mq.getColumnNullCount(calc4, 8))
    assertEquals(0.0, mq.getColumnNullCount(calc4, 9))
    assertEquals(0.0, mq.getColumnNullCount(calc4, 10))
    assertNull(mq.getColumnNullCount(calc4, 11))

    // height is not null
    val calc5 = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, List(expr2))
    assertEquals(0.0, mq.getColumnNullCount(calc5, 0))
    assertEquals(0.0, mq.getColumnNullCount(calc5, 1))
    assertNull(mq.getColumnNullCount(calc5, 2))
    assertNull(mq.getColumnNullCount(calc5, 3))
    assertNull(mq.getColumnNullCount(calc5, 4))
    assertNull(mq.getColumnNullCount(calc5, 5))
    assertEquals(0.0, mq.getColumnNullCount(calc5, 6))
    assertNull(mq.getColumnNullCount(calc5, 7))
    assertEquals(0.0, mq.getColumnNullCount(calc5, 8))
    assertEquals(0.0, mq.getColumnNullCount(calc5, 9))
    assertEquals(0.0, mq.getColumnNullCount(calc5, 10))
    assertNull(mq.getColumnNullCount(calc5, 11))
  }

  @Test
  def testGetColumnNullCountOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach {
      exchange =>
        assertEquals(0.0, mq.getColumnNullCount(exchange, 0))
        assertEquals(0.0, mq.getColumnNullCount(exchange, 1))
        assertEquals(6.0, mq.getColumnNullCount(exchange, 2))
        assertEquals(0.0, mq.getColumnNullCount(exchange, 3))
        assertNull(mq.getColumnNullCount(exchange, 4))
        assertEquals(0.0, mq.getColumnNullCount(exchange, 5))
        assertNull(mq.getColumnNullCount(exchange, 6))
    }
  }

  @Test
  def testGetColumnNullCountOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalLimit, flinkLogicalLimit, batchLimit, batchLocalLimit, batchGlobalLimit, streamLimit,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, batchLocalSortLimit,
      batchGlobalSortLimit, streamSortLimit).foreach {
      sort =>
        assertEquals(0.0, mq.getColumnNullCount(sort, 0))
        assertEquals(0.0, mq.getColumnNullCount(sort, 1))
        assertEquals(6.0, mq.getColumnNullCount(sort, 2))
        assertEquals(0.0, mq.getColumnNullCount(sort, 3))
        assertNull(mq.getColumnNullCount(sort, 4))
        assertEquals(0.0, mq.getColumnNullCount(sort, 5))
        assertNull(mq.getColumnNullCount(sort, 6))
    }
  }

  @Test
  def testGetColumnNullCountOnJoin(): Unit = {
    val left = relBuilder.scan("student").build()
    // right is age <= 15 and score > 4.0
    val right = relBuilder.push(left).filter(
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(3), relBuilder.literal(15)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(4.0))).build()
    // join condition is left.id=right.id
    // inner join
    val innerJoin = relBuilder.push(left).push(right).join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    (0 until innerJoin.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnNullCount(innerJoin, idx))
    }

    // left join
    val leftJoin = relBuilder.push(left).push(right).join(
      JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 0))
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 1))
    assertEquals(6.0, mq.getColumnNullCount(leftJoin, 2))
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 3))
    assertNull(mq.getColumnNullCount(leftJoin, 4))
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 5))
    assertNull(mq.getColumnNullCount(leftJoin, 6))

    val expectedNullCnt1 = mq.getRowCount(left) - mq.getRowCount(innerJoin)
    assertEquals(expectedNullCnt1, mq.getColumnNullCount(leftJoin, 7))
    assertEquals(expectedNullCnt1, mq.getColumnNullCount(leftJoin, 8))
    assertNull(mq.getColumnNullCount(leftJoin, 9))
    assertEquals(expectedNullCnt1, mq.getColumnNullCount(leftJoin, 10))
    assertNull(mq.getColumnNullCount(leftJoin, 11))
    assertEquals(expectedNullCnt1, mq.getColumnNullCount(leftJoin, 12))
    assertNull(mq.getColumnNullCount(leftJoin, 13))

    // right join
    val rightJoin = relBuilder.push(left).push(right).join(
      JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    val expectedNullCnt2 = mq.getRowCount(right) - mq.getRowCount(innerJoin)
    assertEquals(expectedNullCnt2, mq.getColumnNullCount(rightJoin, 0))
    assertEquals(expectedNullCnt2, mq.getColumnNullCount(rightJoin, 1))
    assertEquals(expectedNullCnt2 + 6.0, mq.getColumnNullCount(rightJoin, 2))
    assertEquals(expectedNullCnt2, mq.getColumnNullCount(rightJoin, 3))
    assertNull(mq.getColumnNullCount(rightJoin, 4))
    assertEquals(expectedNullCnt2, mq.getColumnNullCount(rightJoin, 5))
    assertNull(mq.getColumnNullCount(rightJoin, 6))
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 7))
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 8))
    assertNull(mq.getColumnNullCount(rightJoin, 9))
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 10))
    assertNull(mq.getColumnNullCount(rightJoin, 11))
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 12))
    assertNull(mq.getColumnNullCount(rightJoin, 13))

    // full join
    val fullJoin = relBuilder.push(left).push(right).join(
      JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    (0 until fullJoin.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnNullCount(fullJoin, idx))
    }

    // semi/anti join
    Array(logicalSemiJoinWithEquiAndNonEquiCond, logicalAntiJoinWithoutEquiCond).foreach { join =>
      (0 until join.getRowType.getFieldCount).foreach { idx =>
        assertNull(mq.getColumnNullCount(fullJoin, idx))
      }
    }
  }

  @Test
  def testGetColumnNullCountOnDefault(): Unit = {
    (0 until testRel.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnNullCount(testRel, idx))
    }
  }
}
