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
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{EQUALS, LESS_THAN_OR_EQUAL}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdColumnOriginNullCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetColumnOriginNullCountOnTableScan(): Unit = {
    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertEquals(0.0, mq.getColumnOriginNullCount(scan, 0))
        assertEquals(0.0, mq.getColumnOriginNullCount(scan, 1))
        assertEquals(6.0, mq.getColumnOriginNullCount(scan, 2))
        assertEquals(0.0, mq.getColumnOriginNullCount(scan, 3))
        assertNull(mq.getColumnOriginNullCount(scan, 4))
        assertEquals(0.0, mq.getColumnOriginNullCount(scan, 5))
        assertNull(mq.getColumnOriginNullCount(scan, 6))
      }

    val ts = relBuilder.scan("MyTable3").build()
    assertEquals(1.0, mq.getColumnOriginNullCount(ts, 0))
    assertEquals(0.0, mq.getColumnOriginNullCount(ts, 1))
  }

  @Test
  def testGetColumnOriginNullCountOnSnapshot(): Unit = {
    (0 until flinkLogicalSnapshot.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnOriginNullCount(flinkLogicalSnapshot, idx))
    }
  }

  @Test
  def testGetColumnOriginNullCountOnProject(): Unit = {
    assertEquals(0.0, mq.getColumnOriginNullCount(logicalProject, 0))
    assertEquals(0.0, mq.getColumnOriginNullCount(logicalProject, 1))
    assertNull(mq.getColumnOriginNullCount(logicalProject, 2))
    assertNull(mq.getColumnOriginNullCount(logicalProject, 3))
    assertNull(mq.getColumnOriginNullCount(logicalProject, 4))
    assertNull(mq.getColumnOriginNullCount(logicalProject, 5))
    assertNull(mq.getColumnOriginNullCount(logicalProject, 6))
    assertNull(mq.getColumnOriginNullCount(logicalProject, 7))
    assertEquals(0.0, mq.getColumnOriginNullCount(logicalProject, 8))
    assertEquals(0.0, mq.getColumnOriginNullCount(logicalProject, 9))
    assertEquals(0.0, mq.getColumnOriginNullCount(logicalProject, 10))
    assertNull(mq.getColumnOriginNullCount(logicalProject, 11))

    val ts = relBuilder.scan("MyTable3").build()
    relBuilder.push(ts)
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.literal(null))
    val project = relBuilder.project(projects).build()

    assertEquals(null, mq.getColumnOriginNullCount(project, 0))
    assertEquals(1.0, mq.getColumnOriginNullCount(project, 1))
    assertEquals(0.0, mq.getColumnOriginNullCount(project, 2))
    assertEquals(0.0, mq.getColumnOriginNullCount(project, 3))
    assertEquals(1.0, mq.getColumnOriginNullCount(project, 4))
  }

  @Test
  def testGetColumnOriginNullCountOnCalc(): Unit = {
    // only filter
    relBuilder.push(studentLogicalScan)
    // id <= 2
    val expr = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val calc1 = createLogicalCalc(
      studentLogicalScan, studentLogicalScan.getRowType, relBuilder.fields(), List(expr))
    (0 until calc1.getRowType.getFieldCount).foreach { idx =>
      assertNull(mq.getColumnOriginNullCount(calc1, idx))
    }

    val ts = relBuilder.scan("MyTable3").build()
    relBuilder.push(ts)
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.literal(null))
    val outputRowType = relBuilder.project(projects).build().getRowType
    val calc2 = createLogicalCalc(ts, outputRowType, projects, List())
    assertEquals(null, mq.getColumnOriginNullCount(calc2, 0))
    assertEquals(1.0, mq.getColumnOriginNullCount(calc2, 1))
    assertEquals(0.0, mq.getColumnOriginNullCount(calc2, 2))
    assertEquals(0.0, mq.getColumnOriginNullCount(calc2, 3))
    assertEquals(1.0, mq.getColumnOriginNullCount(calc2, 4))
  }

  @Test
  def testGetColumnOriginNullCountOnJoin(): Unit = {
    val innerJoin1 = relBuilder.scan("MyTable3").project(relBuilder.fields().subList(0, 2))
      .scan("MyTable4")
      .join(JoinRelType.INNER,
        relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
      .build
    assertEquals(1.0, mq.getColumnOriginNullCount(innerJoin1, 0))
    assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin1, 1))
    assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin1, 2))
    assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin1, 3))

    val innerJoin2 = relBuilder.scan("MyTable3").project(relBuilder.fields().subList(0, 2))
      .scan("MyTable4")
      .join(JoinRelType.INNER,
        relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
      .build
    assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin2, 0))
    assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin2, 1))
    assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin2, 2))
    assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin2, 3))

    Array(logicalLeftJoinOnUniqueKeys, logicalRightJoinNotOnUniqueKeys,
      logicalFullJoinWithEquiAndNonEquiCond, logicalSemiJoinNotOnUniqueKeys,
      logicalSemiJoinWithEquiAndNonEquiCond).foreach { join =>
      (0 until join.getRowType.getFieldCount).foreach { idx =>
        assertNull(mq.getColumnOriginNullCount(join, idx))
      }
    }
  }
}
