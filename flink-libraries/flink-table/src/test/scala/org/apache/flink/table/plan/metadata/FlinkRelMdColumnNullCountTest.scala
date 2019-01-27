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

import org.apache.calcite.rel.RelDistributions
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalExchange
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdColumnNullCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetColumnNullCountOnTableScan(): Unit = {
    val ts = relBuilder.scan("t1").build()
    assertEquals(1.0, mq.getColumnNullCount(ts, 0))
    assertEquals(0.0, mq.getColumnNullCount(ts, 1))
  }

  @Test
  def testGetColumnNullCountOnLogicalDimensionTableScan(): Unit = {
    assertEquals(null, mq.getColumnNullCount(temporalTableSourceScanWithCalc, 0))
    assertEquals(null, mq.getColumnNullCount(temporalTableSourceScanWithCalc, 1))
    assertEquals(null, mq.getColumnNullCount(temporalTableSourceScanWithCalc, 2))
  }

  @Test
  def testGetColumnNullCountOnExchange(): Unit = {
    val ts = relBuilder.scan("t1").build()
    val exchange = LogicalExchange.create(ts, RelDistributions.SINGLETON)
    assertEquals(1.0, mq.getColumnNullCount(exchange, 0))
    assertEquals(0.0, mq.getColumnNullCount(exchange, 1))
  }

  @Test
  def testGetColumnNullCountOnSort(): Unit = {
    // select * from t1 order by score desc, id
    val sort = relBuilder.scan("t1").sort(
      relBuilder.desc(relBuilder.field("score")),
      relBuilder.field("id")).build
    assertEquals(1.0, mq.getColumnNullCount(sort, 0))
    assertEquals(0.0, mq.getColumnNullCount(sort, 1))
  }

  @Test
  def testGetColumnNullCountOnProject(): Unit = {
    assertNull(mq.getColumnNullCount(project, 0))
    assertEquals(1.0, mq.getColumnNullCount(project, 1))
    assertEquals(0.0, mq.getColumnNullCount(project, 2))
    assertEquals(0.0, mq.getColumnNullCount(project, 3))
    assertEquals(0.0, mq.getColumnNullCount(project, 4))
    assertEquals(0.0, mq.getColumnNullCount(project, 5))
  }

  @Test
  def testGetColumnNullCountOnFilter(): Unit = {
    val ts = relBuilder.scan("t1").build()
    relBuilder.push(ts)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    // filter: $0 <= 2
    val filter1 = relBuilder.filter(expr1).build
    assertNull(mq.getColumnNullCount(filter1, 0))
    assertEquals(0.0, mq.getColumnNullCount(filter1, 1))

    // filter: $0 is not null
    relBuilder.push(ts)
    val expr2 = relBuilder.isNotNull(relBuilder.field(0))
    val filter2 = relBuilder.filter(expr2).build
    assertEquals(0.0, mq.getColumnNullCount(filter2, 0))
    assertEquals(0.0, mq.getColumnNullCount(filter2, 1))
  }

  @Test
  def testGetColumnNullCountOnCalc(): Unit = {
    val ts = relBuilder.scan("t1").build()
    relBuilder.push(ts)
    // projects: $0==1, $0, $1, true, 2.1, 2
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.getRexBuilder.makeLiteral(
        2.1D, typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, false), true),
      relBuilder.getRexBuilder.makeLiteral(
        2L, typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, false), true))
    val outputRowType = relBuilder.project(projects).build().getRowType
    relBuilder.push(ts)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    // calc => projects + filter: $0 <= 2
    val calc1 = buildCalc(ts, outputRowType, projects, List(expr1))
    assertNull(mq.getColumnNullCount(calc1, 0))
    assertNull(mq.getColumnNullCount(calc1, 1))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 2))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 3))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 4))
    assertEquals(0.0, mq.getColumnNullCount(calc1, 5))

    val expr2 = relBuilder.isNotNull(relBuilder.field(0))
    // calc => projects + filter: $0 is not null
    val calc2 = buildCalc(ts, outputRowType, projects, List(expr2))
    assertNull(mq.getColumnNullCount(calc2, 0))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 1))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 2))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 3))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 4))
    assertEquals(0.0, mq.getColumnNullCount(calc2, 5))
  }

  @Test
  def testGetColumnNullCountOnJoin(): Unit = {
    val left = relBuilder.scan("student").build()
    // right is $0 <= 2 and $1 < 1.1
    val right = relBuilder.push(left).filter(
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2)),
      relBuilder.call(LESS_THAN, relBuilder.field(1), relBuilder.literal(1.1D))).build()
    // join condition is left.$0=right.$0
    val innerJoin = relBuilder.push(left).push(right).join(
      JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertNull(mq.getColumnNullCount(innerJoin, 0))
    val leftJoin = relBuilder.push(left).push(right).join(
      JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 0))
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 1))
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 2))
    assertEquals(0.0, mq.getColumnNullCount(leftJoin, 3))
    assertEquals(
      mq.getRowCount(left) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(leftJoin, 4))
    assertEquals(
      mq.getRowCount(left) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(leftJoin, 5))
    assertEquals(
      mq.getRowCount(left) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(leftJoin, 6))
    assertEquals(
      mq.getRowCount(left) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(leftJoin, 7))
    val rightJoin = relBuilder.push(left).push(right).join(
      JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 4))
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 5))
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 6))
    assertEquals(0.0, mq.getColumnNullCount(rightJoin, 7))
    assertEquals(
      mq.getRowCount(right) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(rightJoin, 0))
    assertEquals(
      mq.getRowCount(right) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(rightJoin, 1))
    assertEquals(
      mq.getRowCount(right) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(rightJoin, 2))
    assertEquals(
      mq.getRowCount(right) - mq.getRowCount(innerJoin),
      mq.getColumnNullCount(rightJoin, 3))
    val fullJoin = relBuilder.push(left).push(right).join(
      JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertNull(mq.getColumnNullCount(fullJoin, 0))
    assertNull(mq.getColumnNullCount(fullJoin, 1))
    assertNull(mq.getColumnNullCount(fullJoin, 2))
    assertNull(mq.getColumnNullCount(fullJoin, 3))
  }
}
