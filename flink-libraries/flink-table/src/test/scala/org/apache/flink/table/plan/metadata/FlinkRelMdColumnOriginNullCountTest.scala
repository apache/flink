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

import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdColumnOriginNullCountTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetColumnOriginNullCountOnTableScan(): Unit = {
    val ts = relBuilder.scan("t1").build()
    assertEquals(1.0, mq.getColumnOriginNullCount(ts, 0))
    assertEquals(0.0, mq.getColumnOriginNullCount(ts, 1))
  }

  @Test
  def testGetColumnOriginNullCountOnLogicalSnapshot(): Unit = {
    assertEquals(null, mq.getColumnOriginNullCount(temporalTableSourceScanWithCalc, 0))
    assertEquals(null, mq.getColumnOriginNullCount(temporalTableSourceScanWithCalc, 1))
  }

  @Test
  def testGetColumnOriginNullCountOnProject(): Unit = {
    val project = {
      val ts = relBuilder.scan("t1").build()
      relBuilder.push(ts)
      val projects = List(
        relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
        relBuilder.field(0),
        relBuilder.field(1),
        relBuilder.literal(true),
        relBuilder.literal(null))
      relBuilder.project(projects).build()
    }

    assertEquals(null, mq.getColumnOriginNullCount(project, 0))
    assertEquals(1.0, mq.getColumnOriginNullCount(project, 1))
    assertEquals(0.0, mq.getColumnOriginNullCount(project, 2))
    assertEquals(0.0, mq.getColumnOriginNullCount(project, 3))
    assertEquals(1.0, mq.getColumnOriginNullCount(project, 4))
  }

  @Test
  def testGetColumnOriginNullCountOnCalc(): Unit = {
    val calc = {
      val ts = relBuilder.scan("t1").build()
      relBuilder.push(ts)
      val projects = List(
        relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
        relBuilder.field(0),
        relBuilder.field(1),
        relBuilder.literal(true),
        relBuilder.literal(null))
      val outputRowType = relBuilder.project(projects).build().getRowType
      buildCalc(ts, outputRowType, projects, List())
    }

    assertEquals(null, mq.getColumnOriginNullCount(calc, 0))
    assertEquals(1.0, mq.getColumnOriginNullCount(calc, 1))
    assertEquals(0.0, mq.getColumnOriginNullCount(calc, 2))
    assertEquals(0.0, mq.getColumnOriginNullCount(calc, 3))
    assertEquals(1.0, mq.getColumnOriginNullCount(calc, 4))
  }

  @Test
  def testGetColumnOriginNullCountOnJoin(): Unit = {
    {
      val innerJoin = relBuilder.scan("t2").scan("t1")
          .join(JoinRelType.INNER,
            relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
          .build

      assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin, 0))
      assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin, 1))
      assertEquals(1.0, mq.getColumnOriginNullCount(innerJoin, 2))
      assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin, 3))
    }
    {
      val innerJoin = relBuilder.scan("t2").scan("t1")
          .join(JoinRelType.INNER,
            relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
          .build

      assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin, 0))
      assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin, 1))
      assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin, 2))
      assertEquals(0.0, mq.getColumnOriginNullCount(innerJoin, 3))
    }
  }
}
