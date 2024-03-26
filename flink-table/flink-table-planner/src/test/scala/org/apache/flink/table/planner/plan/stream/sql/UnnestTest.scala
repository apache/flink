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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.common.UnnestTestBase
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class UnnestTest extends UnnestTestBase(true) {

  override def getTableTestUtil: TableTestUtil = streamTestUtil()

  @Test
  def testUnnestWithInvalidLookupJoinHint(): Unit = {
    util.addTableSource[(Int, Array[Int])]("T2", 'a, 'b)
    verifyPlan(
      """
        |SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss','retry-strategy'='fixed_delay', 
        |         'fixed-delay'='155 ms', 'max-attempts'='10') */ T2.a
        |FROM T2 CROSS JOIN UNNEST(T2.b) AS D(c)
        |""".stripMargin)
  }

  @Test
  def testUnnestWithValuesStream(): Unit = {
    val src = util.tableEnv.sqlQuery("SELECT * FROM UNNEST(ARRAY[1,2,3])")
    val rows: java.util.List[Row] = CollectionUtil.iteratorToList(src.execute.collect)
    assertThat(rows.size()).isEqualTo(3)
    assertThat(rows.get(0).toString).isEqualTo("+I[1]")
    assertThat(rows.get(1).toString).isEqualTo("+I[2]")
    assertThat(rows.get(2).toString).isEqualTo("+I[3]")
  }

  @Test
  def testUnnestWithValuesStream2(): Unit = {
    val src =
      util.tableEnv.sqlQuery("SELECT * FROM (VALUES('a')) CROSS JOIN UNNEST(ARRAY[1, 2, 3])")
    val rows: java.util.List[Row] = CollectionUtil.iteratorToList(src.execute.collect)
    assertThat(rows.size()).isEqualTo(3)
    assertThat(rows.get(0).toString).isEqualTo("+I[a, 1]")
    assertThat(rows.get(1).toString).isEqualTo("+I[a, 2]")
    assertThat(rows.get(2).toString).isEqualTo("+I[a, 3]")
  }
}
