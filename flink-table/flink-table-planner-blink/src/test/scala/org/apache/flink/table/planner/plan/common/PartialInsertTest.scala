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

package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class PartialInsertTest(isBatch: Boolean) extends TableTestBase {

  private val util = if (isBatch) batchTestUtil() else streamTestUtil()
  util.addTableSource[(Int, String, String, String, Double)]("MyTable", 'a, 'b, 'c, 'd, 'e)
  util.tableEnv.executeSql(
    s"""
       |create table sink (
       |  `a` INT,
       |  `b` STRING,
       |  `c` STRING,
       |  `d` STRING,
       |  `e` DOUBLE,
       |  `f` BIGINT,
       |  `g` INT
       |) with (
       |  'connector' = 'values',
       |  'sink-insert-only' = 'false'
       |)
       |""".stripMargin)
  util.tableEnv.executeSql(
    s"""
       |create table partitioned_sink (
       |  `a` INT,
       |  `b` AS `a` + 1,
       |  `c` STRING,
       |  `d` STRING,
       |  `e` DOUBLE,
       |  `f` BIGINT,
       |  `g` INT
       |) PARTITIONED BY (`c`, `d`) with (
       |  'connector' = 'values',
       |  'sink-insert-only' = 'false'
       |)
       |""".stripMargin)

  @Test
  def testPartialInsertWithComplexReorder(): Unit = {
    util.verifyRelPlanInsert("INSERT INTO sink (b,e,a,g,f,c,d) " +
        "SELECT b,e,a,456,123,c,d FROM MyTable GROUP BY a,b,c,d,e")
  }

  @Test
  def testPartialInsertWithComplexReorderAndComputedColumn(): Unit = {
    util.verifyRelPlanInsert("INSERT INTO partitioned_sink (e,a,g,f,c,d) " +
        "SELECT e,a,456,123,c,d FROM MyTable GROUP BY a,b,c,d,e")
  }
}

object PartialInsertTest {
  @Parameterized.Parameters(name = "isBatch: {0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
