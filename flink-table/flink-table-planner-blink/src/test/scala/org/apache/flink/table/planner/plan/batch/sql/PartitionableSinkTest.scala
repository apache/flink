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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class PartitionableSinkTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Long, Long, Long)]("MyTable", 'a, 'b, 'c)
  createTable("sink", shuffleBy = false)

  private def createTable(name: String, shuffleBy: Boolean): Unit = {
    util.tableEnv.executeSql(
      s"""
         |create table $name (
         |  a bigint,
         |  b bigint,
         |  c bigint
         |) partitioned by (b, c) with (
         |  'connector' = 'filesystem',
         |  'path' = '/non',
         |  ${if (shuffleBy) "'sink.shuffle-by-partition.enable'='true'," else ""}
         |  'format' = 'testcsv'
         |)
         |""".stripMargin)
  }

  @Test
  def testStatic(): Unit = {
    util.verifyPlanInsert("INSERT INTO sink PARTITION (b=1, c=1) SELECT a FROM MyTable")
  }

  @Test
  def testDynamic(): Unit = {
    util.verifyPlanInsert("INSERT INTO sink SELECT a, b, c FROM MyTable")
  }

  @Test
  def testDynamicShuffleBy(): Unit = {
    createTable("sinkShuffleBy", shuffleBy = true)
    util.verifyPlanInsert("INSERT INTO sinkShuffleBy SELECT a, b, c FROM MyTable")
  }

  @Test
  def testPartial(): Unit = {
    util.verifyPlanInsert("INSERT INTO sink PARTITION (b=1) SELECT a, c FROM MyTable")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongStatic(): Unit = {
    util.verifyPlanInsert("INSERT INTO sink PARTITION (a=1) SELECT b, c FROM MyTable")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongFields(): Unit = {
    util.verifyPlanInsert("INSERT INTO sink PARTITION (b=1) SELECT a, b, c FROM MyTable")
  }

  @Test
  def testStaticWithValues(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "INSERT INTO <table> PARTITION statement only support SELECT clause for now," +
          " 'VALUES ROW(5)' is not supported yet")
    util.verifyPlanInsert("INSERT INTO sink PARTITION (b=1, c=1) VALUES (5)")
  }
}
