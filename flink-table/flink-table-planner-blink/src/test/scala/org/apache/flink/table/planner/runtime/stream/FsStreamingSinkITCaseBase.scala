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

package org.apache.flink.table.planner.runtime.stream

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.util.FiniteTestSource
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestSinkUtil}
import org.apache.flink.types.Row

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists

import org.junit.Assert.assertEquals
import org.junit.rules.Timeout
import org.junit.{Before, Rule, Test}

import scala.collection.Seq

import scala.collection.JavaConversions._

/**
  * Streaming sink ITCase base, test checkpoint.
  */
abstract class FsStreamingSinkITCaseBase extends StreamingTestBase {

  @Rule
  def timeoutPerTest: Timeout = Timeout.seconds(20)

  protected var resultPath: String = _

  private val data = Seq(
    Row.of(Integer.valueOf(1), "a", "b", "c", "12345"),
    Row.of(Integer.valueOf(2), "p", "q", "r", "12345"),
    Row.of(Integer.valueOf(3), "x", "y", "z", "12345"))

  @Before
  override def before(): Unit = {
    super.before()
    resultPath = tempFolder.newFolder().toURI.toString

    env.setParallelism(1)
    env.enableCheckpointing(100)

    val stream = new DataStream(env.getJavaEnv.addSource(
      new FiniteTestSource[Row](data: _*),
      new RowTypeInfo(Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING)))

    tEnv.createTemporaryView("my_table", stream, $("a"), $("b"), $("c"), $("d"), $("e"))
  }

  def additionalProperties(): Array[String] = Array()

  @Test
  def testNonPart(): Unit = {
    test(false)
  }

  @Test
  def testPart(): Unit = {
    test(true)
  }

  private def test(partition: Boolean): Unit = {
    val ddl = s"""
                 |create table sink_table (
                 |  a int,
                 |  b string,
                 |  c string,
                 |  d string,
                 |  e string
                 |)
                 |${if (partition) "partitioned by (d, e)" else ""}
                 |with (
                 |  'connector' = 'filesystem',
                 |  'path' = '$resultPath',
                 |  ${additionalProperties().mkString(",\n")}
                 |)
       """.stripMargin
    tEnv.sqlUpdate(ddl)

    tEnv.insertInto("sink_table", tEnv.sqlQuery("select * from my_table"))
    tEnv.execute("insert")

    check(
      ddl,
      "select * from sink_table",
      data ++ data)
  }

  def check(ddl: String, sqlQuery: String, expectedResult: Seq[Row]): Unit = {
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironment.create(setting)
    tEnv.sqlUpdate(ddl)

    val result = Lists.newArrayList(tEnv.sqlQuery(sqlQuery).execute().collect())

    assertEquals(
      expectedResult.map(TestSinkUtil.rowToString(_)).sorted,
      result.map(TestSinkUtil.rowToString(_)).sorted)
  }
}
