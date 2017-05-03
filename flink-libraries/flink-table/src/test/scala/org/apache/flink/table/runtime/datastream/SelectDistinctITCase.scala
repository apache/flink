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
package org.apache.flink.table.runtime.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class SelectDistinctITCase extends StreamingWithStateTestBase {
  val data = List(
    (1L, 1, "Hello"),
    (2L, 2, "Hello"),
    (3L, 3, "Hi"),
    (4L, 2, "Hello"),
    (5L, 3, "Hi"),
    (6L, 1, "Hello"),
    (7L, 7, "Hello World"),
    (8L, 7, "Hello World"),
    (20L, 7, "Hello World"))

  @Test
  def testSelectDistinctWithSingleField(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("MyTable", t1)

    val sqlQuery = "SELECT distinct c FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello",
      "Hello World",
      "Hi")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSelectDistinctWithMultiField(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("MyTable", t1)

    val sqlQuery = "SELECT distinct b, c FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,Hello",
      "2,Hello",
      "7,Hello World",
      "3,Hi")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
