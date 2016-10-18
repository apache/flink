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
package org.apache.flink.api.scala.stream

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.StreamITCase
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.utils.{TableFunc0, TableFunc1}
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class UserDefinedTableFunctionITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testSQLCrossApply(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    tEnv.registerFunction("split", new TableFunc0)

    val sqlQuery = "SELECT MyTable.c, t.n, t.a FROM MyTable, LATERAL TABLE(split(c)) AS t(n,a)"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Jack#22,Jack,22", "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSQLOuterApply(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    tEnv.registerFunction("split", new TableFunc0)

    val sqlQuery = "SELECT MyTable.c, t.n, t.a FROM MyTable " +
      "LEFT JOIN LATERAL TABLE(split(c)) AS t(n,a) ON TRUE"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "nosharp,null,null", "Jack#22,Jack,22",
      "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableAPICrossApply(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .crossApply(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .toDataStream[Row]

    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableAPIOuterApply(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .outerApply(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .toDataStream[Row]

    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "nosharp,null,null", "Jack#22,Jack,22",
      "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableAPIWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .crossApply(func0('c) as('name, 'age))
      .select('c, 'name, 'age)
      .filter('age > 20)
      .toDataStream[Row]

    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableAPIWithScalarFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    val func1 = new TableFunc1

    val result = t
      .crossApply(func1('c.substring(2)) as 's)
      .select('c, 's)
      .toDataStream[Row]

    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,ack", "Jack#22,22", "John#19,ohn",
                                       "John#19,19", "Anna#44,nna", "Anna#44,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  private def getSmall3TupleDataStream(
    env: StreamExecutionEnvironment)
  : DataStream[(Int, Long, String)] = {

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    env.fromCollection(data)
  }

}
