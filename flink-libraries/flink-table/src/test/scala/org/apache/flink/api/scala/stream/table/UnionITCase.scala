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

package org.apache.flink.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.{StreamTestData, StreamITCase}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, TableEnvironment, TableException}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class UnionITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2).select('c)

    val results = unionDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
        "Hi", "Hello", "Hello world", "Hi", "Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnionWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2.select('a, 'b, 'c)).filter('b < 2).select('c)

    val results = unionDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hi", "Hallo")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnionFieldsNameNotOverlap1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2)

    val results = unionDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    assertEquals(true, StreamITCase.testResults.isEmpty)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnionFieldsNameNotOverlap2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2)

    val results = unionDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    println(StreamITCase.testResults)
    assertEquals(true, StreamITCase.testResults.isEmpty)
  }

  @Test(expected = classOf[TableException])
  def testUnionTablesFromDifferentEnvs(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env)
    val tEnv2 = TableEnvironment.getTableEnvironment(env)

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.unionAll(ds2)
  }

}
