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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.CommonTestData.NonPojo
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class SetOperatorsITCase extends AbstractTestBase {

  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f)

    val unionDs = ds1.unionAll(ds2).select('c)

    val results = unionDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
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

    val results = unionDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("Hi", "Hallo")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnionWithAnyType(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val s1 = env.fromElements((1, new NonPojo), (2, new NonPojo)).toTable(tEnv, 'a, 'b)
    val s2 = env.fromElements((3, new NonPojo), (4, new NonPojo)).toTable(tEnv, 'a, 'b)

    val result = s1.unionAll(s2).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("1,{}", "2,{}", "3,{}", "4,{}")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnionWithCompositeType(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val s1 = env.fromElements((1, (1, "a")), (2, (2, "b")))
      .toTable(tEnv, 'a, 'b)
    val s2 = env.fromElements(((3, "c"), 3), ((4, "d"), 4))
      .toTable(tEnv, 'a, 'b)

    val result = s1.unionAll(s2.select('b, 'a)).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("1,(1,a)", "2,(2,b)", "3,(3,c)", "4,(4,d)")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
