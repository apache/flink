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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamTestData, StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.runtime.utils.CommonTestData.NonPojo
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class SetOperatorsITCase extends StreamingTestBase {

  @Test
  def testUnion(): Unit = {
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f)

    val unionDs = ds1.unionAll(ds2).select('c)

    val results = unionDs.toAppendStream[Row]
    val sink = new TestingAppendSink

    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
        "Hi", "Hello", "Hello world", "Hi", "Hello", "Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnionWithFilter(): Unit = {
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2.select('a, 'b, 'c)).filter('b < 2).select('c)

    val results = unionDs.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hi", "Hallo")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnionWithAnyType(): Unit = {
    val s1 = env.fromElements((1, new NonPojo), (2, new NonPojo)).toTable(tEnv, 'a, 'b)
    val s2 = env.fromElements((3, new NonPojo), (4, new NonPojo)).toTable(tEnv, 'a, 'b)

    val result = s1.unionAll(s2).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,{}", "2,{}", "3,{}", "4,{}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  class NODE {
    val x = new java.util.HashMap[String, String]()
  }
}
