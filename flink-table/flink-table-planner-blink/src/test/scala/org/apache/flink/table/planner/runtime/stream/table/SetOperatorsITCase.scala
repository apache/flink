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

package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.utils.NonPojo
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class SetOperatorsITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def testUnion(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(smallTupleData3).toTable(tEnv, 'd, 'e, 'f)

    val unionDs = ds1.unionAll(ds2).select('c)

    val sink = new TestingAppendSink
    unionDs.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
        "Hi", "Hello", "Hello world", "Hi", "Hello", "Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnionWithFilter(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2.select('a, 'b, 'c)).filter('b < 2).select('c)

    val sink = new TestingAppendSink
    unionDs.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hi", "Hallo")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnionWithAnyType(): Unit = {
    val s1 = env.fromElements((1, new NonPojo), (2, new NonPojo)).toTable(tEnv, 'a, 'b)
    val s2 = env.fromElements((3, new NonPojo), (4, new NonPojo)).toTable(tEnv, 'a, 'b)

    val sink = new TestingAppendSink
    s1.unionAll(s2).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,{}", "2,{}", "3,{}", "4,{}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnionWithCompositeType(): Unit = {
    val s1 = env.fromElements((1, (1, "a")), (2, (2, "b")))
      .toTable(tEnv, 'a, 'b)
    val s2 = env.fromElements(((3, "c"), 3), ((4, "d"), 4))
      .toTable(tEnv, 'a, 'b)

    val sink = new TestingAppendSink
    s1.unionAll(s2.select('b, 'a)).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,1,a", "2,2,b", "3,3,c", "4,4,d")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testInUncorrelated(): Unit = {
    val dataA = Seq(
      (1, 1L, "Hello"),
      (2, 2L, "Hello"),
      (3, 3L, "Hello World"),
      (4, 4L, "Hello")
    )

    val dataB = Seq(
      (1, "hello"),
      (2, "co-hello"),
      (4, "hello")
    )

    val tableA = env.fromCollection(dataA).toTable(tEnv, 'a, 'b, 'c)

    val tableB = env.fromCollection(dataB).toTable(tEnv, 'x, 'y)

    val sink = new TestingRetractSink
    tableA.where('a.in(tableB.select('x))).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,1,Hello", "2,2,Hello", "4,4,Hello"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInUncorrelatedWithConditionAndAgg(): Unit = {
    val dataA = Seq(
      (1, 1L, "Hello"),
      (2, 2L, "Hello"),
      (3, 3L, "Hello World"),
      (4, 4L, "Hello")
    )

    val dataB = Seq(
      (1, "hello"),
      (1, "Hanoi"),
      (1, "Hanoi"),
      (2, "Hanoi-1"),
      (2, "Hanoi-1"),
      (-1, "Hanoi-1")
    )

    val tableA = env.fromCollection(dataA).toTable(tEnv,'a, 'b, 'c)

    val tableB = env.fromCollection(dataB).toTable(tEnv,'x, 'y)

    val sink = new TestingRetractSink

    tableA
      .where('a.in(tableB.where('y.like("%Hanoi%")).groupBy('y).select('x.sum)))
      .toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,2,Hello", "3,3,Hello World"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInWithMultiUncorrelatedCondition(): Unit = {
    val dataA = Seq(
      (1, 1L, "Hello"),
      (2, 2L, "Hello"),
      (3, 3L, "Hello World"),
      (4, 4L, "Hello")
    )

    val dataB = Seq(
      (1, "hello"),
      (2, "co-hello"),
      (4, "hello")
    )

    val dataC = Seq(
      (1L, "Joker"),
      (1L, "Sanity"),
      (2L, "Cool")
    )

    val tableA = env.fromCollection(dataA).toTable(tEnv,'a, 'b, 'c)

    val tableB = env.fromCollection(dataB).toTable(tEnv,'x, 'y)

    val tableC = env.fromCollection(dataC).toTable(tEnv,'w, 'z)

    val sink = new TestingRetractSink

    tableA
      .where('a.in(tableB.select('x)) && 'b.in(tableC.select('w)))
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()

    val expected = Seq(
      "1,1,Hello", "2,2,Hello"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
