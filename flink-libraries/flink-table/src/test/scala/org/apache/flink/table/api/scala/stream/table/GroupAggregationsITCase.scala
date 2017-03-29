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

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
  * Tests of groupby (without window & early-firing) aggregations
  */
class GroupAggregationsITCase extends StreamingMultipleProgramsTestBase {

  @Test(expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. '_foo is not a valid field
      .groupBy('_foo)
      .select('a.avg)
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('a, 'b)
      // must fail. 'c is not a grouping key or aggregation
      .select('c)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1", "2,2", "2,5", "3,4", "3,9", "3,15", "4,7", "4,15",
      "4,24", "4,34", "5,11", "5,23", "5,36", "5,50", "5,65", "6,16", "6,33", "6,51", "6,70",
      "6,90", "6,111")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testForwardGroupingKeyIfNotUsed(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.sum)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1", "2", "5", "4", "9", "15", "7", "15", "24", "34",
      "11", "23", "36", "50", "65", "16", "33", "51", "70", "90", "111")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testDoubleGroupAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.sum as 'd, 'b)
      .groupBy('b, 'd)
      .select('b)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1",
      "2", "2",
      "3", "3", "3",
      "4", "4", "4", "4",
      "5", "5", "5", "5", "5",
      "6", "6", "6", "6", "6", "6")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupedAggregateWithLongKeys(): Unit = {
    // When the key is short, the normalized key is sufficient.
    // This test uses a relative long keys to force serialized comparison.
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.clear

    val ds = env.fromElements(
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2))
      .toTable(tEnv, 'a, 'b, 'c)
      .groupBy('a, 'b)
      .select('c.sum)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("2", "4", "6", "8", "10", "2", "4", "6", "8")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregateWithConstant1(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.clear
    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "4,1", "4,2", "4,2", "4,3", "4,3", "4,3", "4,4", "4,4",
      "4,4", "4,4", "4,5", "4,5", "4,5", "4,5", "4,5", "4,6", "4,6", "4,6", "4,6", "4,6", "4,6")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregateWithConstant2(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select('b, 4 as 'four, 'a)
      .groupBy('b, 'four)
      .select('four, 'a.sum)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "4,1", "4,2", "4,5", "4,4", "4,9", "4,15", "4,7",
      "4,15", "4,24", "4,34", "4,11", "4,23", "4,36", "4,50", "4,65", "4,16", "4,33",
      "4,51", "4,70", "4,90", "4,111")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregateWithExpression(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "0,1,1,1", "1,2,2,1", "2,1,2,1", "3,2,3,1", "1,2,2,2",
      "5,3,3,1", "3,2,3,2", "7,1,4,1", "2,1,3,2", "3,2,3,3", "7,1,4,2", "5,3,4,2", "12,3,5,1",
      "1,2,3,3", "14,2,5,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregateWithExpressionInSelect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a as 'a, 'b % 3 as 'f, 'c as 'c, 'd as 'd, 'e as 'e)
      .groupBy('e, 'f)
      .select('c.min, 'e, 'a.avg, 'd.count, 'f)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "0,1,1,1,1", "1,2,2,1,2", "2,1,2,1,0", "3,2,3,1,1", "1,2,2,2,2", "5,3,3,1,0", "3,2,3,2,1",
      "7,1,4,1,2", "2,1,3,2,0", "3,2,3,3,1", "7,1,4,2,2", "5,3,4,2,0", "12,3,5,1,1", "1,2,3,3,2",
      "14,2,5,1,0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("2,2", "2,5")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregateWithAverage(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.cast(BasicTypeInfo.DOUBLE_TYPE_INFO).avg)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1.0", "2,2.0", "2,2.5", "3,4.0", "3,4.5", "3,5.0", "4,7.0", "4,7.5", "4,8.0", "4,8.5",
      "5,11.0", "5,11.5", "5,12.0", "5,12.5", "5,13.0", "6,16.0", "6,16.5", "6,17.0", "6,17.5",
      "6,18.0", "6,18.5")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
