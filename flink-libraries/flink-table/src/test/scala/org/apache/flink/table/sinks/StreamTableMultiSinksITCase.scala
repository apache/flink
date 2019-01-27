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

package org.apache.flink.table.sinks

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.util.TableFunc0
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test

class StreamTableMultiSinksITCase extends StreamingTestBase {

  @Test
  def testOneSink(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)

    val tableSink = new TestingUpsertTableSink(Array(0,1))
    t.groupBy('num)
      .select('num, 'id.count as 'cnt)
      .writeToSink(tableSink)

    tEnv.execute()
    val expected = List(
      "1,1", "2,1", "2,2", "3,1", "3,2", "3,3", "4,1", "4,2", "4,3", "4,4", "5,1",
      "5,2", "5,3", "5,4", "5,5", "6,1", "6,2", "6,3", "6,4", "6,5", "6,6").sorted
    assertEquals(expected, tableSink.getUpsertResults.sorted)
  }


  @Test
  def testCorrelate(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val func0 = new TableFunc0

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text, 'proctime.proctime)
      .join(func0('text))
      .select('id, 'num, 'age)
      .where('age < 5)
      .where('age > 3)
      .where('age > 2)
      .select('id)

    val upsertSink1 = new TestingUpsertTableSink(Array())
    t.writeToSink(upsertSink1)

    tEnv.execute()

    val retractExpected = List("10").sorted
    assertEquals(retractExpected, upsertSink1.getUpsertResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testRetractSinkAndAppendSink(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    t.where('num < 4).select('num, 'cnt).toRetractStream[Row].addSink(new TestingRetractSink)
        .setParallelism(1)
    t.where('num >= 4).select('num, 'cnt).toAppendStream[Row].addSink(new TestingAppendSink)

    tEnv.execute()
  }

  @Test(expected = classOf[TableException])
  def testUpsertSinkAndAppendSink(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    t.where('num < 4)
      .groupBy('cnt)
      .select('cnt, 'num.count)
      .writeToSink(new TestingUpsertTableSink(Array(0)))
    t.where('num >= 4).select('num, 'cnt).writeToSink(new TestingAppendTableSink)

    tEnv.execute()
  }

  @Test
  def testRetractAndUpsertSink(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    val retractSink = new TestingRetractTableSink
    t.where('num < 4).select('num, 'cnt).writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    t.where('num >= 4 && 'num < 6).select('num, 'cnt).writeToSink(upsertSink)

    tEnv.execute()

    val retractExpected = List(
      "(false,2,1)", "(false,3,1)", "(false,3,2)",
      "(true,1,1)", "(true,2,1)", "(true,2,2)",
      "(true,3,1)", "(true,3,2)", "(true,3,3)").sorted
    assertEquals(retractExpected, retractSink.getRawResults.sorted)

    val upsertExpected = List(
      "(true,4,1)", "(false,4,1)", "(true,4,2)",
      "(false,4,2)", "(true,4,3)", "(false,4,3)",
      "(true,4,4)", "(true,5,1)", "(false,5,1)",
      "(true,5,2)", "(false,5,2)", "(true,5,3)",
      "(false,5,3)", "(true,5,4)", "(false,5,4)",
      "(true,5,5)").sorted
    assertEquals(upsertExpected, upsertSink.getRawResults.sorted)
  }

  @Test
  def testMultiSinkInsqlQuery(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)

    val mytable = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)
    tEnv.registerTable("MyTable", mytable)

    val t = tEnv.sqlQuery("SELECT num, count(id) as cnt FROM MyTable GROUP BY num")
    tEnv.registerTable("T", t)
    val retractSink = new TestingRetractTableSink
    tEnv.sqlQuery("SELECT num, cnt FROM T WHERE num < 4").writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    tEnv.sqlQuery("SELECT num, cnt FROM T WHERE num >= 4 AND num < 6").writeToSink(upsertSink)

    tEnv.execute()

    val retractExpected = List(
      "(true,1,1)", "(true,2,1)", "(false,2,1)",
      "(true,2,2)", "(true,3,1)", "(false,3,1)",
      "(true,3,2)", "(false,3,2)", "(true,3,3)").sorted
    assertEquals(retractExpected, retractSink.getRawResults.sorted)

    val upsertExpected = List(
      "(true,4,1)", "(false,4,1)", "(true,4,2)",
      "(false,4,2)", "(true,4,3)", "(false,4,3)",
      "(true,4,4)", "(true,5,1)", "(false,5,1)",
      "(true,5,2)", "(false,5,2)", "(true,5,3)",
      "(false,5,3)", "(true,5,4)", "(false,5,4)",
      "(true,5,5)").sorted
    assertEquals(upsertExpected, upsertSink.getRawResults.sorted)
  }

  @Test
  def testUpsertAndUpsertSink(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    val upsertSink1 = new TestingUpsertTableSink(Array(0))
    t.where('num < 4).groupBy('cnt).select('cnt, 'num.count as 'frequency)
      .writeToSink(upsertSink1)
    val upsertSink2 = new TestingUpsertTableSink(Array())
    t.where('num >= 4 && 'num < 6).select('num, 'cnt).writeToSink(upsertSink2)

    tEnv.execute()

    val upsertExpected1 = List(
      "(true,1,1)", "(true,1,2)", "(true,1,1)",
      "(true,2,1)", "(true,1,2)", "(true,1,1)",
      "(true,2,2)", "(true,2,1)", "(true,3,1)").sorted
    assertEquals(upsertExpected1, upsertSink1.getRawResults.sorted)

    val upsertExpected2 = List(
      "(true,4,1)", "(false,4,1)", "(true,4,2)",
      "(false,4,2)", "(true,4,3)", "(false,4,3)",
      "(true,4,4)", "(true,5,1)", "(false,5,1)",
      "(true,5,2)", "(false,5,2)", "(true,5,3)",
      "(false,5,3)", "(true,5,4)", "(false,5,4)",
      "(true,5,5)").sorted
    assertEquals(upsertExpected2, upsertSink2.getRawResults.sorted)
  }

  @Test
  def testRetractAndUpsertSinkWithTimeIndicator(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)
    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text, 'proctime.proctime)
      .window(Over orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('num, 'id.count over 'w as 'cnt, 'proctime)

    val retractSink = new TestingRetractTableSink
    t.where('num < 4).select('num, 'cnt).writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    t.where('num >= 4 && 'num < 6).select('num, 'cnt).writeToSink(upsertSink)

    tEnv.execute()

    val retractExpected = List(
      "(true,1,1)", "(true,2,2)", "(true,2,3)",
      "(true,3,4)", "(true,3,5)", "(true,3,6)").sorted
    assertEquals(retractExpected, retractSink.getRawResults.sorted)

    val upsertExpected = List(
      "(true,4,10)", "(true,4,7)", "(true,4,8)",
      "(true,4,9)", "(true,5,11)", "(true,5,12)",
      "(true,5,13)", "(true,5,14)", "(true,5,15)").sorted
    assertEquals(upsertExpected, upsertSink.getRawResults.sorted)
  }

  @Test
  def testRetractAndUpsertSinkWithUDTF(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val func0 = new TableFunc0

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text, 'proctime.proctime)
      .join(func0('text))
      .window(Over orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('num, 'id.count over 'w as 'cnt, 'proctime)

    val retractSink = new TestingRetractTableSink
    t.where('num < 5).select('num, 'cnt).writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    t.where('num >= 5).select('num, 'cnt).writeToSink(upsertSink)

    tEnv.execute()

    val retractExpected = List("(true,4,1)", "(true,4,2)", "(true,4,3)", "(true,4,4)").sorted
    assertEquals(retractExpected, retractSink.getRawResults.sorted)

    val upsertExpected = List(
      "(true,5,5)", "(true,5,6)", "(true,5,7)",
      "(true,5,8)", "(true,5,9)", "(true,6,10)",
      "(true,6,11)", "(true,6,12)", "(true,6,13)",
      "(true,6,14)", "(true,6,15)").sorted
    assertEquals(upsertExpected, upsertSink.getRawResults.sorted)
  }

  @Test
  def testJoin(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    env.setParallelism(1)

    val func0 = new TableFunc0

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text, 'proctime.proctime)
      .join(func0('text))


    val upsertSink1 = new TestingUpsertTableSink(Array())
    t.where('id === 7).select('id, 'text).writeToSink(upsertSink1)
    val upsertSink2 = new TestingUpsertTableSink(Array())
    t.where('id === 8).select('id, 'text).writeToSink(upsertSink2)

    tEnv.execute()

    val retractExpected = List("(true,7,Comment#1)").sorted
    assertEquals(retractExpected, upsertSink1.getRawResults.sorted)

    val upsertExpected = List("(true,8,Comment#2)").sorted
    assertEquals(upsertExpected,upsertSink2.getRawResults.sorted)
  }
}
