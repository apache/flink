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

package org.apache.flink.table.api.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableConfig, TableConfigOptions, TableEnvironment}
import org.apache.flink.table.runtime.utils.{StreamTestData, TestingAppendTableSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.{TableFunc1, TableTestBase}
import org.junit.Assert.assertEquals
import org.junit._

import scala.io.Source

class ExplainTest extends TableTestBase {

  @Test
  def testFilter(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, String)]('a, 'b).filter("a % 2 = 0")
    util.verifyPlan(table)
  }

  @Test
  def testUnion(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Int, String)]('count, 'word)
    val table2 = util.addTable[(Int, String)]('count, 'word)
    val table = table1.unionAll(table2)
    util.verifyPlan(table)
  }

  @Test
  def testSubsectionOptimizationStream0(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)

    t.groupBy('num)
      .select('num, 'id.count as 'cnt)
      .writeToSink(new TestingUpsertTableSink(Array(0)))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testSubsectionOptimizationStream0.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testSubsectionOptimizationForSQL(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T", ds, 'id, 'num, 'text)
    tEnv.sqlQuery("SELECT num, count(id) as cnt FROM T GROUP BY num")
      .writeToSink(new TestingUpsertTableSink(Array(0)))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testSubsectionOptimizationForSQL.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testSubsectionOptimizationStream1(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val table = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)

    val table1 = table.where('id <= 10).select('id as 'id1, 'num)
    val table2 = table.where('id >= 0).select('id, 'num, 'text)
    val table3 = table2.where('num >= 5).select('id as 'id2, 'text)
    val table4 = table2.where('num < 5).select('id as 'id3, 'text as 'text1)
    val table5 = table1.join(table3, 'id1 === 'id2).select('id1, 'num, 'text as 'text2)
    val table6 = table4.join(table5, 'id1 === 'id3).select('id1, 'num, 'text1)

    table6.writeToSink(new TestingUpsertTableSink(Array()))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testSubsectionOptimizationStream1.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testRetractAndUpsertSink(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    t.where('num < 4).select('num, 'cnt)
        .writeToSink(new TestingRetractTableSink)
    t.where('num >= 4 && 'num < 6).select('num, 'cnt)
      .writeToSink(new TestingUpsertTableSink(Array()))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testRetractAndUpsertSink.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testRetractAndUpsertSinkForSQL(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T", ds, 'id, 'num, 'text)
    val t1 = tEnv.sqlQuery("SELECT num, count(id) as cnt FROM T GROUP BY num")
    tEnv.registerTable("T1", t1)

    tEnv.sqlQuery("SELECT num, cnt FROM T1 WHERE num < 4")
      .writeToSink(new TestingRetractTableSink)

    tEnv.sqlQuery("SELECT num, cnt FROM T1 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testRetractAndUpsertSinkForSQL.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testUpdateAsRetractConsumedAtSinkBlock(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val mytable = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", mytable)

    val t = tEnv.sqlQuery("SELECT a, b, c FROM MyTable")
    tEnv.registerTable("T", t)
    val retractSink = new TestingRetractTableSink
    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as rank_num
         |  FROM T)
         |WHERE rank_num <= 10
      """.stripMargin
    tEnv.sqlQuery(sql).writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    tEnv.sqlQuery("SELECT a, b FROM T WHERE a < 6").writeToSink(upsertSink)

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testUpdateAsRetractConsumedAtSinkBlock.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testUpdateAsRetractConsumedAtSourceBlock(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val myTable = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", myTable)

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as rank_num
         |  FROM MyTable)
         |WHERE rank_num <= 10
      """.stripMargin

    val t = tEnv.sqlQuery(sql)
    tEnv.registerTable("T", t)
    val retractSink = new TestingRetractTableSink
    tEnv.sqlQuery("SELECT a FROM T where a > 6").writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    tEnv.sqlQuery("SELECT a, b FROM T WHERE a < 6").writeToSink(upsertSink)

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testUpdateAsRetractConsumedAtSourceBlock.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testUpsertAndUpsertSink(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val t = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    t.where('num < 4).groupBy('cnt).select('cnt, 'num.count as 'frequency)
        .writeToSink(new TestingUpsertTableSink(Array(0)))
    t.where('num >= 4 && 'num < 6).select('num, 'cnt)
      .writeToSink(new TestingUpsertTableSink(Array()))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testUpsertAndUpsertSink.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testUpsertAndUpsertSinkForSQL(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T", ds, 'id, 'num, 'text)
    val t1 = tEnv.sqlQuery("SELECT num, count(id) as cnt FROM T GROUP BY num")
    tEnv.registerTable("T1", t1)

    tEnv.sqlQuery(
      """
        |SELECT cnt, count(num) frequency
        |FROM (
        |  SELECT * FROM T1 WHERE num < 4
        |)
        |GROUP BY cnt
      """.stripMargin)
        .writeToSink(new TestingUpsertTableSink(Array(0)))

    tEnv.sqlQuery("SELECT num, cnt FROM T1 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testUpsertAndUpsertSinkForSQL.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testMultiLevelViewForSQL(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    conf.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T", ds, 'id, 'num, 'text)

    val t1 = tEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%hello%'")
    tEnv.registerTable("T1", t1)
    t1.writeToSink(new TestingAppendTableSink)
    val t2 = tEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%world%'")
    tEnv.registerTable("T2", t2)

    val t3 = tEnv.sqlQuery(
      """
        |SELECT num, count(id) as cnt
        |FROM
        |(
        | (SELECT * FROM T1)
        | UNION ALL
        | (SELECT * FROM T2)
        |)
        |GROUP BY num
      """.stripMargin)
    tEnv.registerTable("T3", t3)

    tEnv.sqlQuery("SELECT num, cnt FROM T3 WHERE num < 4")
        .writeToSink(new TestingRetractTableSink)

    tEnv.sqlQuery("SELECT num, cnt FROM T3 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testMultiLevelViewForSQL.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }

  @Test
  def testSharedUnionNode(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    conf.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T", ds, 'id, 'num, 'text)

    val t1 = tEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%hello%'")
    tEnv.registerTable("T1", t1)
    t1.writeToSink(new TestingAppendTableSink)
    val t2 = tEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%world%'")
    tEnv.registerTable("T2", t2)

    val t3 = tEnv.sqlQuery(
      """
        |SELECT * FROM T1
        |UNION ALL
        |SELECT * FROM T2
      """.stripMargin)
    tEnv.registerTable("T3", t3)
    tEnv.sqlQuery("SELECT * FROM T3 WHERE num >= 5")
      .writeToSink(new TestingRetractTableSink)

    val t4 = tEnv.sqlQuery(
      """
        |SELECT num, count(id) as cnt
        |FROM T3
        |GROUP BY num
      """.stripMargin)
    tEnv.registerTable("T4", t4)

    tEnv.sqlQuery("SELECT num, cnt FROM T4 WHERE num < 4")
      .writeToSink(new TestingRetractTableSink)

    tEnv.sqlQuery("SELECT num, cnt FROM T4 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    val result = replaceString(tEnv.explain())
    val source = readFromResource("testSharedUnionNode.out")
    val expected = replaceString(source)
    assertEquals(expected, result)
  }


  @Test
  def testSubsectionOptimizationWithUdtf(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)
    tEnv.registerDataStream("t3", StreamTestData.get5TupleDataStream(env), 'i, 'j, 'k, 'l, 'm)
    tEnv.registerFunction("split", new TableFunc1)

    val t1 = tEnv.scan("t1")
    val t2 = tEnv.scan("t2")
    val t3 = tEnv.scan("t3")
    val result = t1.join(t2).where("b === e").join(t3).where("a === i")
      .join(new Table(tEnv, "split(f) AS f1"))
    result.writeToSink(new CsvTableSink("file"))

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testSubsectionOptimizationWithUdtf.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testMultiSinksSplitOnUnion1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)

    val t1 = tEnv.scan("t1").select('a, 'c)
    val t2 = tEnv.scan("t2").select('d, 'f)
    val table = t1.unionAll(t2)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testMultiSinksSplitOnUnion1.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testMultiSinksSplitOnUnion2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)

    val t1 = tEnv.scan("t1")
    val t2 = tEnv.scan("t2")

    val query = "SELECT a, c FROM t1  union all SELECT d, f FROM t2"
    val table = tEnv.sqlQuery(query)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testMultiSinksSplitOnUnion2.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testMultiSinksSplitOnUnion3(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)
    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)
    tEnv.registerDataStream("t3", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)

    val t1 = tEnv.scan("t1")
    val t2 = tEnv.scan("t2")
    val t3 = tEnv.scan("t3")
    val table = t1.unionAll(t2).unionAll(t3)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    val result3 = t1.unionAll(t2).select('a)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)
    result3.writeToSink(new TestingUpsertTableSink(Array()))

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testMultiSinksSplitOnUnion3.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testMultiSinksSplitOnUnion4(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)
    tEnv.registerDataStream("t3", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)


    val query = "SELECT a, c FROM t1 union all SELECT d, f FROM t2 "
    val table = tEnv.sqlQuery(query)
    val table2 = table.unionAll(tEnv.sqlQuery("select a, c from t3"))
    val result1 = table.select('a)
    val result2 = table2.select('a.sum as 'total_sum)
    val result3 = table2.select('a.min as 'total_min)
    result1.writeToSink(new TestingAppendTableSink)
    result2.writeToSink(new TestingRetractTableSink)
    result3.writeToSink(new TestingUpsertTableSink(Array()))

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testMultiSinksSplitOnUnion4.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testMultiSinksSplitOnUnion5(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)
    tEnv.registerDataStream("t3", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)

    val query = "SELECT a, c FROM t1 union all SELECT d, f FROM t2 " +
      "union all select a, c from t3"
    val table = tEnv.sqlQuery(query)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testMultiSinksSplitOnUnion5.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testSingleSinkSplitOnUnion1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)

    val scan1 = tEnv.scan("t1").select('a, 'c)
    val scan2 = tEnv.scan("t2").select('d, 'f)
    val table = scan1.unionAll(scan2)
    val result = table.select('a.sum as 'total_sum)
    result.writeToSink(new TestingRetractTableSink)

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testSingleSinkSplitOnUnion1.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testSingleSinkSplitOnUnion2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)
    tEnv.registerDataStream("t2", StreamTestData.getSmall3TupleDataStream(env), 'd, 'e, 'f)
    val query = "SELECT a, c FROM t1  union all SELECT d, f FROM t2"
    val table = tEnv.sqlQuery(query)
    val result = table.select('a.sum as 'total_sum)
    result.writeToSink(new TestingRetractTableSink)

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testSingleSinkSplitOnUnion2.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  @Test
  def testUnionAggWithDifferentGroupings(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)

    tEnv.registerDataStream("t1", StreamTestData.get3TupleDataStream(env), 'a, 'b, 'c)

    val query = "SELECT a, b, c FROM t1"
    val table = tEnv.sqlQuery(query)
    val result1 = table.groupBy('a, 'b, 'c).select('a, 'b, 'c, 'a.sum as 'a_sum)
    val result2 = table.groupBy('b, 'c).select(1 as 'a, 'b, 'c, 'a.sum as 'a_sum)
    val result3 = result1.unionAll(result2)
    result3.writeToSink(new TestingUpsertTableSink(Array()))

    val actual = replaceString(tEnv.explain())
    val source = readFromResource("testUnionAggWithDifferentGroupings.out")
    val expected = replaceString(source)
    assertEquals(expected, actual)
  }

  private def readFromResource(name: String): String = {
    val inputStream = getClass.getResource("/explain/" + name).getFile
    Source.fromFile(inputStream).mkString
  }

  private def replaceString(s: String): String = {
    /* Stage {id} is ignored, because id keeps incrementing in test class
     * while StreamExecutionEnvironment is up
     */
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
  }
}
