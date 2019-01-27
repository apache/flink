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

package org.apache.flink.table.plan

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableConfigOptions, Types}
import org.apache.flink.table.expressions.utils.Func1
import org.apache.flink.table.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.optimize.FlinkStreamPrograms
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithRetract
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.util._

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet

import org.apache.calcite.sql.validate.SqlMonotonicity._
import org.apache.calcite.tools.RuleSets
import org.junit.Assert._
import org.junit.Test

import java.util.concurrent.TimeUnit

class ModifiedMonotonicityTest extends TableTestBase {

  @Test
  def testSource(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(String, Int)]('word, 'number)
    assertMono(new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT)), table, util)
  }

  @Test
  def testSelect(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(String, Int)]('word, 'number)
    val resultTable = table.select('word, 'number)
    assertMono(new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT)), resultTable, util)
  }

  @Test
  def testCorrelate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)
    val resultTable = table.join(function('c) as 's).select('c, 's)
    assertMono(new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT)), resultTable, util)
  }

  @Test
  def testOver(): Unit = {
    val util = streamTestUtil()
    val table: Table = util.addTable[(Int, String, Long)]("MyTable",
      'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    val weightedAvg = new WeightedAvgWithRetract
    val plusOne = Func1
    val resultTable = table
      .window(Over partitionBy 'b orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select(
        plusOne('a.sum over 'w as 'wsum) as 'd,
        ('a.count over 'w).exp(),
        (weightedAvg('c, 'a) over 'w) + 1,
        "AVG:".toExpr + (weightedAvg('c, 'a) over 'w),
        array(weightedAvg('c, 'a) over 'w, 'a.count over 'w))
    assertMono(new RelModifiedMonotonicity(
      Array(CONSTANT, CONSTANT, CONSTANT, CONSTANT, CONSTANT)), resultTable, util)
  }

  @Test
  def testUnion(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)
    val resultTable = table1.select('a, 'c).unionAll(table2.select('a, 'c))
    assertMono(new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT)), resultTable, util)
  }

  @Test
  def testWindowAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .select('w1.proctime as 'proctime, 'string, 'int.count)
    assertMono(
      new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT, CONSTANT)), windowedTable, util)
  }

  @Test
  def testWindowAggEarlyFire(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.getConfig.withEarlyFireInterval(Time.of(1, TimeUnit.MINUTES))
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .select('w1.proctime as 'proctime, 'string, 'int.count)
    assertMono(null, windowedTable, util)
  }

  @Test
  def testOneAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('b, 'a.max, 'a.avg)
    assertMono(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING, NOT_MONOTONIC)), resultTable, util)
  }

  @Test
  def testOneAggWithLocalGlobal(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('b, 'a.count, 'a.avg)
    assertMono(new RelModifiedMonotonicity(Array(CONSTANT, INCREASING, NOT_MONOTONIC)),
               resultTable, util)
  }

  @Test
  def testTwoAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('a, 'b)
      .select('a, 'b, 'c.count as 'c)
      .groupBy('a)
      .select('a, 'c.max)
    assertMono(new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)), resultTable, util)
  }

  @Test
  def testTwoAggWithLocalGlobal(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('a, 'b)
      .select('a, 'b, 'c.count as 'c)
      .groupBy('a)
      .select('a, 'c.max)
    assertMono(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)),
      resultTable, util)
  }

  @Test
  def testJoin(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val table2 = util.addTable[(Long, Int, String)]('aa, 'bb, 'cc)

    val lTable = table1.groupBy('a).select('a, 'b.min)
    val rTable = table2.groupBy('aa, 'bb).select('aa, 'bb, 'cc.count)
    val resultTable = lTable.join(rTable, 'a === 'bb)
    assertMono(new RelModifiedMonotonicity(
      Array(CONSTANT, DECREASING, CONSTANT, CONSTANT, INCREASING)), resultTable, util)
  }

  @Test
  def testJoinWithDelete(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val table2 = util.addTable[(Long, Int, String)]('aa, 'bb, 'cc)

    val lTable = table1.groupBy('a).select('a, 'b.min)
    val rTable = table2.groupBy('aa, 'bb).select('aa, 'bb, 'cc.count as 'cc)
    val resultTable = lTable.join(rTable, 'a === 'cc)
    assertMono(null, resultTable, util)
  }

  @Test
  def testUpdateSourceAndMax(): Unit = {
    val util = streamTestUtil()

    val rowTypeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)

    util.tableEnv.registerTableSource("MyTable", new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))
    )(rowTypeInfo.asInstanceOf[TypeInformation[(Int, Long, String)]]))

    injectRules(
      util.tableEnv,
      FlinkStreamPrograms.LOGICAL_REWRITE,
      RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val resultTable = util.tableEnv.scan("MyTable")
      .groupBy('pk)
      .select('pk, 'a.max, 'c.count)
    assertMono(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC, INCREASING)),
      resultTable,
      util)
  }

  @Test
  def testUpdateSourceAndMax2(): Unit = {
    val util = streamTestUtil()

    val rowTypeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)

    util.tableEnv.registerTableSource("MyTable", new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))
    )(rowTypeInfo.asInstanceOf[TypeInformation[(Int, Long, String)]]))

    injectRules(
      util.tableEnv,
      FlinkStreamPrograms.LOGICAL_REWRITE,
      RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val resultTable = util.tableEnv.scan("MyTable")
      .groupBy('c)
      .select('a.count)
    assertMono(null, resultTable, util)
  }

  def assertMono(expect: RelModifiedMonotonicity, table: Table, util: StreamTableTestUtil): Unit = {
    val relNode = table.getRelNode
    val optimized = util.tableEnv.optimize(relNode, updatesAsRetraction = false)
    val mq = util.tableEnv.getRelBuilder.getCluster.getMetadataQuery
    assertEquals(expect,
       mq.asInstanceOf[FlinkRelMetadataQuery].getRelModifiedMonotonicity(optimized))
  }
}
