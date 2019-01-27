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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType, TypeInfoWrappedDataType}
import org.apache.flink.table.api.{TableConfigOptions, TableException, Types}
import org.apache.flink.table.expressions.AggFunctionCall
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test

class AggregateTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testCannotCountOnMultiFields(): Unit = {
    val sql = "SELECT b, count(a, c) FROM MyTable GROUP BY b"
    thrown.expect(classOf[TableException])
    thrown.expectMessage("We now only support the count of one field")
    streamUtil.explainSql(sql)
  }

  @Test
  def testAggWithMiniBatch(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c)  FROM MyTable GROUP BY b"
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testAggAfterUnionWithMiniBatch(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    streamUtil.addTable[(Long, Int, String)]("T1", 'a, 'b, 'c)
    streamUtil.addTable[(Long, Int, String)]("T2", 'a, 'b, 'c)

    val sql =
      """
        |SELECT a, sum(b), count(distinct c)
        |FROM (
        |  SELECT * FROM T1
        |  UNION ALL
        |  SELECT * FROM T2
        |) GROUP BY a
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testGroupbyWithoutWindow(): Unit = {
    val sql = "SELECT COUNT(a) FROM MyTable GROUP BY b"
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testLocalGlobalAggAfterUnion(): Unit = {
    // enable local global optimize
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    streamUtil.addTable[(Int, String, Long)]("T1", 'a, 'b, 'c)
    streamUtil.addTable[(Int, String, Long)]("T2", 'a, 'b, 'c)

    val sql =
      """
        |SELECT a, sum(c), count(distinct b)
        |FROM (
        |  SELECT * FROM T1
        |  UNION ALL
        |  SELECT * FROM T2
        |) GROUP BY a
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testAggWithFilterClause(): Unit = {
    streamUtil.addTable[(Int, Long, String, Boolean)]("T", 'a, 'b, 'c, 'd)

    val sql =
      """
        |SELECT
        |  a,
        |  sum(b) filter (where c = 'A'),
        |  count(distinct c) filter (where d is true),
        |  max(b)
        |FROM T GROUP BY a
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testAggWithFilterClauseWithLocalGlobal(): Unit = {
    streamUtil.addTable[(Int, Long, String, Boolean)]("T", 'a, 'b, 'c, 'd)
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    val sql =
      """
        |SELECT
        |  a,
        |  sum(b) filter (where c = 'A'),
        |  count(distinct c) filter (where d is true),
        |  count(distinct c) filter (where b = 1),
        |  max(b)
        |FROM T GROUP BY a
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testUserDefinedAggregateFunctionWithScalaAccumulator(): Unit = {
    streamUtil.addFunction("udag", new MyAgg)
    val call = streamUtil
      .tableEnv
      .chainedFunctionCatalog
      .lookupFunction("udag", Seq())
      .asInstanceOf[AggFunctionCall]

    val typeInfo = call.externalAccType
    assertEquals(2,
      typeInfo.asInstanceOf[TypeInfoWrappedDataType].
          getTypeInfo.asInstanceOf[CaseClassTypeInfo[_]].getTotalFields)
    val caseTypeInfo = typeInfo.asInstanceOf[TypeInfoWrappedDataType].
        getTypeInfo.asInstanceOf[CaseClassTypeInfo[_]]
    assertEquals(Types.LONG, caseTypeInfo.getTypeAt(0))
    assertEquals(Types.LONG, caseTypeInfo.getTypeAt(1))

    streamUtil.addFunction("udag2", new MyAgg2)
    val call2 = streamUtil
      .tableEnv
      .chainedFunctionCatalog
      .lookupFunction("udag2", Seq())
      .asInstanceOf[AggFunctionCall]

    val typeInfo2 = call2.externalAccType
    assertEquals(2,
      typeInfo2.asInstanceOf[RowType].getFieldNames.length)
    val rowTypeInfo = typeInfo2.asInstanceOf[RowType]
    assertEquals(DataTypes.LONG, rowTypeInfo.getInternalTypeAt(0))
    assertEquals(DataTypes.INT, rowTypeInfo.getInternalTypeAt(1))
  }

  @Test
  def testColumnIntervalOnDifferentType(): Unit = {
    streamUtil.addTable[(Int, Long)]("T", 'a, 'b)

    // FlinkRelMdModifiedMonotonicity will analyse sum argument's column interval
    // this test covers all column interval types

    val sql =
      """
        |SELECT
        |  a,
        |  sum(cast(1 as INT)),
        |  sum(cast(2 as BIGINT)),
        |  sum(cast(3 as TINYINT)),
        |  sum(cast(4 as SMALLINT)),
        |  sum(cast(5 as FLOAT)),
        |  sum(cast(6 as DECIMAL)),
        |  sum(cast(7 as DOUBLE))
        |FROM T GROUP BY a
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }
}

case class MyAccumulator(var sum: Long, var count: Long)

class MyAgg extends AggregateFunction[Long, MyAccumulator] {

  //Overloaded accumulate method
  def accumulate(acc: MyAccumulator, value: Long): Unit = {
  }

  override def createAccumulator(): MyAccumulator = MyAccumulator(0, 0)

  override def getValue(accumulator: MyAccumulator): Long = 1L
}

class MyAgg2 extends AggregateFunction[Long, Row] {

  def accumulate(acc: Row, value: Long): Unit = {}

  override def createAccumulator(): Row = new Row(2)

  override def getValue(accumulator: Row): Long = 1L

  override def getAccumulatorType: DataType =
    DataTypes.createRowType(DataTypes.LONG, DataTypes.INT)
}
