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

package org.apache.flink.table.catalog

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.factories.utils.TestCollectionTableFactory
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Ignore, Test}

import java.util

import scala.collection.JavaConversions._

/** Test cases for catalog table. */
@RunWith(classOf[Parameterized])
class CatalogTableITCase(isStreaming: Boolean) {

  private val batchExec: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var batchEnv: BatchTableEnvironment = _
  private val streamExec: StreamExecutionEnvironment = StreamExecutionEnvironment
    .getExecutionEnvironment
  private var streamEnv: StreamTableEnvironment = _

  private val SOURCE_DATA = List(
      toRow(1, "a"),
      toRow(2, "b"),
      toRow(3, "c")
  )

  private val DIM_DATA = List(
    toRow(1, "aDim"),
    toRow(2, "bDim"),
    toRow(3, "cDim")
  )

  implicit def rowOrdering: Ordering[Row] = Ordering.by((r : Row) => {
    val builder = new StringBuilder
    0 until r.getArity foreach(idx => builder.append(r.getField(idx)))
    builder.toString()
  })

  @Before
  def before(): Unit = {
    batchExec.setParallelism(4)
    streamExec.setParallelism(4)
    batchEnv = BatchTableEnvironment.create(batchExec)
    streamEnv = StreamTableEnvironment.create(streamExec)
    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.isStreaming = isStreaming
  }

  def toRow(args: Any*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  def tableEnv: TableEnvironment = {
    if (isStreaming) {
      streamEnv
    } else {
      batchEnv
    }
  }

  def execJob(name: String) = {
    if (isStreaming) {
      streamExec.execute(name)
    } else {
      batchExec.execute(name)
    }
  }

  @Test
  def testInsertInto(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3)
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.b, (t1.a + 1) as c from t1
      """.stripMargin
    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Ignore // need to implement
  @Test
  def testInsertTargetTableWithComputedColumn(): Unit = {
    TestCollectionTableFactory.initData(SOURCE_DATA)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b varchar,
        |  c as a + 1
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2(a, b)
        |select t1.a, t1.b from t1
      """.stripMargin
    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(SOURCE_DATA.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testInsertWithJoinedSource(): Unit = {
    val sourceData = List(
      toRow(1, 1000, 2),
      toRow(2, 1, 3),
      toRow(3, 2000, 4),
      toRow(1, 2, 2),
      toRow(2, 3000, 3)
    )

    val expected = List(
      toRow(1, 1000, 2, 1),
      toRow(1, 2, 2, 1),
      toRow(2, 1, 1, 2),
      toRow(2, 3000, 1, 2)
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b int,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b int,
        |  c int,
        |  d int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select a.a, a.b, b.a, b.b
        |  from t1 a
        |  join t1 b
        |  on a.a = b.b
      """.stripMargin
    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testInsertWithAggregateSource(): Unit = {
    if (isStreaming) {
      return
    }
    val sourceData = List(
      toRow(1, 1000, 2),
      toRow(2, 1000, 3),
      toRow(3, 2000, 4),
      toRow(4, 2000, 5),
      toRow(5, 3000, 6)
    )

    val expected = List(
      toRow(3, 1000),
      toRow(5, 3000),
      toRow(7, 2000)
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b int,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select sum(a), t1.b from t1 group by t1.b
      """.stripMargin
    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test @Ignore // need to implement
  def testStreamSourceTableWithProctime(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )
    TestCollectionTableFactory.initData(sourceData, emitInterval = 1000L)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b int,
        |  c as proctime,
        |  primary key(a)
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select sum(a), sum(b) from t1 group by TUMBLE(c, INTERVAL '1' SECOND)
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(TestCollectionTableFactory.RESULT.sorted, sourceData.sorted)
  }

  @Test @Ignore // need to implement
  def testStreamSourceTableWithRowtime(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )
    TestCollectionTableFactory.initData(sourceData, emitInterval = 1000L)
    val sourceDDL =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  primary key(a),
        |  WATERMARK wm FOR a AS BOUNDED WITH DELAY 1000 MILLISECOND
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a bigint,
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select sum(a), sum(b) from t1 group by TUMBLE(wm, INTERVAL '1' SECOND)
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(TestCollectionTableFactory.RESULT.sorted, sourceData.sorted)
  }

  @Test @Ignore // need to implement
  def testBatchSourceTableWithProctime(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )
    TestCollectionTableFactory.initData(sourceData, emitInterval = 1000L)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b int,
        |  c as proctime,
        |  primary key(a)
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select sum(a), sum(b) from t1 group by TUMBLE(c, INTERVAL '1' SECOND)
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(TestCollectionTableFactory.RESULT.sorted, sourceData.sorted)
  }

  @Test @Ignore // need to implement
  def testBatchTableWithRowtime(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )
    TestCollectionTableFactory.initData(sourceData, emitInterval = 1000L)
    val sourceDDL =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  primary key(a),
        |  WATERMARK wm FOR a AS BOUNDED WITH DELAY 1000 MILLISECOND
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a bigint,
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select sum(a), sum(b) from t1 group by TUMBLE(wm, INTERVAL '1' SECOND)
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(query)
    execJob("testJob")
    assertEquals(TestCollectionTableFactory.RESULT.sorted, sourceData.sorted)
  }

  @Test
  def testDropTableWithFullPath(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val ddl2 =
      """
        |create table t2(
        |  a bigint,
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.sqlUpdate(ddl1)
    tableEnv.sqlUpdate(ddl2)
    assert(tableEnv.listTables().sameElements(Array[String]("t1", "t2")))
    tableEnv.sqlUpdate("DROP TABLE default_catalog.default_database.t2")
    assert(tableEnv.listTables().sameElements(Array("t1")))
  }

  @Test
  def testDropTableWithPartialPath(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val ddl2 =
      """
        |create table t2(
        |  a bigint,
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.sqlUpdate(ddl1)
    tableEnv.sqlUpdate(ddl2)
    assert(tableEnv.listTables().sameElements(Array[String]("t1", "t2")))
    tableEnv.sqlUpdate("DROP TABLE default_database.t2")
    tableEnv.sqlUpdate("DROP TABLE t1")
    assert(tableEnv.listTables().isEmpty)
  }

  @Test(expected = classOf[TableException])
  def testDropTableWithInvalidPath(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.sqlUpdate(ddl1)
    assert(tableEnv.listTables().sameElements(Array[String]("t1")))
    tableEnv.sqlUpdate("DROP TABLE catalog1.database1.t1")
    assert(tableEnv.listTables().isEmpty)
  }

  @Test
  def testDropTableWithInvalidPathIfExists(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.sqlUpdate(ddl1)
    assert(tableEnv.listTables().sameElements(Array[String]("t1")))
    tableEnv.sqlUpdate("DROP TABLE IF EXISTS catalog1.database1.t1")
    assert(tableEnv.listTables().sameElements(Array[String]("t1")))
  }
}

object CatalogTableITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
