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
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, ValidationException}
import org.apache.flink.table.factories.utils.TestCollectionTableFactory
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row

import org.junit.Assert.{assertEquals, fail}
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Ignore, Rule, Test}

import java.util

import scala.collection.JavaConversions._

/** Test cases for catalog table. */
@RunWith(classOf[Parameterized])
class CatalogTableITCase(isStreaming: Boolean) extends AbstractTestBase {

  private val batchExec: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var batchEnv: BatchTableEnvironment = _
  private val streamExec: StreamExecutionEnvironment = StreamExecutionEnvironment
    .getExecutionEnvironment
  private var streamEnv: StreamTableEnvironment = _
  private val settings = EnvironmentSettings.newInstance().useOldPlanner().build()

  private val SOURCE_DATA = List(
      toRow(1, "a"),
      toRow(2, "b"),
      toRow(3, "c")
  )

  implicit def rowOrdering: Ordering[Row] = Ordering.by((r : Row) => {
    val builder = new StringBuilder
    0 until r.getArity foreach(idx => builder.append(r.getField(idx)))
    builder.toString()
  })

  var _expectedEx: ExpectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedEx

  @Before
  def before(): Unit = {
    batchExec.setParallelism(4)
    streamExec.setParallelism(4)
    batchEnv = BatchTableEnvironment.create(batchExec)
    streamEnv = StreamTableEnvironment.create(streamExec, settings)
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
      tableEnv.execute(name)
    } else {
      tableEnv.execute(name)
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

  @Test @Ignore("FLINK-14320") // need to implement
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
        |  a timestamp(3),
        |  b bigint,
        |  WATERMARK FOR a AS a - interval '1' SECOND
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a timestamp(3),
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select a, sum(b) from t1 group by TUMBLE(a, INTERVAL '1' SECOND)
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

  @Test @Ignore("FLINK-14320") // need to implement
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
        |  a timestamp(3),
        |  b bigint,
        |  WATERMARK FOR a AS a - interval '1' SECOND
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a timestamp(3),
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select a, sum(b) from t1 group by TUMBLE(a, INTERVAL '1' SECOND)
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

  @Test(expected = classOf[ValidationException])
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

  @Test
  def testDropTableSameNameWithTemporaryTable(): Unit = {
    val createTable1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val createTable2 =
      """
        |create temporary table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(createTable1)
    tableEnv.executeSql(createTable2)

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("Temporary table with identifier "
      + "'`default_catalog`.`default_database`.`t1`' exists. "
      + "Drop it first before removing the permanent table.")
    tableEnv.executeSql("drop table t1")
  }

  @Test
  def testDropViewSameNameWithTable(): Unit = {
    val createTable1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(createTable1)

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("View with identifier "
      + "'default_catalog.default_database.t1' does not exist.")
    tableEnv.executeSql("drop view t1")
  }

  @Test
  def testDropViewSameNameWithTableIfNotExists(): Unit = {
    val createTable1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(createTable1)
    tableEnv.executeSql("drop view if exists t1")
    assert(tableEnv.listTables().sameElements(Array("t1")))
  }

  @Test
  def testAlterTable(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'k1' = 'v1'
        |)
      """.stripMargin
    tableEnv.sqlUpdate(ddl1)
    tableEnv.sqlUpdate("alter table t1 rename to t2")
    assert(tableEnv.listTables().sameElements(Array[String]("t2")))
    tableEnv.sqlUpdate("alter table t2 set ('k1' = 'a', 'k2' = 'b')")
    val expectedProperties = new util.HashMap[String, String]()
    expectedProperties.put("connector", "COLLECTION")
    expectedProperties.put("k1", "a")
    expectedProperties.put("k2", "b")
    val properties = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .getTable(new ObjectPath(tableEnv.getCurrentDatabase, "t2"))
      .getProperties
    assertEquals(expectedProperties, properties)
  }

  @Test
  def testUseCatalog(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
    tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
    tableEnv.sqlUpdate("use catalog cat1")
    assertEquals("cat1", tableEnv.getCurrentCatalog)
    tableEnv.sqlUpdate("use catalog cat2")
    assertEquals("cat2", tableEnv.getCurrentCatalog)
  }

  @Test
  def testUseDatabase(): Unit = {
    val catalog = new GenericInMemoryCatalog("cat1")
    tableEnv.registerCatalog("cat1", catalog)
    val catalogDB1 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db1")
    val catalogDB2 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db2")
    catalog.createDatabase("db1", catalogDB1, true)
    catalog.createDatabase("db2", catalogDB2, true)
    tableEnv.sqlUpdate("use cat1.db1")
    assertEquals("db1", tableEnv.getCurrentDatabase)
    tableEnv.sqlUpdate("use db2")
    assertEquals("db2", tableEnv.getCurrentDatabase)
  }

  @Test
  def testCreateDatabase: Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("default"))
    tableEnv.sqlUpdate("use catalog cat1")
    tableEnv.sqlUpdate("create database db1 ")
    tableEnv.sqlUpdate("create database if not exists db1 ")
    try {
      tableEnv.sqlUpdate("create database db1 ")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.sqlUpdate("create database cat2.db1 comment 'test_comment'" +
                         " with ('k1' = 'v1', 'k2' = 'v2')")
    val database = tableEnv.getCatalog("cat2").get().getDatabase("db1")
    assertEquals("test_comment", database.getComment)
    assertEquals(2, database.getProperties.size())
    val expectedProperty = new util.HashMap[String, String]()
    expectedProperty.put("k1", "v1")
    expectedProperty.put("k2", "v2")
    assertEquals(expectedProperty, database.getProperties)
  }

  @Test
  def testDropDatabase: Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.sqlUpdate("use catalog cat1")
    tableEnv.sqlUpdate("create database db1")
    tableEnv.sqlUpdate("drop database db1")
    tableEnv.sqlUpdate("drop database if exists db1")
    try {
      tableEnv.sqlUpdate("drop database db1")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.sqlUpdate("create database db1")
    tableEnv.sqlUpdate("use db1")
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
    val ddl2 =
      """
        |create table t2(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.sqlUpdate(ddl2)
    try {
      tableEnv.sqlUpdate("drop database db1")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.sqlUpdate("drop database db1 cascade")
  }

  @Test
  def testAlterDatabase: Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.sqlUpdate("use catalog cat1")
    tableEnv.sqlUpdate("create database db1 comment 'db1_comment' with ('k1' = 'v1')")
    tableEnv.sqlUpdate("alter database db1 set ('k1' = 'a', 'k2' = 'b')")
    val database = tableEnv.getCatalog("cat1").get().getDatabase("db1")
    assertEquals("db1_comment", database.getComment)
    assertEquals(2, database.getProperties.size())
    val expectedProperty = new util.HashMap[String, String]()
    expectedProperty.put("k1", "a")
    expectedProperty.put("k2", "b")
    assertEquals(expectedProperty, database.getProperties)
  }

  @Test(expected = classOf[ValidationException])
  def testCreateViewTwice(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T3(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(viewDDL)
    tableEnv.sqlUpdate(viewDDL)
  }

  @Test(expected = classOf[ValidationException])
  def testCreateTemporaryViewTwice(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW T3(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(viewDDL)
    tableEnv.sqlUpdate(viewDDL)
  }

  @Test
  def testCreateViewIfNotExistsTwice(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    val query = "SELECT c, d FROM T3"

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(viewDDL)
    tableEnv.sqlUpdate(viewDDL)

    val result = tableEnv.sqlQuery(query)
    result.insertInto("T2")
    execJob("testJob")
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testCreateTemporaryViewIfNotExistsTwice(): Unit = {
    val sourceData = List(
      toRow(1, 1000),
      toRow(2, 2000),
      toRow(3, 3000)
    )

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW IF NOT EXISTS T3(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    val query = "SELECT c, d FROM T3"

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(viewDDL)
    tableEnv.sqlUpdate(viewDDL)

    val result = tableEnv.sqlQuery(query)
    result.insertInto("T2")
    execJob("testJob")
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testDropViewWithFullPath(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val view1DDL =
      """
        |CREATE VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    val view2DDL =
      """
        |CREATE VIEW T3(x, y) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(view1DDL)
    tableEnv.sqlUpdate(view2DDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2", "T3")))

    tableEnv.sqlUpdate("DROP VIEW default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T3")))

    tableEnv.sqlUpdate("DROP VIEW default_catalog.default_database.T3")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
  }

  @Test
  def testDropViewWithPartialPath(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val view1DDL =
      """
        |CREATE VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    val view2DDL =
      """
        |CREATE VIEW T3(x, y) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(view1DDL)
    tableEnv.sqlUpdate(view2DDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2", "T3")))

    tableEnv.sqlUpdate("DROP VIEW T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T3")))

    tableEnv.sqlUpdate("DROP VIEW default_database.T3")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
  }

  @Test(expected = classOf[ValidationException])
  def testDropViewWithInvalidPath(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(viewDDL)
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
    tableEnv.sqlUpdate("DROP VIEW default_catalog1.default_database1.T2")
  }

  @Test
  def testDropViewWithInvalidPathIfExists(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(viewDDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
    tableEnv.sqlUpdate("DROP VIEW IF EXISTS default_catalog1.default_database1.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
  }

  @Test(expected = classOf[ValidationException])
  def testDropViewTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(viewDDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
    tableEnv.sqlUpdate("DROP VIEW default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
    tableEnv.sqlUpdate("DROP VIEW default_catalog.default_database.T2")
  }

  @Test
  def testDropViewIfExistsTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(viewDDL)

    assert(tableEnv.listTables().sameElements(Array[String]("T1", "T2")))
    tableEnv.sqlUpdate("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
    tableEnv.sqlUpdate("DROP VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTables().sameElements(Array[String]("T1")))
  }

  @Test(expected = classOf[ValidationException])
  def testDropTemporaryViewTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(viewDDL)

    assert(tableEnv.listTemporaryViews().sameElements(Array[String]("T2")))
    tableEnv.sqlUpdate("DROP TEMPORARY VIEW default_catalog.default_database.T2")
    assert(tableEnv.listTemporaryViews().sameElements(Array[String]()))
    tableEnv.sqlUpdate("DROP TEMPORARY VIEW default_catalog.default_database.T2")
  }

  @Test
  def testDropTemporaryViewIfExistsTwice(): Unit = {
    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW T2(c, d) AS SELECT a, b FROM T1
      """.stripMargin

    tableEnv.sqlUpdate(sourceDDL)
    tableEnv.sqlUpdate(viewDDL)

    assert(tableEnv.listTemporaryViews().sameElements(Array[String]("T2")))
    tableEnv.sqlUpdate("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTemporaryViews().sameElements(Array[String]()))
    tableEnv.sqlUpdate("DROP TEMPORARY VIEW IF EXISTS default_catalog.default_database.T2")
    assert(tableEnv.listTemporaryViews().sameElements(Array[String]()))
  }

}

object CatalogTableITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
