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

package org.apache.flink.table.planner.catalog

import org.apache.flink.table.api.config.{ExecutionConfigOptions, TableConfigOptions}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, ValidationException}
import org.apache.flink.table.catalog.{CatalogDatabaseImpl, CatalogFunctionImpl, GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Rule, Test}

import java.util
import scala.collection.JavaConversions._

/** Test cases for catalog table. */
@RunWith(classOf[Parameterized])
class CatalogTableITCase(isStreamingMode: Boolean) extends AbstractTestBase {
  //~ Instance fields --------------------------------------------------------

  private val settings = if (isStreamingMode) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  private val tableEnv: TableEnvironment = TableEnvironmentImpl.create(settings)

  var _expectedEx: ExpectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedEx

  @Before
  def before(): Unit = {
    tableEnv.getConfig.getConfiguration.setBoolean(
      TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED,
      true)

    tableEnv.getConfig
      .getConfiguration
      .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
    TestCollectionTableFactory.reset()

    val func = new CatalogFunctionImpl(
      classOf[JavaFunc0].getName)
    tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().createFunction(
      new ObjectPath(tableEnv.getCurrentDatabase, "myfunc"),
      func,
      true)
  }

  //~ Tools ------------------------------------------------------------------

  implicit def rowOrdering: Ordering[Row] = Ordering.by((r: Row) => {
    val builder = new StringBuilder
    0 until r.getArity foreach (idx => builder.append(r.getField(idx)))
    builder.toString()
  })

  def toRow(args: Any*): Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  //~ Tests ------------------------------------------------------------------

  private def testUdf(funcPrefix: String): Unit = {
    val sinkDDL =
      s"""
         |create table sinkT(
         |  a bigint
         |) with (
         |  'connector' = 'COLLECTION',
         |  'is-bounded' = '$isStreamingMode'
         |)
      """.stripMargin
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(s"insert into sinkT select ${funcPrefix}myfunc(cast(1 as bigint))").await()
    assertEquals(Seq(toRow(2L)), TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testUdfWithFullIdentifier(): Unit = {
    testUdf("default_catalog.default_database.")
  }

  @Test
  def testUdfWithDatabase(): Unit = {
    testUdf("default_database.")
  }

  @Test
  def testUdfWithNon(): Unit = {
    testUdf("")
  }

  @Test
  def testUdfWithWrongCatalog(): Unit = {
    Assertions

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

      tableEnv.executeSql(ddl1)
      assert(tableEnv.listTables().sameElements(Array[String]("t1")))
      tableEnv.executeSql("DROP TABLE IF EXISTS catalog1.database1.t1")
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
          |  a bigint not null,
          |  b bigint,
          |  c varchar
          |) with (
          |  'connector' = 'COLLECTION',
          |  'k1' = 'v1'
          |)
      """.stripMargin
      tableEnv.executeSql(ddl1)
      tableEnv.executeSql("alter table t1 rename to t2")
      assert(tableEnv.listTables().sameElements(Array[String]("t2")))
      tableEnv.executeSql("alter table t2 set ('k1' = 'a', 'k2' = 'b')")
      val expectedProperties = new util.HashMap[String, String]()
      expectedProperties.put("connector", "COLLECTION")
      expectedProperties.put("k1", "a")
      expectedProperties.put("k2", "b")
      val properties = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
        .getTable(new ObjectPath(tableEnv.getCurrentDatabase, "t2"))
        .getProperties
      assertEquals(expectedProperties, properties)
      val currentCatalog = tableEnv.getCurrentCatalog
      val currentDB = tableEnv.getCurrentDatabase
      tableEnv.executeSql("alter table t2 add constraint ct1 primary key(a) not enforced")
      val tableSchema1 = tableEnv.getCatalog(currentCatalog).get()
        .getTable(ObjectPath.fromString(s"${currentDB}.t2"))
        .getSchema
      assert(tableSchema1.getPrimaryKey.isPresent)
      assertEquals("CONSTRAINT ct1 PRIMARY KEY (a)",
        tableSchema1.getPrimaryKey.get().asSummaryString())
      tableEnv.executeSql("alter table t2 drop constraint ct1")
      val tableSchema2 = tableEnv.getCatalog(currentCatalog).get()
        .getTable(ObjectPath.fromString(s"${currentDB}.t2"))
        .getSchema
      assertEquals(false, tableSchema2.getPrimaryKey.isPresent)
    }

    @Test
    def testUseCatalogAndShowCurrentCatalog(): Unit = {
      tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
      tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
      tableEnv.executeSql("use catalog cat1")
      assertEquals("cat1", tableEnv.getCurrentCatalog)
      tableEnv.executeSql("use catalog cat2")
      assertEquals("cat2", tableEnv.getCurrentCatalog)
      assertEquals("+I[cat2]", tableEnv.executeSql("show current catalog").collect().next().toString)
    }

    @Test
    def testUseDatabaseAndShowCurrentDatabase(): Unit = {
      val catalog = new GenericInMemoryCatalog("cat1")
      tableEnv.registerCatalog("cat1", catalog)
      val catalogDB1 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db1")
      val catalogDB2 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db2")
      catalog.createDatabase("db1", catalogDB1, true)
      catalog.createDatabase("db2", catalogDB2, true)
      tableEnv.executeSql("use cat1.db1")
      assertEquals("db1", tableEnv.getCurrentDatabase)
      var currentDatabase = tableEnv.executeSql("show current database").collect().next().toString
      assertEquals("+I[db1]", currentDatabase)
      tableEnv.executeSql("use db2")
      assertEquals("db2", tableEnv.getCurrentDatabase)
      currentDatabase = tableEnv.executeSql("show current database").collect().next().toString
      assertEquals("+I[db2]", currentDatabase)
    }

    @Test
    def testCreateDatabase(): Unit = {
      tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
      tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("default"))
      tableEnv.executeSql("use catalog cat1")
      tableEnv.executeSql("create database db1 ")
      tableEnv.executeSql("create database if not exists db1 ")
      try {
        tableEnv.executeSql("create database db1 ")
        fail("ValidationException expected")
      }
      );
    }

    catch
    {
      case _: ValidationException => //ignore
    }
    tableEnv.executeSql("create database cat2.db1 comment 'test_comment'" +
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
  def testDropDatabase(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.executeSql("use catalog cat1")
    tableEnv.executeSql("create database db1")
    tableEnv.executeSql("drop database db1")
    tableEnv.executeSql("drop database if exists db1")
    try {
      tableEnv.executeSql("drop database db1")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.executeSql("create database db1")
    tableEnv.executeSql("use db1")
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
    tableEnv.executeSql(ddl1)
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
    tableEnv.executeSql(ddl2)
    try {
      tableEnv.executeSql("drop database db1")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.executeSql("drop database db1 cascade")
  }

  @Test
  def testAlterDatabase(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.executeSql("use catalog cat1")
    tableEnv.executeSql("create database db1 comment 'db1_comment' with ('k1' = 'v1')")
    tableEnv.executeSql("alter database db1 set ('k1' = 'a', 'k2' = 'b')")
    val database = tableEnv.getCatalog("cat1").get().getDatabase("db1")
    assertEquals("db1_comment", database.getComment)
    assertEquals(2, database.getProperties.size())
    val expectedProperty = new util.HashMap[String, String]()
    expectedProperty.put("k1", "a")
    expectedProperty.put("k2", "b")
    assertEquals(expectedProperty, database.getProperties)
  }
}

object CatalogTableITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
