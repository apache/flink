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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api._
import org.junit.{Before, Test}
import org.junit.Assert._

class InMemoryExternalCatalogTest {

  private val databaseName = "db1"

  private var catalog: InMemoryExternalCatalog = _

  @Before
  def setUp(): Unit = {
    catalog = new InMemoryExternalCatalog()
    catalog.createDatabase(ExternalCatalogDatabase(databaseName), ignoreIfExists = false)
  }

  @Test
  def testCreateTable(): Unit = {
    assertTrue(catalog.listTables(databaseName).isEmpty)
    catalog.createTable(createTableInstance(databaseName, "t1"), ignoreIfExists = false)
    val tables = catalog.listTables(databaseName)
    assertEquals(1, tables.size())
    assertEquals("t1", tables.get(0))
  }

  @Test(expected = classOf[TableAlreadyExistException])
  def testCreateExistedTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(createTableInstance(databaseName, tableName), false)
    catalog.createTable(createTableInstance(databaseName, tableName), false)
  }

  @Test
  def testGetTable(): Unit = {
    val originTable = createTableInstance(databaseName, "t1")
    catalog.createTable(originTable, false)
    assertEquals(catalog.getTable(databaseName, "t1"), originTable)
  }

  @Test(expected = classOf[DatabaseNotExistException])
  def testGetTableUnderNotExistDatabaseName(): Unit = {
    catalog.getTable("notexistedDb", "t1")
  }

  @Test(expected = classOf[TableNotExistException])
  def testGetNotExistTable(): Unit = {
    catalog.getTable(databaseName, "t1")
  }

  @Test
  def testAlterTable(): Unit = {
    val tableName = "t1"
    val table = createTableInstance(databaseName, tableName)
    catalog.createTable(table, false)
    assertEquals(catalog.getTable(databaseName, tableName), table)
    val newTable = createTableInstance(databaseName, tableName)
    catalog.alterTable(newTable, false)
    val currentTable = catalog.getTable(databaseName, tableName)
    // validate the table is really replaced after alter table
    assertNotEquals(table, currentTable)
    assertEquals(newTable, currentTable)
  }

  @Test(expected = classOf[TableNotExistException])
  def testAlterNotExistTable(): Unit = {
    catalog.alterTable(createTableInstance(databaseName, "t1"), false)
  }

  @Test
  def testDropTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(createTableInstance(databaseName, tableName), false)
    assertTrue(catalog.listTables(databaseName).contains(tableName))
    catalog.dropTable(databaseName, tableName, false)
    assertFalse(catalog.listTables(databaseName).contains(tableName))
  }

  @Test(expected = classOf[TableNotExistException])
  def testDropNotExistTable(): Unit = {
    catalog.dropTable(databaseName, "t1", false)
  }

  @Test
  def testListDatabases(): Unit = {
    val databases = catalog.listDatabases()
    assertEquals(1, databases.size())
    assertEquals(databaseName, databases.get(0))
  }

  @Test
  def testGetDatabase(): Unit = {
    assertNotNull(catalog.getDatabase(databaseName))
  }

  @Test(expected = classOf[DatabaseNotExistException])
  def testGetNotExistDatabase(): Unit = {
    catalog.getDatabase("notexistedDb")
  }

  @Test
  def testCreateDatabase(): Unit = {
    val originDatabasesNum = catalog.listDatabases().size
    catalog.createDatabase(ExternalCatalogDatabase("db2"), false)
    assertEquals(catalog.listDatabases().size, originDatabasesNum + 1)
  }

  @Test(expected = classOf[DatabaseAlreadyExistException])
  def testCreateExistedDatabase(): Unit = {
    catalog.createDatabase(ExternalCatalogDatabase(databaseName), false)
  }

  private def createTableInstance(dbName: String, tableName: String): ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO
      )
    )
    ExternalCatalogTable(
      TableIdentifier(dbName, tableName),
      "csv",
      schema)
  }
}
