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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{ConnectorDescriptor, Schema}
import org.junit.Assert._
import org.junit.{Before, Test}

class InMemoryExternalCatalogTest {

  private val databaseName = "db1"

  private var catalog: InMemoryExternalCatalog = _

  @Before
  def setUp(): Unit = {
    catalog = new InMemoryExternalCatalog(databaseName)
  }

  @Test
  def testCreateTable(): Unit = {
    assertTrue(catalog.listTables().isEmpty)
    catalog.createTable("t1", createTableInstance(), ignoreIfExists = false)
    val tables = catalog.listTables()
    assertEquals(1, tables.size())
    assertEquals("t1", tables.get(0))
  }

  @Test(expected = classOf[TableAlreadyExistException])
  def testCreateExistedTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(tableName, createTableInstance(), ignoreIfExists = false)
    catalog.createTable(tableName, createTableInstance(), ignoreIfExists = false)
  }

  @Test
  def testGetTable(): Unit = {
    val originTable = createTableInstance()
    catalog.createTable("t1", originTable, ignoreIfExists = false)
    assertEquals(catalog.getTable("t1"), originTable)
  }

  @Test(expected = classOf[TableNotExistException])
  def testGetNotExistTable(): Unit = {
    catalog.getTable("nonexisted")
  }

  @Test
  def testAlterTable(): Unit = {
    val tableName = "t1"
    val table = createTableInstance()
    catalog.createTable(tableName, table, ignoreIfExists = false)
    assertEquals(catalog.getTable(tableName), table)
    val newTable = createTableInstance(Array("number"), Array(Types.INT))
    catalog.alterTable(tableName, newTable, ignoreIfNotExists = false)
    val currentTable = catalog.getTable(tableName)
    // validate the table is really replaced after alter table
    assertNotEquals(table, currentTable)
    assertEquals(newTable, currentTable)
  }

  @Test(expected = classOf[TableNotExistException])
  def testAlterNotExistTable(): Unit = {
    catalog.alterTable("nonexisted", createTableInstance(), ignoreIfNotExists = false)
  }

  @Test
  def testDropTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(tableName, createTableInstance(), ignoreIfExists = false)
    assertTrue(catalog.listTables().contains(tableName))
    catalog.dropTable(tableName, ignoreIfNotExists = false)
    assertFalse(catalog.listTables().contains(tableName))
  }

  @Test(expected = classOf[TableNotExistException])
  def testDropNotExistTable(): Unit = {
    catalog.dropTable("nonexisted", ignoreIfNotExists = false)
  }

  @Test(expected = classOf[CatalogNotExistException])
  def testGetNotExistDatabase(): Unit = {
    catalog.getSubCatalog("notexistedDb")
  }

  @Test
  def testCreateDatabase(): Unit = {
    catalog.createSubCatalog("db2", new InMemoryExternalCatalog("db2"), ignoreIfExists = false)
    assertEquals(1, catalog.listSubCatalogs().size)
  }

  @Test(expected = classOf[CatalogAlreadyExistException])
  def testCreateExistedDatabase(): Unit = {
    catalog.createSubCatalog("existed", new InMemoryExternalCatalog("existed"),
      ignoreIfExists = false)

    assertNotNull(catalog.getSubCatalog("existed"))
    val databases = catalog.listSubCatalogs()
    assertEquals(1, databases.size())
    assertEquals("existed", databases.get(0))

    catalog.createSubCatalog("existed", new InMemoryExternalCatalog("existed"),
      ignoreIfExists = false)
  }

  @Test
  def testNestedCatalog(): Unit = {
    val sub = new InMemoryExternalCatalog("sub")
    val sub1 = new InMemoryExternalCatalog("sub1")
    catalog.createSubCatalog("sub", sub, ignoreIfExists = false)
    sub.createSubCatalog("sub1", sub1, ignoreIfExists = false)
    sub1.createTable("table", createTableInstance(), ignoreIfExists = false)
    val tables = catalog.getSubCatalog("sub").getSubCatalog("sub1").listTables()
    assertEquals(1, tables.size())
    assertEquals("table", tables.get(0))
  }

  private def createTableInstance(): ExternalCatalogTable = {
    val connDesc = new TestConnectorDesc
    val schemaDesc = new Schema()
      .field("first", BasicTypeInfo.STRING_TYPE_INFO)
      .field("second", BasicTypeInfo.INT_TYPE_INFO)
    new ExternalCatalogTableBuilder(connDesc)
      .withSchema(schemaDesc)
      .asTableSource()
  }

  private def createTableInstance(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): ExternalCatalogTable = {
    val connDesc = new TestConnectorDesc
    val schemaDesc = new Schema()
    fieldNames.zipWithIndex.foreach { case (fieldName, index) =>
      schemaDesc.field(fieldName, fieldTypes(index))
    }
    new ExternalCatalogTableBuilder(connDesc)
      .withSchema(schemaDesc)
      .asTableSource()
  }

  class TestConnectorDesc extends ConnectorDescriptor("test", 1, false) {
    override protected def toConnectorProperties: _root_.java.util.Map[String, String] = {
      _root_.java.util.Collections.emptyMap()
    }
  }
}
