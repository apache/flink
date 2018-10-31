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
import org.apache.flink.table.functions.UserDefinedFunction
import org.junit.Assert._
import org.junit.{Before, Test}

class InMemoryExternalCatalogTest {

  private val databaseName = "db1"

  private var catalog: InMemoryExternalCatalog = _

  @Before
  def setUp(): Unit = {
    catalog = new InMemoryExternalCatalog(databaseName)
  }

  // ------ Table ------

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

  // ------ SubCatalog ------

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

  // ------ View ------

  @Test
  def testCreateView(): Unit = {
    assertTrue(catalog.listViews().isEmpty)
    catalog.createView("v1", createViewInstance(), ignoreIfExists = false)
    val views = catalog.listViews()
    assertEquals(1, views.size())
    assertEquals("v1", views.get(0))
  }

  @Test(expected = classOf[ViewAlreadyExistException])
  def testCreateExistedView(): Unit = {
    val viewName = "v1"
    catalog.createView(viewName, createViewInstance(), ignoreIfExists = false)
    catalog.createView(viewName, createViewInstance(), ignoreIfExists = false)
  }

  @Test
  def testGetView(): Unit = {
    val originView = createViewInstance()
    catalog.createView("v1", originView, ignoreIfExists = false)
    assertEquals(catalog.getView("v1"), originView)
  }

  @Test(expected = classOf[ViewNotExistException])
  def testGetNotExistView(): Unit = {
    catalog.getView("nonexisted")
  }

  @Test
  def testAlterView(): Unit = {
    val viewName = "v1"
    val view = createViewInstance()
    catalog.createView(viewName, view, ignoreIfExists = false)
    assertEquals(catalog.getView(viewName), view)

    val newView = createNewViewInstance()
    catalog.alterView(viewName, newView, ignoreIfNotExists = false)
    val currentView = catalog.getView(viewName)
    // validate the view is really replaced after alter view
    assertNotEquals(view, currentView)
    assertEquals(newView, currentView)
  }

  @Test(expected = classOf[ViewNotExistException])
  def testAlterNotExistView(): Unit = {
    catalog.alterView("nonexisted", createViewInstance(), ignoreIfNotExists = false)
  }

  @Test
  def testDropView(): Unit = {
    val viewName = "v1"
    catalog.createView(viewName, createViewInstance(), ignoreIfExists = false)
    assertTrue(catalog.listViews().contains(viewName))
    catalog.dropView(viewName, ignoreIfNotExists = false)
    assertFalse(catalog.listViews().contains(viewName))
  }

  @Test(expected = classOf[ViewNotExistException])
  def testDropNotExistView(): Unit = {
    catalog.dropView("nonexisted", ignoreIfNotExists = false)
  }

  // ------ UDF ------

  @Test
  def testCreateFunction(): Unit = {
    assertTrue(catalog.listFunctions().isEmpty)
    catalog.createFunction("f1", createFunctionInstance(), ignoreIfExists = false)
    val functions = catalog.listFunctions()
    assertEquals(1, functions.size())
    assertEquals("f1", functions.get(0))
  }

  @Test(expected = classOf[FunctionAlreadyExistException])
  def testCreateExistedFunction(): Unit = {
    val functionName = "f1"
    catalog.createFunction(functionName, createFunctionInstance(), ignoreIfExists = false)
    catalog.createFunction(functionName, createFunctionInstance(), ignoreIfExists = false)
  }

  @Test
  def testGetFunction(): Unit = {
    val originFunction = createFunctionInstance()
    catalog.createFunction("f1", originFunction, ignoreIfExists = false)
    assertEquals(catalog.getFunction("f1"), originFunction)
  }

  @Test(expected = classOf[FunctionNotExistException])
  def testGetNotExistFunction(): Unit = {
    catalog.getFunction("nonexisted")
  }

  @Test
  def testAlterFunction(): Unit = {
    val functionName = "f1"
    val function = createFunctionInstance()
    catalog.createFunction(functionName, function, ignoreIfExists = false)
    assertEquals(catalog.getFunction(functionName), function)

    val newFunction = createNewFunctionInstance()
    catalog.alterFunction(functionName, newFunction, ignoreIfNotExists = false)
    val currentFunction = catalog.getFunction(functionName)
    // validate the function is really replaced after alter view
    assertNotEquals(function, currentFunction)
    assertEquals(newFunction, currentFunction)
  }

  @Test(expected = classOf[FunctionNotExistException])
  def testAlterNotExistFunction(): Unit = {
    catalog.alterFunction("nonexisted", createFunctionInstance(), ignoreIfNotExists = false)
  }

  @Test
  def testDropFunction(): Unit = {
    val functionName = "f1"
    catalog.createFunction(functionName, createFunctionInstance(), ignoreIfExists = false)
    assertTrue(catalog.listFunctions().contains(functionName))
    catalog.dropFunction(functionName, ignoreIfNotExists = false)
    assertFalse(catalog.listFunctions().contains(functionName))
  }

  @Test(expected = classOf[FunctionNotExistException])
  def testDropNotExistFunction(): Unit = {
    catalog.dropFunction("nonexisted", ignoreIfNotExists = false)
  }

  // ------ Utils ------

  private def createTableInstance(): ExternalCatalogTable = {
    val connDesc = new TestConnectorDesc
    val schemaDesc = new Schema()
      .field("first", BasicTypeInfo.STRING_TYPE_INFO)
      .field("second", BasicTypeInfo.INT_TYPE_INFO)
    ExternalCatalogTable.builder(connDesc)
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
    ExternalCatalogTable.builder(connDesc)
      .withSchema(schemaDesc)
      .asTableSource()
  }

  private def createViewInstance(): String = {
    "select a from b"
  }

  private def createNewViewInstance(): String = {
    "select c from d"
  }

  private def createFunctionInstance(): UserDefinedFunction = {
    new TestFunction1
  }

  private def createNewFunctionInstance(): UserDefinedFunction = {
    new TestFunction2
  }

  class TestConnectorDesc extends ConnectorDescriptor("test", 1, false) {
    override protected def toConnectorProperties: _root_.java.util.Map[String, String] = {
      _root_.java.util.Collections.emptyMap()
    }
  }

  class TestFunction1 extends UserDefinedFunction {

  }

  class TestFunction2 extends UserDefinedFunction {

  }
}
