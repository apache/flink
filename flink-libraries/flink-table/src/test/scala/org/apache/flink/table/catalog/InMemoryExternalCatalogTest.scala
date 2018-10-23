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

import java.util.{LinkedHashMap, LinkedHashSet}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{ConnectorDescriptor, DescriptorProperties, Schema, Statistics}
import org.junit.{Before, Test}
import org.junit.Assert._

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

  @Test
  def testCreatePartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    assertTrue(catalog.listPartitions(tableName).isEmpty)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
    val partitionSpecs = catalog.listPartitions(tableName)
    assertEquals(partitionSpecs.size(), 1)
    assertEquals(partitionSpecs.get(0), newPartitionSpec)
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testCreatePartitionOnUnPartitionedTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, ignoreIfExists = false)
  }

  @Test(expected = classOf[PartitionAlreadyExistException])
  def testCreateExistedPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
    val newPartitionSpec1 = new LinkedHashMap[String, String]()
    newPartitionSpec1.put("hour", "12")
    newPartitionSpec1.put("ds", "2016-02-01")
    val newPartition1 = createPartition(newPartitionSpec1)
    catalog.createPartition(tableName, newPartition1, false)
  }

  @Test(expected = classOf[TableNotExistException])
  def testCreatePartitionOnNotExistTable(): Unit = {
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition("notexistedTb", newPartition, false)
  }

  @Test
  def testGetPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, ignoreIfExists = false)
    assertEquals(catalog.getPartition(tableName, newPartitionSpec), newPartition)
  }

  @Test(expected = classOf[PartitionNotExistException])
  def testGetNotExistPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    assertEquals(catalog.getPartition(tableName, newPartitionSpec), newPartition)
  }

  @Test
  def testDropPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, ignoreIfExists = false)
    assertTrue(catalog.listPartitions(tableName).contains(newPartitionSpec))
    catalog.dropPartition(tableName, newPartitionSpec, ignoreIfNotExists = false)
    assertFalse(catalog.listPartitions(tableName).contains(newPartitionSpec))
  }

  @Test(expected = classOf[PartitionNotExistException])
  def testDropNotExistPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val partitionSpec = new LinkedHashMap[String, String]()
    partitionSpec.put("hour", "12")
    partitionSpec.put("ds", "2016-02-01")
    catalog.dropPartition(tableName, partitionSpec, ignoreIfNotExists = false)
  }

  @Test
  def testListPartitionSpec(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    assertTrue(catalog.listPartitions(tableName).isEmpty)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
    val partitionSpecs = catalog.listPartitions(tableName)
    assertEquals(partitionSpecs.size(), 1)
    assertEquals(partitionSpecs.get(0), newPartitionSpec)
  }

  @Test
  def testAlterPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new LinkedHashMap[String, String]()
    newPartitionSpec.put("hour", "12")
    newPartitionSpec.put("ds", "2016-02-01")
    val newPartition = createPartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
    val updatedPartition = createPartition(newPartitionSpec, Statistics().rowCount(10L))
    catalog.alterPartition(tableName, updatedPartition, false)
    val currentPartition = catalog.getPartition(tableName, newPartitionSpec)
    assertEquals(currentPartition, updatedPartition)
    assertNotEquals(currentPartition, newPartition)
  }

  private def createTableInstance(): ExternalCatalogTable = {
    val connDesc = new TestConnectorDesc
    val schemaDesc = new Schema()
      .field("first", BasicTypeInfo.STRING_TYPE_INFO)
      .field("second", BasicTypeInfo.INT_TYPE_INFO)
    ExternalCatalogTable.builder(connDesc)
      .withSchema(schemaDesc)
      .asTableSource()
  }

  private def createPartitionedTableInstance(): ExternalCatalogTable = {
    val connDesc = new TestConnectorDesc
    val schemaDesc = new Schema()
                     .field("first", BasicTypeInfo.STRING_TYPE_INFO)
                     .field("second", BasicTypeInfo.INT_TYPE_INFO)
    val partitionColumns = new LinkedHashSet[String]()
    partitionColumns.add("ds")
    partitionColumns.add("hour")
    ExternalCatalogTable.builder(connDesc)
    .withSchema(schemaDesc)
    .withPartitionColumnNames(partitionColumns)
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

  private def createPartition(
    partitionSpec: LinkedHashMap[String, String],
    statistics: Statistics = Statistics()): ExternalCatalogPartition = {
    val connDesc = new TestConnectorDesc
    ExternalCatalogPartition.builder(connDesc)
      .withPartitionSpec(partitionSpec)
      .withStatistics(statistics)
      .build()
  }

  class TestConnectorDesc extends ConnectorDescriptor("test", version = 1, formatNeeded = false) {
    override protected def addConnectorProperties(properties: DescriptorProperties): Unit = {
    }
  }

}
