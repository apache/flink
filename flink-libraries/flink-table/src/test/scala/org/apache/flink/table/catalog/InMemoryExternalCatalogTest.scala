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

import java.util.{LinkedHashMap => JLinkedHashMap, LinkedHashSet => JLinkedHashSet}

import com.google.common.collect.{ImmutableMap, ImmutableSet}
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
  def testCreatePartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    assertTrue(catalog.listPartitions(databaseName, tableName).isEmpty)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("hour", "12", "ds", "2016-02-01"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, tableName, newPartition, false)
    val partitionSpecs = catalog.listPartitions(databaseName, tableName)
    assertEquals(partitionSpecs.size(), 1)
    assertEquals(partitionSpecs.get(0), newPartitionSpec)
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testCreatePartitionOnUnPartitionedTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createNonPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, tableName, newPartition, false)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCreateInvalidPartitionSpec(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "h", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, tableName, newPartition, false)
  }

  @Test(expected = classOf[PartitionAlreadyExistException])
  def testCreateExistedPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, tableName, newPartition, false)
    val newPartitionSpec1 = new JLinkedHashMap[String, String](
      ImmutableMap.of("hour", "12", "ds", "2016-02-01"))
    val newPartition1 = ExternalCatalogTablePartition(newPartitionSpec1)
    catalog.createPartition(databaseName, tableName, newPartition1, false)
  }

  @Test(expected = classOf[TableNotExistException])
  def testCreatePartitionOnNotExistTable(): Unit = {
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, "notexistedTb", newPartition, false)
  }

  @Test(expected = classOf[DatabaseNotExistException])
  def testCreatePartitionOnNotExistDatabase(): Unit = {
    val tableName = "t1"
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition("notexistedDb", tableName, newPartition, false)
  }

  @Test
  def testGetPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, tableName, newPartition, false)
    assertEquals(catalog.getPartition(databaseName, tableName, newPartitionSpec), newPartition)
  }

  @Test(expected = classOf[PartitionNotExistException])
  def testGetNotExistPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    assertEquals(catalog.getPartition(databaseName, tableName, newPartitionSpec), newPartition)
  }

  @Test
  def testDropPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, tableName, newPartition, false)
    assertTrue(catalog.listPartitions(databaseName, tableName).contains(newPartitionSpec))
    catalog.dropPartition(databaseName, tableName, newPartitionSpec, false)
    assertFalse(catalog.listPartitions(databaseName, tableName).contains(newPartitionSpec))
  }

  @Test(expected = classOf[PartitionNotExistException])
  def testDropNotExistPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val partitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    catalog.dropPartition(databaseName, tableName, partitionSpec, false)
  }

  @Test
  def testListPartitionSpec(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    assertTrue(catalog.listPartitions(databaseName, tableName).isEmpty)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(databaseName, tableName, newPartition, false)
    val partitionSpecs = catalog.listPartitions(databaseName, tableName)
    assertEquals(partitionSpecs.size(), 1)
    assertEquals(partitionSpecs.get(0), newPartitionSpec)
  }

  @Test
  def testAlterPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      createPartitionedTableInstance(databaseName, tableName),
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(
      newPartitionSpec,
      properties = ImmutableMap.of("location" , "/tmp/ds=2016-02-01/hour=12"))
    catalog.createPartition(databaseName, tableName, newPartition, false)
    val updatedPartition = ExternalCatalogTablePartition(
      newPartitionSpec,
      properties = ImmutableMap.of("location",  "/tmp1/ds=2016-02-01/hour=12"))
    catalog.alterPartition(databaseName, tableName, updatedPartition, false)
    val currentPartition = catalog.getPartition(databaseName, tableName, newPartitionSpec)
    assertEquals(currentPartition, updatedPartition)
    assertNotEquals(currentPartition, newPartition)
  }

  @Test
  def testCreateTable(): Unit = {
    assertTrue(catalog.listTables(databaseName).isEmpty)
    catalog.createTable(
      createNonPartitionedTableInstance(databaseName, "t1"),
      ignoreIfExists = false)
    val tables = catalog.listTables(databaseName)
    assertEquals(1, tables.size())
    assertEquals("t1", tables.get(0))
  }

  @Test(expected = classOf[TableAlreadyExistException])
  def testCreateExistedTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(createNonPartitionedTableInstance(databaseName, tableName), false)
    catalog.createTable(createNonPartitionedTableInstance(databaseName, tableName), false)
  }

  @Test
  def testGetTable(): Unit = {
    val originTable = createNonPartitionedTableInstance(databaseName, "t1")
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
    val table = createNonPartitionedTableInstance(databaseName, tableName)
    catalog.createTable(table, false)
    assertEquals(catalog.getTable(databaseName, tableName), table)
    val newTable = createNonPartitionedTableInstance(databaseName, tableName)
    catalog.alterTable(newTable, false)
    val currentTable = catalog.getTable(databaseName, tableName)
    // validate the table is really replaced after alter table
    assertNotEquals(table, currentTable)
    assertEquals(newTable, currentTable)
  }

  @Test(expected = classOf[TableNotExistException])
  def testAlterNotExistTable(): Unit = {
    catalog.alterTable(createNonPartitionedTableInstance(databaseName, "t1"), false)
  }

  @Test
  def testDropTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(createNonPartitionedTableInstance(databaseName, tableName), false)
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

  private def createNonPartitionedTableInstance(
      dbName: String,
      tableName: String): ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO
      ))
    ExternalCatalogTable(
      TableIdentifier(dbName, tableName),
      "csv",
      schema)
  }

  private def createPartitionedTableInstance(
      dbName: String,
      tableName: String): ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO
      ))
    ExternalCatalogTable(
      TableIdentifier(dbName, tableName),
      "hive",
      schema,
      partitionColumnNames = new JLinkedHashSet(ImmutableSet.of("ds","hour")),
      isPartitioned = true
    )
  }

}
