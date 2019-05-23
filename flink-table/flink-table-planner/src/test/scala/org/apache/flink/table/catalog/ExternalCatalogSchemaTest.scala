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

import java.util.{Collections, Properties}

import com.google.common.collect.Lists
import org.apache.calcite.config.{CalciteConnectionConfigImpl, CalciteConnectionProperty}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.validate.SqlMonikerType
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.plan.schema.{TableSourceSinkTable, TableSourceTable}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.utils.TableTestBase
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class ExternalCatalogSchemaTest extends TableTestBase {

  private val schemaName: String = "test"
  private var externalCatalogSchema: SchemaPlus = _
  private var calciteCatalogReader: CalciteCatalogReader = _
  private val db = "db1"
  private val tb = "tb1"

  @Before
  def setUp(): Unit = {
    val rootSchemaPlus: SchemaPlus = CalciteSchema.createRootSchema(true, false).plus()
    val catalog = CommonTestData.getInMemoryTestCatalog(isStreaming = true)
    ExternalCatalogSchema.registerCatalog(
      streamTestUtil().tableEnv, rootSchemaPlus, schemaName, catalog)
    externalCatalogSchema = rootSchemaPlus.getSubSchema("schemaName")
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
    val prop = new Properties()
    prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName, "false")
    val calciteConnConfig = new CalciteConnectionConfigImpl(prop)
    calciteCatalogReader = new CalciteCatalogReader(
      CalciteSchema.from(rootSchemaPlus),
      Collections.emptyList(),
      typeFactory,
      calciteConnConfig
    )
  }

  @Test
  def testGetSubSchema(): Unit = {
    val allSchemaObjectNames = calciteCatalogReader
        .getAllSchemaObjectNames(Lists.newArrayList(schemaName))
    val subSchemas = allSchemaObjectNames.asScala
        .filter(_.getType.equals(SqlMonikerType.SCHEMA))
        .map(_.getFullyQualifiedNames.asScala.toList).toSet
    assertTrue(Set(List(schemaName), List(schemaName, "db1"),
      List(schemaName, "db2"), List(schemaName, "db3")) == subSchemas)
  }

  @Test
  def testGetTable(): Unit = {
    val relOptTable = calciteCatalogReader.getTable(Lists.newArrayList(schemaName, db, tb))
    assertNotNull(relOptTable)
    val tableSourceSinkTable = relOptTable.unwrap(classOf[TableSourceSinkTable[_, _]])
    tableSourceSinkTable.tableSourceTable match {
      case Some(tst: TableSourceTable[_]) =>
        assertTrue(tst.tableSource.isInstanceOf[CsvTableSource])
      case _ =>
        fail("unexpected table type!")
    }
  }

  @Test
  def testGetNotExistTable(): Unit = {
    val relOptTable = calciteCatalogReader.getTable(
      Lists.newArrayList(schemaName, db, "nonexist-tb"))
    assertNull(relOptTable)
  }
}
