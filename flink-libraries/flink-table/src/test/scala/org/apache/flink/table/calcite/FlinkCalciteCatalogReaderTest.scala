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

package org.apache.flink.table.calcite

import java.util.Collections

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.schema.{SchemaPlus, Table}
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, FlinkTable}
import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.`type`.RelDataType
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Mockito.{mock, when}

class FlinkCalciteCatalogReaderTest {

  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  private val tableMockName = "ts"
  private var rootSchemaPlus: SchemaPlus = _
  private var catalogReader: FlinkCalciteCatalogReader = _

  @Before
  def setup(): Unit = {
    rootSchemaPlus = CalciteSchema.createRootSchema(true, false).plus()
    catalogReader = new FlinkCalciteCatalogReader(
      CalciteSchema.from(rootSchemaPlus),
      false,
      Collections.emptyList(),
      typeFactory)
  }

  @Test
  def testGetFlinkTable(): Unit = {
    val flinkTableMock = mock(classOf[FlinkTable[Any]])
    when(flinkTableMock.getRowType(typeFactory)).thenReturn(mock(classOf[RelDataType]))
    rootSchemaPlus.add(tableMockName, flinkTableMock)
    val resultTable = catalogReader.getTable(ImmutableList.of(tableMockName))
    assertTrue(resultTable.isInstanceOf[FlinkRelOptTable])
  }

  @Test
  def testGetNonFlinkTable(): Unit = {
    val nonFlinkTableMock = mock(classOf[Table])
    when(nonFlinkTableMock.getRowType(typeFactory)).thenReturn(mock(classOf[RelDataType]))
    rootSchemaPlus.add(tableMockName, nonFlinkTableMock)
    val resultTable = catalogReader.getTable(ImmutableList.of(tableMockName))
    assertFalse(resultTable.isInstanceOf[FlinkRelOptTable])
  }

}
