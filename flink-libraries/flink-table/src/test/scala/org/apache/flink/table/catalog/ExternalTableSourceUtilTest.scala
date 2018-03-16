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

import java.util.{Collections => JCollections, Set => JSet}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.plan.schema.StreamTableSourceTable
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.utils.MockTableEnvironment
import org.apache.flink.types.Row
import org.junit.Assert.assertTrue
import org.junit.{Before, Test}

class ExternalTableSourceUtilTest {

  @Before
  def setUp() : Unit = {
    ExternalTableSourceUtil.injectTableSourceConverter("mock", classOf[MockTableSourceConverter])
  }

  @Test
  def testExternalStreamTable() = {
    val schema = new TableSchema(Array("foo"), Array(BasicTypeInfo.INT_TYPE_INFO))
    val table = ExternalCatalogTable("mock", schema)
    val tableSource = ExternalTableSourceUtil.fromExternalCatalogTable(
      new MockTableEnvironment, table)
    assertTrue(tableSource.isInstanceOf[StreamTableSourceTable[_]])
  }
}

class MockTableSourceConverter extends TableSourceConverter[StreamTableSource[Row]] {
  override def requiredProperties: JSet[String] = JCollections.emptySet()
  override def fromExternalCatalogTable(externalCatalogTable: ExternalCatalogTable)
    : StreamTableSource[Row] = {
    new StreamTableSource[Row] {
      override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] =
        throw new UnsupportedOperationException

      override def getReturnType: TypeInformation[Row] = {
        val schema = externalCatalogTable.schema
        Types.ROW(schema.getColumnNames, schema.getTypes)
      }

      override def getTableSchema: TableSchema = externalCatalogTable.schema

    }
  }
}
