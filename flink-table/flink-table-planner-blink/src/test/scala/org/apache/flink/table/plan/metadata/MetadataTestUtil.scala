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

package org.apache.flink.table.plan.metadata

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.calcite.{FlinkCalciteCatalogReader, FlinkTypeSystem}
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.table.plan.stats.{ColumnStats, FlinkStatistic, TableStats}
import org.apache.flink.types.Row

import org.apache.calcite.config.{CalciteConnectionConfigImpl, CalciteConnectionProperty, Lex}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{FrameworkConfig, Frameworks}
import org.mockito.Mockito.{mock, when}

import java.util.{Collections, Properties}

import scala.collection.JavaConversions._

object MetadataTestUtil {

  def createFrameworkConfig(defaultSchema: SchemaPlus): FrameworkConfig = {
    val sqlParserConfig = SqlParser
      .configBuilder()
      .setLex(Lex.JAVA)
      .build()
    Frameworks.newConfigBuilder
      .defaultSchema(defaultSchema)
      .parserConfig(sqlParserConfig)
      .costFactory(new FlinkCostFactory)
      .typeSystem(new FlinkTypeSystem)
      .operatorTable(new SqlStdOperatorTable)
      .build
  }

  def createCatalogReader(
      rootSchema: SchemaPlus,
      typeFactory: RelDataTypeFactory): FlinkCalciteCatalogReader = {
    val prop = new Properties()
    prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName, "false")
    val calciteConnConfig = new CalciteConnectionConfigImpl(prop)
    new FlinkCalciteCatalogReader(
      CalciteSchema.from(rootSchema),
      Collections.emptyList(),
      typeFactory,
      calciteConnConfig)
  }

  def initRootSchema(): SchemaPlus = {
    val rootSchema = CalciteSchema.createRootSchema(true, false).plus()
    rootSchema.add("student", createStudentTable())
    rootSchema.add("emp", createEmpTable())
    rootSchema
  }

  private def createStudentTable(): DataStreamTable[Row] = {
    val schema = new TableSchema(
      Array("id", "name", "score", "age", "height", "sex"),
      Array(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO))

    val colStatsMap = Map[String, ColumnStats](
      "id" -> new ColumnStats(50L, 0L, 4D, 4, 60, 0),
      "name" -> new ColumnStats(48L, 0L, 7.2, 12, null, null),
      "score" -> new ColumnStats(20L, 6L, 8D, 8, 4.8D, 2.7D),
      "age" -> new ColumnStats(7L, 0L, 4D, 4, 18, 12),
      "height" -> new ColumnStats(35L, 0L, 8D, 8, 172.1D, 161.0D),
      "sex" -> new ColumnStats(2L, 0L, 1D, 1, null, null))

    val tableStats = new TableStats(50L, colStatsMap)
    getDataStreamTable(schema, new FlinkStatistic(tableStats))
  }

  private def createEmpTable(): DataStreamTable[Row] = {
    val schema = new TableSchema(
      Array("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno"),
      Array(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        SqlTimeTypeInfo.DATE,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO))

    getDataStreamTable(schema, new FlinkStatistic(null))
  }

  private def getDataStreamTable(
      tableSchema: TableSchema,
      statistic: FlinkStatistic,
      producesUpdates: Boolean = false,
      isAccRetract: Boolean = false): DataStreamTable[Row] = {
    val mockDataStream: DataStream[Row] = mock(classOf[DataStream[Row]])
    // TODO use BaseRowTypeInfo later
    val typeInfo = new RowTypeInfo(tableSchema.getFieldTypes, tableSchema.getFieldNames)
    when(mockDataStream.getType).thenReturn(typeInfo)
    new DataStreamTable[Row](
      mockDataStream,
      producesUpdates,
      isAccRetract,
      tableSchema.getFieldTypes.indices.toArray,
      tableSchema.getFieldNames,
      statistic)
  }

}
