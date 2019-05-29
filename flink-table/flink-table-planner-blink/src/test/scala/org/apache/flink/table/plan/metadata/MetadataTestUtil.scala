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
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.`type`.{InternalType, InternalTypes, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableSchema}
import org.apache.flink.table.calcite.{FlinkCalciteCatalogReader, FlinkContextImpl, FlinkTypeSystem}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.table.plan.stats.{ColumnStats, FlinkStatistic, TableStats}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.config.{CalciteConnectionConfigImpl, CalciteConnectionProperty, Lex}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.rel.RelCollationTraitDef
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{FrameworkConfig, Frameworks}
import org.mockito.Mockito.{mock, when}

import java.util.{Collections, Properties}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object MetadataTestUtil {

  def createFrameworkConfig(defaultSchema: SchemaPlus, config: TableConfig): FrameworkConfig = {
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
      .context(new FlinkContextImpl(config))
      .traitDefs(Array(
        ConventionTraitDef.INSTANCE,
        FlinkRelDistributionTraitDef.INSTANCE,
        RelCollationTraitDef.INSTANCE
      ): _*)
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
    rootSchema.add("MyTable1", createMyTable1())
    rootSchema.add("MyTable2", createMyTable2())
    rootSchema.add("MyTable3", createMyTable3())
    rootSchema.add("MyTable4", createMyTable4())
    rootSchema.add("TemporalTable1", createTemporalTable1())
    rootSchema.add("TemporalTable2", createTemporalTable2())
    rootSchema.add("TemporalTable3", createTemporalTable3())
    rootSchema
  }

  private def createStudentTable(): DataStreamTable[BaseRow] = {
    val schema = new TableSchema(
      Array("id", "name", "score", "age", "height", "sex", "class"),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO))

    val colStatsMap = Map[String, ColumnStats](
      "id" -> new ColumnStats(50L, 0L, 8D, 8, null, 0),
      "name" -> new ColumnStats(48L, 0L, 7.2, 12, null, null),
      "score" -> new ColumnStats(20L, 6L, 8D, 8, 4.8D, 2.7D),
      "age" -> new ColumnStats(7L, 0L, 4D, 4, 18, 12),
      "height" -> new ColumnStats(35L, null, 8D, 8, 172.1D, 161.0D),
      "sex" -> new ColumnStats(2L, 0L, 1D, 1, null, null))

    val tableStats = new TableStats(50L, colStatsMap)
    val uniqueKeys = Set(Set("id").asJava).asJava
    getDataStreamTable(schema, new FlinkStatistic(tableStats, uniqueKeys))
  }

  private def createEmpTable(): DataStreamTable[BaseRow] = {
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

  private def createMyTable1(): DataStreamTable[BaseRow] = {
    val schema = new TableSchema(
      Array("a", "b", "c", "d", "e"),
      Array(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.DATE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(20000000L, 0L, 4D, 4, null, 0),
      "b" -> new ColumnStats(800000000L, 0L, 8D, 8, 800000000L, 1L),
      "c" -> new ColumnStats(1581L, 0L, 12D, 12, null, null),
      "d" -> new ColumnStats(245623352L, 136231L, 88.8D, 140, null, null),
      "e" -> new ColumnStats(null, 0L, 4d, 4, 100, 1)
    )

    val tableStats = new TableStats(800000000L, colStatsMap)
    val uniqueKeys = Set(Set("b").asJava).asJava
    getDataStreamTable(schema, new FlinkStatistic(tableStats, uniqueKeys))
  }

  private def createMyTable2(): DataStreamTable[BaseRow] = {
    val schema = new TableSchema(
      Array("a", "b", "c", "d", "e"),
      Array(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.DATE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(20000000L, 0L, 4D, 4, null, null),
      "b" -> new ColumnStats(2556L, 62L, 8D, 8, 5247L, 8L),
      "c" -> new ColumnStats(682L, 0L, 12D, 12, null, null),
      "d" -> new ColumnStats(125234L, 0L, 10.52, 16, null, null),
      "e" -> new ColumnStats(null, 0L, 4d, 4, 300, 200)
    )

    val tableStats = new TableStats(20000000L, colStatsMap)
    getDataStreamTable(schema, new FlinkStatistic(tableStats))
  }

  private def createMyTable3(): DataStreamTable[BaseRow] = {
    val schema = new TableSchema(
      Array("a", "b"),
      Array(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(10L, 1L, 4D, 4, 5, -5),
      "b" -> new ColumnStats(5L, 0L, 8D, 8, 6.1D, 0D)
    )

    val tableStats = new TableStats(100L, colStatsMap)
    getDataStreamTable(schema, new FlinkStatistic(tableStats))
  }

  private def createMyTable4(): DataStreamTable[BaseRow] = {
    val schema = new TableSchema(
      Array("a", "b", "c", "d"),
      Array(BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(50L, 0L, 8D, 8, 50, 1),
      "b" -> new ColumnStats(7L, 0L, 8D, 8, 5.1D, 0D),
      "c" -> new ColumnStats(25L, 0L, 4D, 4, 46, 0),
      "d" -> new ColumnStats(46L, 0L, 8D, 8, 172.1D, 161.0D)
    )

    val tableStats = new TableStats(50L, colStatsMap)
    val uniqueKeys = Set(Set("a").asJava, Set("a", "b").asJava).asJava
    getDataStreamTable(schema, new FlinkStatistic(tableStats, uniqueKeys))
  }

  private def createTemporalTable1(): DataStreamTable[BaseRow] = {
    val fieldNames = Array("a", "b", "c", "proctime", "rowtime")
    val fieldTypes = Array[InternalType](
      InternalTypes.LONG,
      InternalTypes.STRING,
      InternalTypes.INT,
      InternalTypes.PROCTIME_INDICATOR,
      InternalTypes.ROWTIME_INDICATOR)

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(30L, 0L, 4D, 4, 45, 5),
      "b" -> new ColumnStats(5L, 0L, 32D, 32, null, null),
      "c" -> new ColumnStats(48L, 0L, 8D, 8, 50, 0)
    )

    val tableStats = new TableStats(50L, colStatsMap)
    getDataStreamTable(fieldNames, fieldTypes, new FlinkStatistic(tableStats),
      producesUpdates = false, isAccRetract = false)
  }

  private def createTemporalTable2(): DataStreamTable[BaseRow] = {
    val fieldNames = Array("a", "b", "c", "proctime", "rowtime")
    val fieldTypes = Array[InternalType](
      InternalTypes.LONG,
      InternalTypes.STRING,
      InternalTypes.INT,
      InternalTypes.PROCTIME_INDICATOR,
      InternalTypes.ROWTIME_INDICATOR)

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(50L, 0L, 8D, 8, 55, 5),
      "b" -> new ColumnStats(5L, 0L, 16D, 32, null, null),
      "c" -> new ColumnStats(48L, 0L, 4D, 4, 50, 0)
    )

    val tableStats = new TableStats(50L, colStatsMap)
    val uniqueKeys = Set(Set("a").asJava).asJava
    getDataStreamTable(fieldNames, fieldTypes, new FlinkStatistic(tableStats, uniqueKeys),
      producesUpdates = false, isAccRetract = false)
  }

  private def createTemporalTable3(): DataStreamTable[BaseRow] = {
    val fieldNames = Array("a", "b", "c", "proctime", "rowtime")
    val fieldTypes = Array[InternalType](
      InternalTypes.INT,
      InternalTypes.LONG,
      InternalTypes.STRING,
      InternalTypes.PROCTIME_INDICATOR,
      InternalTypes.ROWTIME_INDICATOR)

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(3740000000L, 0L, 4D, 4, null, null),
      "b" -> new ColumnStats(53252726L, 1474L, 8D, 8, 100000000L, -100000000L),
      "c" -> new ColumnStats(null, 0L, 18.6, 64, null, null)
    )

    val tableStats = new TableStats(4000000000L, colStatsMap)
    getDataStreamTable(fieldNames, fieldTypes, new FlinkStatistic(tableStats),
      producesUpdates = false, isAccRetract = false)
  }

  private def getDataStreamTable(
      tableSchema: TableSchema,
      statistic: FlinkStatistic,
      producesUpdates: Boolean = false,
      isAccRetract: Boolean = false): DataStreamTable[BaseRow] = {
    val types = tableSchema.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo)
    getDataStreamTable(tableSchema.getFieldNames, types, statistic, producesUpdates, isAccRetract)
  }

  private def getDataStreamTable(
      fieldNames: Array[String],
      fieldTypes: Array[InternalType],
      statistic: FlinkStatistic,
      producesUpdates: Boolean,
      isAccRetract: Boolean): DataStreamTable[BaseRow] = {
    val mockDataStream: DataStream[BaseRow] = mock(classOf[DataStream[BaseRow]])
    val typeInfo = new BaseRowTypeInfo(fieldTypes, fieldNames)
    when(mockDataStream.getType).thenReturn(typeInfo)
    new DataStreamTable[BaseRow](
      mockDataStream,
      producesUpdates,
      isAccRetract,
      fieldTypes.indices.toArray,
      fieldNames,
      statistic)
  }

}
