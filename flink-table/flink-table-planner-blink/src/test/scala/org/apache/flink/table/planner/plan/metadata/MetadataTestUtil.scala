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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.table.api.{DataTypes, TableException, TableSchema}
import org.apache.flink.table.catalog.{CatalogTable, Column, ObjectIdentifier, ResolvedCatalogTable, ResolvedSchema, UniqueConstraint}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, TableSourceTable}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, IntType, LocalZonedTimestampType, LogicalType, TimestampKind, TimestampType, VarCharType}

import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Schema.TableType
import org.apache.calcite.schema.{Schema, SchemaPlus, Table}
import org.apache.calcite.sql.{SqlCall, SqlNode}

import java.util
import java.util.Collections

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object MetadataTestUtil {

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
    rootSchema.add("projected_table_source_table", createProjectedTableSourceTable())
    rootSchema.add(
      "projected_table_source_table_with_partial_pk",
      createProjectedTableSourceTableWithPartialCompositePrimaryKey())
    rootSchema
  }

  private def createStudentTable(): Table = {
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
    getMetadataTable(schema, new FlinkStatistic(tableStats, uniqueKeys))
  }

  private def createEmpTable(): Table = {
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

    getMetadataTable(schema, new FlinkStatistic(TableStats.UNKNOWN))
  }

  private def createMyTable1(): Table = {
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
    getMetadataTable(schema, new FlinkStatistic(tableStats, uniqueKeys))
  }

  private def createMyTable2(): Table = {
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
    getMetadataTable(schema, new FlinkStatistic(tableStats))
  }

  private def createMyTable3(): Table = {
    val schema = new TableSchema(
      Array("a", "b", "c"),
      Array(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(10L, 1L, 4D, 4, 5, -5),
      "b" -> new ColumnStats(5L, 0L, 8D, 8, 6.1D, 0D),
      "c" ->
        ColumnStats.Builder.builder().setNdv(100L).setNullCount(1L).setAvgLen(16D).setMaxLen(128)
          .setMax("zzzzz").setMin("").build()
    )

    val tableStats = new TableStats(100L, colStatsMap)
    getMetadataTable(schema, new FlinkStatistic(tableStats))
  }

  private def createMyTable4(): Table = {
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
    getMetadataTable(schema, new FlinkStatistic(tableStats, uniqueKeys))
  }

  private def createTemporalTable1(): Table = {
    val fieldNames = Array("a", "b", "c", "proctime", "rowtime")
    val fieldTypes = Array[LogicalType](
      new BigIntType(),
      new VarCharType(VarCharType.MAX_LENGTH),
      new IntType(),
      new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3),
      new TimestampType(true, TimestampKind.ROWTIME, 3))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(30L, 0L, 4D, 4, 45, 5),
      "b" -> new ColumnStats(5L, 0L, 32D, 32, null, null),
      "c" -> new ColumnStats(48L, 0L, 8D, 8, 50, 0)
    )

    val tableStats = new TableStats(50L, colStatsMap)
    getMetadataTable(fieldNames, fieldTypes, new FlinkStatistic(tableStats))
  }

  private def createTemporalTable2(): Table = {
    val fieldNames = Array("a", "b", "c", "proctime", "rowtime")
    val fieldTypes = Array[LogicalType](
      new BigIntType(),
      new VarCharType(VarCharType.MAX_LENGTH),
      new IntType(),
      new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3),
      new TimestampType(true, TimestampKind.ROWTIME, 3))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(50L, 0L, 8D, 8, 55, 5),
      "b" -> new ColumnStats(5L, 0L, 16D, 32, null, null),
      "c" -> new ColumnStats(48L, 0L, 4D, 4, 50, 0)
    )

    val tableStats = new TableStats(50L, colStatsMap)
    val uniqueKeys = Set(Set("a").asJava).asJava
    getMetadataTable(fieldNames, fieldTypes, new FlinkStatistic(tableStats, uniqueKeys))
  }

  private def createTemporalTable3(): Table = {
    val fieldNames = Array("a", "b", "c", "proctime", "rowtime")
    val fieldTypes = Array[LogicalType](
      new IntType(),
      new BigIntType(),
      new VarCharType(VarCharType.MAX_LENGTH),
      new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3),
      new TimestampType(true, TimestampKind.ROWTIME, 3))

    val colStatsMap = Map[String, ColumnStats](
      "a" -> new ColumnStats(3740000000L, 0L, 4D, 4, null, null),
      "b" -> new ColumnStats(53252726L, 1474L, 8D, 8, 100000000L, -100000000L),
      "c" -> new ColumnStats(null, 0L, 18.6, 64, null, null)
    )

    val tableStats = new TableStats(4000000000L, colStatsMap)
    getMetadataTable(fieldNames, fieldTypes, new FlinkStatistic(tableStats))
  }

  private def createProjectedTableSourceTable(): Table = {
    val catalogTable = CatalogTable.fromProperties(
      Map(
        "connector" -> "values",
        "bounded" -> "true",
        "schema.0.name" -> "a",
        "schema.0.data-type" -> "BIGINT NOT NULL",
        "schema.1.name" -> "b",
        "schema.1.data-type" -> "INT",
        "schema.2.name" -> "c",
        "schema.2.data-type" -> "VARCHAR(2147483647)",
        "schema.3.name" -> "d",
        "schema.3.data-type" -> "BIGINT NOT NULL",
        "schema.primary-key.name" -> "PK_1",
        "schema.primary-key.columns" -> "a,d")
    )

    val resolvedSchema = new ResolvedSchema(
      util.Arrays.asList(
        Column.physical("a", DataTypes.BIGINT().notNull()),
        Column.physical("b", DataTypes.INT()),
        Column.physical("c", DataTypes.STRING()),
        Column.physical("d", DataTypes.BIGINT().notNull())),
      Collections.emptyList(),
      UniqueConstraint.primaryKey("PK_1", util.Arrays.asList("a", "d")))

    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    val rowType = typeFactory.buildRelNodeRowType(
      Seq("a", "c", "d"),
      Seq(new BigIntType(false), new DoubleType(), new VarCharType(false, 100)))

    new MockTableSourceTable(
      ObjectIdentifier.of("default_catalog", "default_database", "projected_table_source_table"),
      rowType,
      new TestTableSource(),
      true,
      new ResolvedCatalogTable(catalogTable, resolvedSchema),
      Array("project=[a, c, d]"))
  }

  private def createProjectedTableSourceTableWithPartialCompositePrimaryKey(): Table = {
    val catalogTable = CatalogTable.fromProperties(
      Map(
        "connector" -> "values",
        "bounded" -> "true",
        "schema.0.name" -> "a",
        "schema.0.data-type" -> "BIGINT NOT NULL",
        "schema.1.name" -> "b",
        "schema.1.data-type" -> "BIGINT NOT NULL",
        "schema.primary-key.name" -> "PK_1",
        "schema.primary-key.columns" -> "a,b")
    )

    val resolvedSchema = new ResolvedSchema(
      util.Arrays.asList(
        Column.physical("a", DataTypes.BIGINT().notNull()),
        Column.physical("b", DataTypes.BIGINT().notNull())),
      Collections.emptyList(),
      UniqueConstraint.primaryKey("PK_1", util.Arrays.asList("a", "b")))

    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    val rowType = typeFactory.buildRelNodeRowType(
      Seq("a"),
      Seq(new BigIntType(false)))

    new MockTableSourceTable(
      ObjectIdentifier.of(
        "default_catalog",
        "default_database",
        "projected_table_source_table_with_partial_pk"),
      rowType,
      new TestTableSource(),
      true,
      new ResolvedCatalogTable(catalogTable, resolvedSchema),
      Array("project=[a]"))
  }

  private def getMetadataTable(
      tableSchema: TableSchema,
      statistic: FlinkStatistic,
      producesUpdates: Boolean = false,
      isAccRetract: Boolean = false): Table = {
    val names = tableSchema.getFieldNames
    val types = tableSchema.getFieldTypes.map(fromTypeInfoToLogicalType)
    getMetadataTable(names, types, statistic)
  }

  private def getMetadataTable(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType],
      statistic: FlinkStatistic): Table = {
    val flinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    val rowType = flinkTypeFactory.buildRelNodeRowType(fieldNames, fieldTypes)
    new MockMetaTable(rowType, statistic)
  }
}

/** A mock table used for metadata test, it implements both [[Table]]
  * and [[FlinkPreparingTableBase]]. */
class MockMetaTable(rowType: RelDataType, statistic: FlinkStatistic)
  extends FlinkPreparingTableBase(null, rowType,
    Collections.singletonList("MockMetaTable"), statistic)
  with Table {
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = rowType

  override def getJdbcTableType: Schema.TableType = TableType.TABLE

  override def isRolledUp(column: String): Boolean = false

  override def rolledUpColumnValidInsideAgg(column: String,
    call: SqlCall, parent: SqlNode, config: CalciteConnectionConfig): Boolean = false
}

class TestTableSource extends ScanTableSource {
  override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()

  override def getScanRuntimeProvider(
      context: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    throw new TableException("Unsupported operation")
  }

  override def copy = new TestTableSource()

  override def asSummaryString = "test-source"
}

class MockTableSourceTable(
    tableIdentifier: ObjectIdentifier,
    rowType: RelDataType,
    tableSource: DynamicTableSource,
    isStreamingMode: Boolean,
    catalogTable: ResolvedCatalogTable,
    extraDigests: Array[String] = Array.empty)
  extends TableSourceTable(
    null,
    tableIdentifier,
    rowType,
    FlinkStatistic.UNKNOWN,
    tableSource,
    isStreamingMode,
    catalogTable,
    extraDigests)
  with Table {
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = rowType

  override def getJdbcTableType: Schema.TableType = TableType.TABLE

  override def isRolledUp(column: String): Boolean = false

  override def rolledUpColumnValidInsideAgg(column: String,
      call: SqlCall, parent: SqlNode, config: CalciteConnectionConfig): Boolean = false
}
