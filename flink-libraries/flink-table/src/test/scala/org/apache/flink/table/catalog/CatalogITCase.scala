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

import java.sql.Timestamp
import java.util
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, IntType, TypeInfoWrappedDataType}
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.expressions.{Proctime, ResolvedFieldReference}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.util.TableSchemaUtil

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import org.mockito.Mockito

/**
  * IT tests for catalogs.
  */
class ExternalCatalogITCase {

  private val builtin = "mycatalog"
  private val default = "mydb"
  private[this] val CONNECTOR_TYPE_COLLECTION = "COLLECTION"

  @Before
  def before(): Unit = {
    CollectionTableFactory.checkParam = false
  }

  def toRow(args: Object*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  @Test
  def testComputedColumn(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val tableEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig())

    var catalog = new FlinkInMemoryCatalog(builtin)
    catalog.createDatabase(default, new CatalogDatabase(), false)
    tableEnv.registerCatalog(builtin, catalog)
    tableEnv.setDefaultDatabase(builtin, default)

    val rowTypeInfo =
      new RowTypeInfo(Array[TypeInformation[_]](
        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), Array[String]("a", "b"))
    val data = new util.LinkedList[Row]
    data.add(toRow(new Integer(1), new Integer(2)))
    data.add(toRow(new Integer(1), new Integer(3)))
    val physicalSchema =
      TableSchemaUtil.fromDataType(new TypeInfoWrappedDataType(rowTypeInfo))
    val richTableSchema =
      new RichTableSchema(physicalSchema.getColumnNames, physicalSchema.getTypes)
    richTableSchema.setPrimaryKey("a")
    val tableSchemaBuilder = TableSchema.builder()
    physicalSchema.getColumns.foreach(
      column => tableSchemaBuilder.field(column.name, column.internalType))
    tableSchemaBuilder.field("c", DataTypes.INT)
    val computedColumn = new util.HashMap[String, RexNode]()
    val mockRelNode = Mockito.mock(classOf[RelNode])
    Mockito.when(mockRelNode.getRowType).thenReturn(
      tableEnv.getTypeFactory.buildRelDataType(
        physicalSchema.getColumnNames, physicalSchema.getTypes))
    val relBuilder = tableEnv.getRelBuilder
    relBuilder.push(mockRelNode)
    computedColumn.put("a", ResolvedFieldReference("a", DataTypes.INT).toRexNode(relBuilder))
    computedColumn.put("b", ResolvedFieldReference("b", DataTypes.INT).toRexNode(relBuilder))
    computedColumn.put("c", (ResolvedFieldReference("a", DataTypes.INT) + 1)
      .toRexNode(relBuilder))
    CollectionTableFactory.initData(
      rowTypeInfo, data)

    catalog.createTable(
      new ObjectPath(tableEnv.getDefaultDatabaseName(),"t1"),
      new CatalogTable(
        CONNECTOR_TYPE_COLLECTION,
        tableSchemaBuilder.build(),
        new util.HashMap[String, String](),
        richTableSchema,
        null,
        "",
        null,
        false,
        computedColumn,
        null,
        -1,
        0,
        0,
        false
      ),
      false)
    tableEnv.sqlUpdate("" +
        "insert into t1(a, b) select t1.b, w.c from t1 join t1 " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS w on t1.a = w.a")
    tableEnv.execute()
    val expected = new util.LinkedList[Row]()
    expected.add(toRow(new Integer(2), new Integer(2)))
    expected.add(toRow(new Integer(2), new Integer(2)))
    expected.add(toRow(new Integer(3), new Integer(2)))
    expected.add(toRow(new Integer(3), new Integer(2)))
    assertEquals(expected, CollectionTableFactory.RESULT)
  }

  @Test
  def testProctime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig())

    var catalog = new FlinkInMemoryCatalog(builtin)
    catalog.createDatabase(default, new CatalogDatabase(), false)
    tableEnv.registerCatalog(builtin, catalog)
    tableEnv.setDefaultDatabase(builtin, default)

    val rowTypeInfo =
      new RowTypeInfo(Array[TypeInformation[_]](
        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), Array[String]("a", "b"))
    val data = new util.LinkedList[Row]
    data.add(toRow(new Integer(1), new Integer(1)))
    data.add(toRow(new Integer(1), new Integer(2)))
    data.add(toRow(new Integer(1), new Integer(3)))
    data.add(toRow(new Integer(1), new Integer(4)))
    val physicalSchema =
      TableSchemaUtil.fromDataType(new TypeInfoWrappedDataType(rowTypeInfo))
    val richTableSchema =
      new RichTableSchema(physicalSchema.getColumnNames, physicalSchema.getTypes)
    richTableSchema.setPrimaryKey("a")
    val tableSchemaBuilder = TableSchema.builder()
    physicalSchema.getColumns.foreach(
      column => tableSchemaBuilder.field(column.name, column.internalType))
    tableSchemaBuilder.field("c", DataTypes.PROCTIME_INDICATOR, false)
    val computedColumn = new util.HashMap[String, RexNode]()
    val mockRelNode = Mockito.mock(classOf[RelNode])
    Mockito.when(mockRelNode.getRowType).thenReturn(
      tableEnv.getTypeFactory.buildRelDataType(
        physicalSchema.getColumnNames, physicalSchema.getTypes))
    val relBuilder = tableEnv.getRelBuilder
    relBuilder.push(mockRelNode)
    computedColumn.put("a", ResolvedFieldReference("a", DataTypes.INT).toRexNode(relBuilder))
    computedColumn.put("b", ResolvedFieldReference("b", DataTypes.STRING).toRexNode(relBuilder))
    computedColumn.put("c", Proctime().toRexNode(relBuilder))
    CollectionTableFactory.initData(
      rowTypeInfo, data)

    catalog.createTable(
      new ObjectPath(tableEnv.getDefaultDatabaseName(),"t1"),
      new CatalogTable(
        CONNECTOR_TYPE_COLLECTION,
        tableSchemaBuilder.build(),
        new util.HashMap[String, String](),
        richTableSchema,
        null,
        "",
        null,
        false,
        computedColumn,
        null,
        -1,
        0,
        0,
        true
      ),
      false)
    tableEnv.sqlUpdate("" +
        "insert into t1(a, b) " +
        "select sum(a), sum(b) from t1 group by TUMBLE(c, INTERVAL '2' Day)")
    tableEnv.execute()
    val expected = new util.LinkedList[Row]()
    assertEquals(expected, CollectionTableFactory.RESULT)
  }

  @Test
  def testRowtime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig())

    var catalog = new FlinkInMemoryCatalog(builtin)
    catalog.createDatabase(default, new CatalogDatabase(), false)
    tableEnv.registerCatalog(builtin, catalog)
    tableEnv.setDefaultDatabase(builtin, default)

    val rowTypeInfo =
      new RowTypeInfo(Array[TypeInformation[_]](
        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP),
        Array[String]("a", "b", "c"))
    val data = new util.LinkedList[Row]
    data.add(toRow(new Integer(1), new Integer(1), new Timestamp(1)))
    data.add(toRow(new Integer(1), new Integer(2), new Timestamp(2)))
    data.add(toRow(new Integer(1), new Integer(3), new Timestamp(3)))
    data.add(toRow(new Integer(1), new Integer(4), new Timestamp(4)))
    val physicalSchema =
      TableSchemaUtil.fromDataType(new TypeInfoWrappedDataType(rowTypeInfo))
    val richTableSchema =
      new RichTableSchema(physicalSchema.getColumnNames, physicalSchema.getTypes)
    richTableSchema.setPrimaryKey("a")
    val tableSchemaBuilder = TableSchema.builder()
    physicalSchema.getColumns.foreach(
      column =>
        column.name match {
          case "c" =>
            tableSchemaBuilder.field("c", DataTypes.ROWTIME_INDICATOR, false)
          case _ =>
            tableSchemaBuilder.field(column.name, column.internalType)
        }
    )
    val mockRelNode = Mockito.mock(classOf[RelNode])
    Mockito.when(mockRelNode.getRowType).thenReturn(
      tableEnv.getTypeFactory.buildRelDataType(
        physicalSchema.getColumnNames, physicalSchema.getTypes))
    val relBuilder = tableEnv.getRelBuilder
    relBuilder.push(mockRelNode)
    CollectionTableFactory.initData(
      rowTypeInfo, data)

    catalog.createTable(
      new ObjectPath(tableEnv.getDefaultDatabaseName(),"t1"),
      new CatalogTable(
        CONNECTOR_TYPE_COLLECTION,
        tableSchemaBuilder.build(),
        new util.HashMap[String, String](),
        richTableSchema,
        null,
        "",
        null,
        false,
        null,
        "c",
        1L,
        0,
        0,
        true
      ),
      false)
    tableEnv.sqlUpdate("" +
        "insert into t1(a, b, c) " +
        "select sum(a), sum(b), max(to_timestamp(b)) " +
        "from t1 group by TUMBLE(c, INTERVAL '2' Second)")
    tableEnv.execute()
    val expected = new util.LinkedList[Row]()
    expected.add(toRow(new Integer(4), new Integer(10), new Timestamp(4)))
    assertEquals(expected, CollectionTableFactory.RESULT)
  }

  @Test
  def testProctimeInBatch(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val tableEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig())

    var catalog = new FlinkInMemoryCatalog(builtin)
    catalog.createDatabase(default, new CatalogDatabase(), false)
    tableEnv.registerCatalog(builtin, catalog)
    tableEnv.setDefaultDatabase(builtin, default)

    val rowTypeInfo =
      new RowTypeInfo(Array[TypeInformation[_]](
        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), Array[String]("a", "b"))
    val data = new util.LinkedList[Row]
    data.add(toRow(new Integer(1), new Integer(1)))
    data.add(toRow(new Integer(1), new Integer(2)))
    data.add(toRow(new Integer(1), new Integer(3)))
    data.add(toRow(new Integer(1), new Integer(4)))
    val physicalSchema =
      TableSchemaUtil.fromDataType(new TypeInfoWrappedDataType(rowTypeInfo))
    val richTableSchema =
      new RichTableSchema(physicalSchema.getColumnNames, physicalSchema.getTypes)
    richTableSchema.setPrimaryKey("a")
    val tableSchemaBuilder = TableSchema.builder()
    physicalSchema.getColumns.foreach(
      column => tableSchemaBuilder.field(column.name, column.internalType))
    tableSchemaBuilder.field("c", DataTypes.PROCTIME_INDICATOR, false)
    val computedColumn = new util.HashMap[String, RexNode]()
    val mockRelNode = Mockito.mock(classOf[RelNode])
    Mockito.when(mockRelNode.getRowType).thenReturn(
      tableEnv.getTypeFactory.buildRelDataType(
        physicalSchema.getColumnNames, physicalSchema.getTypes))
    val relBuilder = tableEnv.getRelBuilder
    relBuilder.push(mockRelNode)
    computedColumn.put("a", ResolvedFieldReference("a", DataTypes.INT).toRexNode(relBuilder))
    computedColumn.put("b", ResolvedFieldReference("b", DataTypes.STRING).toRexNode(relBuilder))
    computedColumn.put("c", Proctime().toRexNode(relBuilder))
    CollectionTableFactory.initData(
      rowTypeInfo, data)

    catalog.createTable(
      new ObjectPath(tableEnv.getDefaultDatabaseName(),"t1"),
      new CatalogTable(
        CONNECTOR_TYPE_COLLECTION,
        tableSchemaBuilder.build(),
        new util.HashMap[String, String](),
        richTableSchema,
        null,
        "",
        null,
        false,
        computedColumn,
        null,
        1,
        0,
        0,
        false
      ),
      false)
    tableEnv.sqlUpdate("" +
        "insert into t1(a, b) " +
        "select a, cast(c as int) from t1")
    tableEnv.execute()
    assertEquals(4, CollectionTableFactory.RESULT.size())
  }

  @Test
  def testRowtimeInBatch(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig())

    var catalog = new FlinkInMemoryCatalog(builtin)
    catalog.createDatabase(default, new CatalogDatabase(), false)
    tableEnv.registerCatalog(builtin, catalog)
    tableEnv.setDefaultDatabase(builtin, default)

    val rowTypeInfo =
      new RowTypeInfo(Array[TypeInformation[_]](
        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP),
        Array[String]("a", "b", "c"))
    val data = new util.LinkedList[Row]
    data.add(toRow(new Integer(1), new Integer(1), new Timestamp(1)))
    data.add(toRow(new Integer(1), new Integer(2), new Timestamp(2)))
    data.add(toRow(new Integer(1), new Integer(3), new Timestamp(3)))
    data.add(toRow(new Integer(1), new Integer(4), new Timestamp(4)))
    val physicalSchema =
      TableSchemaUtil.fromDataType(new TypeInfoWrappedDataType(rowTypeInfo))
    val richTableSchema =
      new RichTableSchema(physicalSchema.getColumnNames, physicalSchema.getTypes)
    richTableSchema.setPrimaryKey("a")
    val tableSchemaBuilder = TableSchema.builder()
    physicalSchema.getColumns.foreach(
      column =>
        column.name match {
          case "c" =>
            tableSchemaBuilder.field("c", DataTypes.ROWTIME_INDICATOR, false)
          case _ =>
            tableSchemaBuilder.field(column.name, column.internalType)
        }
    )
    val mockRelNode = Mockito.mock(classOf[RelNode])
    Mockito.when(mockRelNode.getRowType).thenReturn(
      tableEnv.getTypeFactory.buildRelDataType(
        physicalSchema.getColumnNames, physicalSchema.getTypes))
    val relBuilder = tableEnv.getRelBuilder
    relBuilder.push(mockRelNode)
    CollectionTableFactory.initData(
      rowTypeInfo, data)

    catalog.createTable(
      new ObjectPath(tableEnv.getDefaultDatabaseName(),"t1"),
      new CatalogTable(
        CONNECTOR_TYPE_COLLECTION,
        tableSchemaBuilder.build(),
        new util.HashMap[String, String](),
        richTableSchema,
        null,
        "",
        null,
        false,
        null,
        "c",
        1L,
        0,
        0,
        false
      ),
      false)
    tableEnv.sqlUpdate("" +
        "insert into t1(a, b, c) " +
        "select a, b, c from t1")
    tableEnv.execute()
    val expected = new util.LinkedList[Row]()
    expected.add(toRow(new Integer(1), new Integer(1), new Timestamp(1)))
    expected.add(toRow(new Integer(1), new Integer(2), new Timestamp(2)))
    expected.add(toRow(new Integer(1), new Integer(3), new Timestamp(3)))
    expected.add(toRow(new Integer(1), new Integer(4), new Timestamp(4)))
    assertEquals(expected, CollectionTableFactory.RESULT)
  }
}

class Parser extends TableFunction[Row] with Serializable {
  def eval(a: Integer, b: Integer): Unit = {
    val row = new Row(2)
    row.setField(0, a)
    row.setField(1, b)
    collect(row)
  }

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType =
    DataTypes.createRowType(
      Array[DataType](IntType.INSTANCE, IntType.INSTANCE), Array[String]("a", "b"))
}
