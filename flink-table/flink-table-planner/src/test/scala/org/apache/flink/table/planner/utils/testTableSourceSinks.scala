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
package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import org.apache.flink.legacy.table.factories.StreamTableSourceFactory
import org.apache.flink.legacy.table.sinks.StreamTableSink
import org.apache.flink.legacy.table.sources.{InputFormatTableSource, StreamTableSource}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.legacy.io.CollectionInputFormat
import org.apache.flink.table.api.{DataTypes, TableDescriptor, TableEnvironment}
import org.apache.flink.table.catalog._
import org.apache.flink.table.descriptors._
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR, CONNECTOR_TYPE}
import org.apache.flink.table.expressions.DefaultSqlFactory
import org.apache.flink.table.factories.FactoryUtil
import org.apache.flink.table.legacy.api.TableSchema
import org.apache.flink.table.legacy.descriptors.Schema
import org.apache.flink.table.legacy.factories.{TableSinkFactory, TableSourceFactory}
import org.apache.flink.table.legacy.sinks.TableSink
import org.apache.flink.table.legacy.sources._
import org.apache.flink.table.planner._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.hint.OptionsHintTest.IS_BOUNDED
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.table.utils.TableSchemaUtils.getPhysicalSchema
import org.apache.flink.types.Row

import _root_.java.{lang, util}
import _root_.java.io.{File, FileOutputStream, OutputStreamWriter}
import _root_.java.util.function.BiConsumer
import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

import java.nio.file.Files

object TestTableSourceSinks {
  def createPersonCsvTemporaryTable(tEnv: TableEnvironment, tableName: String): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TEMPORARY TABLE $tableName (
                       |  first STRING,
                       |  id INT,
                       |  score DOUBLE,
                       |  last STRING
                       |) WITH (
                       |  'connector.type' = 'filesystem',
                       |  'connector.path' = '$getPersonCsvPath',
                       |  'format.type' = 'csv',
                       |  'format.field-delimiter' = '#',
                       |  'format.line-delimiter' = '$$',
                       |  'format.ignore-first-line' = 'true',
                       |  'format.comment-prefix' = '%'
                       |)
                       |""".stripMargin)
  }

  def createOrdersCsvTemporaryTable(tEnv: TableEnvironment, tableName: String): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TEMPORARY TABLE $tableName (
                       |  amount BIGINT,
                       |  currency STRING,
                       |  ts BIGINT
                       |) WITH (
                       |  'connector.type' = 'filesystem',
                       |  'connector.path' = '$getOrdersCsvPath',
                       |  'format.type' = 'csv',
                       |  'format.field-delimiter' = ',',
                       |  'format.line-delimiter' = '$$'
                       |)
                       |""".stripMargin)
  }

  def createRatesCsvTemporaryTable(tEnv: TableEnvironment, tableName: String): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TEMPORARY TABLE $tableName (
                       |  currency STRING,
                       |  rate BIGINT
                       |) WITH (
                       |  'connector.type' = 'filesystem',
                       |  'connector.path' = '$getRatesCsvPath',
                       |  'format.type' = 'csv',
                       |  'format.field-delimiter' = ',',
                       |  'format.line-delimiter' = '$$'
                       |)
                       |""".stripMargin)
  }

  def createCsvTemporarySinkTable(
      tEnv: TableEnvironment,
      tableSchema: TableSchema,
      tableName: String,
      numFiles: Int = 1): String = {
    val tempDir = Files.createTempDirectory("csv-test").toFile
    tempDir.deleteOnExit()
    val tempDirPath = tempDir.getAbsolutePath

    val schema = tableSchema.toSchema
    val tableDescriptor = TableDescriptor
      .forConnector("filesystem")
      .schema(schema)
      .option("path", tempDirPath)
      .option(FactoryUtil.SINK_PARALLELISM, lang.Integer.valueOf(numFiles))
      .format("testcsv")
      .build()

    tEnv.createTable(tableName, tableDescriptor)
    tempDirPath
  }

  /**
   * Used for stream/TableScanITCase#testTableSourceWithoutTimeAttribute or
   * batch/TableScanITCase#testTableSourceWithoutTimeAttribute
   */
  def createWithoutTimeAttributesTableSource(tEnv: TableEnvironment, tableName: String): Unit = {
    val data = Seq(
      Row.of("Mary", Long.box(1L), Int.box(1)),
      Row.of("Bob", Long.box(2L), Int.box(3))
    )
    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(s"""
                       | create table $tableName (
                       |  `name` string,
                       |  `id` bigint,
                       |  `value` int
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'true',
                       |  'data-id' = '$dataId'
                       | )
                       |""".stripMargin)
  }

  lazy val getPersonCsvPath = {
    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )

    writeToTempFile(csvRecords.mkString("$"), "csv-test", "tmp")
  }

  lazy val getOrdersCsvPath = {
    val csvRecords = Seq(
      "2,Euro,2",
      "1,US Dollar,3",
      "50,Yen,4",
      "3,Euro,5",
      "5,US Dollar,6"
    )

    writeToTempFile(csvRecords.mkString("$"), "csv-order-test", "tmp")
  }

  lazy val getRatesCsvPath = {
    val csvRecords = Seq(
      "US Dollar,102",
      "Yen,1",
      "Euro,119",
      "RMB,702"
    )
    writeToTempFile(csvRecords.mkString("$"), "csv-rate-test", "tmp")

  }

  private def writeToTempFile(
      contents: String,
      filePrefix: String,
      fileSuffix: String,
      charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }
}

class TestDataTypeTableSource(tableSchema: TableSchema, values: Seq[Row])
  extends InputFormatTableSource[Row] {

  override def getInputFormat: InputFormat[Row, _ <: InputSplit] = {
    new CollectionInputFormat[Row](
      values.asJava,
      fromDataTypeToTypeInfo(getProducedDataType)
        .createSerializer(new SerializerConfigImpl)
        .asInstanceOf[TypeSerializer[Row]])
  }

  override def getReturnType: TypeInformation[Row] =
    throw new RuntimeException("Should not invoke this deprecated method.")

  override def getProducedDataType: DataType = tableSchema.toRowDataType

  override def getTableSchema: TableSchema = tableSchema
}

class TestStreamTableSource(tableSchema: TableSchema, values: Seq[Row])
  extends StreamTableSource[Row] {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromData(values.asJava, tableSchema.toRowType)
  }

  override def getProducedDataType: DataType = tableSchema.toRowDataType

  override def getTableSchema: TableSchema = tableSchema
}

class TestStreamTableSourceFactory extends StreamTableSourceFactory[Row] {
  override def createStreamTableSource(properties: JMap[String, String]): StreamTableSource[Row] = {
    val descriptorProperties = new DescriptorProperties
    descriptorProperties.putProperties(properties)
    val tableSchema = descriptorProperties.getTableSchema(Schema.SCHEMA)
    val serializedRows = descriptorProperties.getOptionalString("data").orElse(null)
    val values = if (serializedRows != null) {
      EncodingUtils.decodeStringToObject(serializedRows, classOf[List[Row]])
    } else {
      Seq.empty[Row]
    }
    new TestStreamTableSource(tableSchema, values)
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestStreamTableSource")
    context
  }

  override def supportedProperties(): JList[String] = {
    val supported = new util.ArrayList[String]()
    supported.add("*")
    supported
  }
}

/**
 * A data source that implements some very basic partitionable table source in-memory.
 *
 * @param isBounded
 *   whether this is a bounded source
 * @param remainingPartitions
 *   remaining partitions after partition pruning
 */
class TestPartitionableTableSource(
    override val isBounded: Boolean,
    remainingPartitions: JList[JMap[String, String]],
    isCatalogTable: Boolean)
  extends StreamTableSource[Row]
  with PartitionableTableSource {

  private val fieldTypes: Array[TypeInformation[_]] = Array(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO)
  // 'part1' and 'part2' are partition fields
  private val fieldNames = Array("id", "name", "part1", "part2")
  private val returnType = new RowTypeInfo(fieldTypes, fieldNames)

  private val data = mutable.Map[String, Seq[Row]](
    "part1=A,part2=1" -> Seq(row(1, "Anna", "A", 1), row(2, "Jack", "A", 1)),
    "part1=A,part2=2" -> Seq(row(3, "John", "A", 2), row(4, "nosharp", "A", 2)),
    "part1=B,part2=3" -> Seq(row(5, "Peter", "B", 3), row(6, "Lucy", "B", 3)),
    "part1=C,part2=1" -> Seq(row(7, "He", "C", 1), row(8, "Le", "C", 1))
  )

  override def getPartitions: JList[JMap[String, String]] = {
    if (isCatalogTable) {
      throw new RuntimeException("Should not expected.")
    }
    List(
      Map("part1" -> "A", "part2" -> "1").asJava,
      Map("part1" -> "A", "part2" -> "2").asJava,
      Map("part1" -> "B", "part2" -> "3").asJava,
      Map("part1" -> "C", "part2" -> "1").asJava
    ).asJava
  }

  override def applyPartitionPruning(
      remainingPartitions: JList[JMap[String, String]]): TableSource[_] = {
    new TestPartitionableTableSource(isBounded, remainingPartitions, isCatalogTable)
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val remainingData = if (remainingPartitions != null) {
      val remainingPartitionList = remainingPartitions.map {
        m => s"part1=${m.get("part1")},part2=${m.get("part2")}"
      }
      data.filterKeys(remainingPartitionList.contains).values.flatten
    } else {
      data.values.flatten
    }

    execEnv.fromData[Row](remainingData, getReturnType).setParallelism(1).setMaxParallelism(1)
  }

  override def explainSource(): String = {
    if (remainingPartitions != null) {
      s"partitions=${PartitionUtils.sortPartitionsByKey(remainingPartitions)}"
    } else {
      ""
    }
  }

  override def getReturnType: TypeInformation[Row] = returnType

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
}

class TestPartitionableSourceFactory extends TableSourceFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestPartitionableSource")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val supported = new util.ArrayList[String]()
    supported.add("*")
    supported
  }

  override def createTableSource(properties: util.Map[String, String]): TableSource[Row] = {
    val dp = new DescriptorProperties()
    dp.putProperties(properties)

    val isBounded = dp.getBoolean("is-bounded")
    val sourceFetchPartitions = dp.getBoolean("source-fetch-partitions")
    val remainingPartitions = dp
      .getOptionalArray(
        "remaining-partition",
        new java.util.function.Function[String, util.Map[String, String]] {
          override def apply(t: String): util.Map[String, String] = {
            dp.getString(t)
              .split(",")
              .map(kv => kv.split(":"))
              .map(a => (a(0), a(1)))
              .toMap[String, String]
          }
        }
      )
      .orElse(null)
    new TestPartitionableTableSource(isBounded, remainingPartitions, sourceFetchPartitions)
  }
}

object TestPartitionableSourceFactory {
  private val tableSchema: org.apache.flink.table.api.Schema = org.apache.flink.table.api.Schema
    .newBuilder()
    .fromResolvedSchema(ResolvedSchema.of(
      Column.physical("id", DataTypes.INT()),
      Column.physical("name", DataTypes.STRING()),
      Column.physical("part1", DataTypes.STRING()),
      Column.physical("part2", DataTypes.INT())
    ))
    .build()

  /** For java invoking. */
  def createTemporaryTable(tEnv: TableEnvironment, tableName: String, isBounded: Boolean): Unit = {
    createTemporaryTable(tEnv, tableName, isBounded, tableSchema = tableSchema)
  }

  def createTemporaryTable(
      tEnv: TableEnvironment,
      tableName: String,
      isBounded: Boolean,
      tableSchema: org.apache.flink.table.api.Schema = tableSchema,
      remainingPartitions: JList[JMap[String, String]] = null,
      sourceFetchPartitions: Boolean = false): Unit = {
    val properties = new DescriptorProperties()
    properties.putString("is-bounded", isBounded.toString)
    properties.putBoolean("source-fetch-partitions", sourceFetchPartitions)
    properties.putString(CONNECTOR_TYPE, "TestPartitionableSource")
    if (remainingPartitions != null) {
      remainingPartitions.zipWithIndex.foreach {
        case (part, i) =>
          properties.putString(
            "remaining-partition." + i,
            part.map { case (k, v) => s"$k:$v" }.reduce((kv1, kv2) => s"$kv1,:$kv2")
          )
      }
    }

    val table =
      CatalogTable
        .newBuilder()
        .schema(tableSchema)
        .comment("")
        .partitionKeys(util.Arrays.asList[String]("part1", "part2"))
        .options(properties.asMap())
        .build()
    val catalog = tEnv.getCatalog(tEnv.getCurrentCatalog).get()
    val path = new ObjectPath(tEnv.getCurrentDatabase, tableName)
    catalog.createTable(path, table, false)

    val partitions = List(
      Map("part1" -> "A", "part2" -> "1").asJava,
      Map("part1" -> "A", "part2" -> "2").asJava,
      Map("part1" -> "B", "part2" -> "3").asJava,
      Map("part1" -> "C", "part2" -> "1").asJava
    )
    partitions.foreach(
      spec =>
        catalog.createPartition(
          path,
          new CatalogPartitionSpec(new java.util.LinkedHashMap(spec)),
          new CatalogPartitionImpl(Map[String, String](), ""),
          true))

  }
}

/** Factory for [[OptionsTableSource]]. */
class TestOptionsTableFactory extends TableSourceFactory[Row] with TableSinkFactory[Row] {
  import TestOptionsTableFactory._

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR, "OPTIONS")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val supported = new JArrayList[String]()
    supported.add("*")
    supported
  }

  override def createTableSource(context: TableSourceFactory.Context): TableSource[Row] = {
    createPropertiesSource(
      context.getTable
        .asInstanceOf[ResolvedCatalogTable]
        .toProperties(DefaultSqlFactory.INSTANCE))
  }

  override def createTableSink(context: TableSinkFactory.Context): TableSink[Row] = {
    createPropertiesSink(
      context.getTable
        .asInstanceOf[ResolvedCatalogTable]
        .toProperties(DefaultSqlFactory.INSTANCE))
  }
}

/** A table source that explains the properties in the plan. */
class OptionsTableSource(isBounded: Boolean, tableSchema: TableSchema, props: JMap[String, String])
  extends StreamTableSource[Row] {

  override def explainSource(): String = s"${classOf[OptionsTableSource].getSimpleName}" +
    s"(props=$props)"

  override def getTableSchema: TableSchema = getPhysicalSchema(tableSchema)

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] =
    None.asInstanceOf[DataStream[Row]]

  override def isBounded: Boolean = {
    isBounded
  }
}

/** A table source that explains the properties in the plan. */
class OptionsTableSink(tableSchema: TableSchema, val props: JMap[String, String])
  extends StreamTableSink[Row] {

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    None.asInstanceOf[DataStreamSink[Row]]
  }

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = this

  override def getTableSchema: TableSchema = tableSchema

  override def getConsumedDataType: DataType = {
    getPhysicalSchema(tableSchema).toRowDataType
  }
}

object TestOptionsTableFactory {

  def createPropertiesSource(props: JMap[String, String]): OptionsTableSource = {
    val properties = new DescriptorProperties()
    properties.putProperties(props)
    val schema = properties.getTableSchema(Schema.SCHEMA)
    val propsToShow = new util.HashMap[String, String]()
    val isBounded = properties.getBoolean(IS_BOUNDED)
    props.forEach(new BiConsumer[String, String] {
      override def accept(k: String, v: String): Unit = {
        if (
          !k.startsWith(Schema.SCHEMA)
          && !k.equalsIgnoreCase(CONNECTOR)
          && !k.equalsIgnoreCase(IS_BOUNDED)
        ) {
          propsToShow.put(k, v)
        }
      }
    })

    new OptionsTableSource(isBounded, schema, propsToShow)
  }

  def createPropertiesSink(props: JMap[String, String]): OptionsTableSink = {
    val properties = new DescriptorProperties()
    properties.putProperties(props)
    val schema = properties.getTableSchema(Schema.SCHEMA)
    val propsToShow = new util.HashMap[String, String]()
    props.forEach(new BiConsumer[String, String] {
      override def accept(k: String, v: String): Unit = {
        if (
          !k.startsWith(Schema.SCHEMA)
          && !k.equalsIgnoreCase(CONNECTOR)
          && !k.equalsIgnoreCase(IS_BOUNDED)
        ) {
          propsToShow.put(k, v)
        }
      }
    })

    new OptionsTableSink(schema, propsToShow)
  }
}
