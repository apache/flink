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

import java.util

import org.apache.flink.api.common.io.{OutputFormat, RichOutputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.{TableDescriptor, TableEnvironment, TableSchema}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.descriptors.Schema.SCHEMA
import org.apache.flink.table.factories.StreamTableSinkFactory
import org.apache.flink.table.sinks._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.TableConnectorUtils
import org.apache.flink.types.Row

import scala.collection.mutable

/**
  * Utilities to ingest and retrieve results into and from a table program.
  */
object MemoryTableSourceSinkUtil {

  val tableData: mutable.ListBuffer[Row] = mutable.ListBuffer[Row]()

  def tableDataStrings: Seq[String] = tableData.map(_.toString)

  def clear(): Unit = {
    MemoryTableSourceSinkUtil.tableData.clear()
  }

  def createDataTypeOutputFormatTable(
      tEnv: TableEnvironment,
      schema: TableSchema,
      tableName: String): Unit = {
    val sink = new DataTypeOutputFormatTableSink(schema)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(tableName, sink)
  }

  def createLegacyUnsafeMemoryAppendTable(
      tEnv: TableEnvironment,
      schema: TableSchema,
      tableName: String): Unit = {
    val sink = new UnsafeMemoryAppendTableSink
    sink.configure(schema.getFieldNames, schema.getFieldTypes)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(tableName, sink)
  }

  def createDataTypeAppendStreamTable(
      tEnv: TableEnvironment,
      schema: TableSchema,
      tableName: String): Unit = {
    val sink = new DataTypeAppendStreamTableSink(schema)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(tableName, sink)
  }

  final class UnsafeMemoryAppendTableSink
    extends TableSinkBase[Row] with AppendStreamTableSink[Row] {

    override def getOutputType: TypeInformation[Row] = {
      new RowTypeInfo(getTableSchema.getFieldTypes, getTableSchema.getFieldNames)
    }

    override protected def copy: TableSinkBase[Row] = {
      new UnsafeMemoryAppendTableSink
    }

    override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[Row] = {
      dataStream.addSink(new MemoryAppendSink)
        .setParallelism(dataStream.getParallelism)
        .name(TableConnectorUtils.generateRuntimeName(this.getClass, getFieldNames))
    }
  }

  final class LegacyUnsafeMemoryAppendTableFactory extends StreamTableSinkFactory[Row] {
    override def createStreamTableSink(
        properties: util.Map[String, String]): StreamTableSink[Row] = {
      val dp = new DescriptorProperties
      dp.putProperties(properties)
      val tableSchema = dp.getTableSchema(SCHEMA)
      val sink = new UnsafeMemoryAppendTableSink
      sink.configure(tableSchema.getFieldNames, tableSchema.getFieldTypes)
        .asInstanceOf[StreamTableSink[Row]]
    }

    override def requiredContext(): util.Map[String, String] = {
      val context = new util.HashMap[String, String]()
      context.put(CONNECTOR_TYPE, "LegacyUnsafeMemoryAppendTable")
      context
    }

    override def supportedProperties(): util.List[String] = {
      val properties = new util.ArrayList[String]()
      properties.add("*")
      properties
    }
  }

  private class MemoryAppendSink extends RichSinkFunction[Row]() {

    override def invoke(value: Row): Unit = {
      tableData.synchronized {
        tableData += Row.copy(value)
      }
    }
  }

  private class MemoryCollectionOutputFormat extends RichOutputFormat[Row] {

    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = {}

    override def writeRecord(record: Row): Unit = {
      tableData.synchronized {
        tableData += Row.copy(record)
      }
    }

    override def close(): Unit = {}
  }

  final class DataTypeOutputFormatTableSink(
      schema: TableSchema) extends OutputFormatTableSink[Row] {

    override def getConsumedDataType: DataType = schema.toRowDataType

    override def getOutputFormat: OutputFormat[Row] = new MemoryCollectionOutputFormat

    override def getTableSchema: TableSchema = schema

    override def configure(
        fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = this
  }

  final class DataTypeOutputFormatTableFactory extends StreamTableSinkFactory[Row] {
    override def createStreamTableSink(
        properties: util.Map[String, String]): StreamTableSink[Row] = {
      val dp = new DescriptorProperties
      dp.putProperties(properties)
      val tableSchema = dp.getTableSchema(SCHEMA)
      return new DataTypeOutputFormatTableSink(tableSchema)
    }

    override def requiredContext(): util.Map[String, String] = {
      val context = new util.HashMap[String, String]()
      context.put(CONNECTOR_TYPE, "DataTypeOutputFormatTable")
      context
    }

    override def supportedProperties(): util.List[String] = {
      val properties = new util.ArrayList[String]()
      properties.add("*")
      properties
    }
  }

  final class DataTypeAppendStreamTableSink(
      schema: TableSchema) extends AppendStreamTableSink[Row] {

    override def getConsumedDataType: DataType = schema.toRowDataType

    override def getTableSchema: TableSchema = schema

    override def configure(
        fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = this

    override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
      dataStream.writeUsingOutputFormat(new MemoryCollectionOutputFormat)
    }

  }

  final class DataTypeAppendStreamTableFactory extends StreamTableSinkFactory[Row] {
    override def createStreamTableSink(
        properties: util.Map[String, String]): StreamTableSink[Row] = {
      val dp = new DescriptorProperties
      dp.putProperties(properties)
      val tableSchema = dp.getTableSchema(SCHEMA)
      return new DataTypeAppendStreamTableSink(tableSchema)
    }

    override def requiredContext(): util.Map[String, String] = {
      val context = new util.HashMap[String, String]()
      context.put(CONNECTOR_TYPE, "DataTypeAppendStreamTable")
      context
    }

    override def supportedProperties(): util.List[String] = {
      val properties = new util.ArrayList[String]()
      properties.add("*")
      properties
    }
  }
}
