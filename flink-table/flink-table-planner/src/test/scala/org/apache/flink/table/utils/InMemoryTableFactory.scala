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

package org.apache.flink.table.utils

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors.DescriptorProperties._
import org.apache.flink.table.descriptors.Rowtime._
import org.apache.flink.table.descriptors.Schema._
import org.apache.flink.table.descriptors.{DescriptorProperties, SchemaValidator}
import org.apache.flink.table.factories.{StreamTableSinkFactory, StreamTableSourceFactory, TableFactory}
import org.apache.flink.table.sinks.StreamTableSink
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.types.Row

import java.sql.Timestamp
import java.util

/**
  * Factory for creating stream table sources and sinks.
  *
  * See [[MemoryTableSourceSinkUtil.UnsafeMemoryTableSource]] and
  * [[MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink]].
  *
  * @param terminationCount determines when to shutdown the streaming source function
  */
class InMemoryTableFactory(terminationCount: Int)
  extends TableFactory
  with StreamTableSourceFactory[Row]
  with StreamTableSinkFactory[Row] {

  override def createStreamTableSink(
      properties: util.Map[String, String])
    : StreamTableSink[Row] = {

    val params: DescriptorProperties = new DescriptorProperties(true)
    params.putProperties(properties)

    // validate
    new SchemaValidator(true, true, true).validate(params)

    val tableSchema = SchemaValidator.deriveTableSinkSchema(params)
    val fieldTypes = tableSchema.getFieldDataTypes.map(t => {
      if (t.getLogicalType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
        // force to use Timestamp because old planner only support Timestamp
        t.bridgedTo(classOf[Timestamp])
      } else {
        t
      }
    })

    new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink()
      .configure(tableSchema.getFieldNames, fromDataTypeToLegacyInfo(fieldTypes))
      .asInstanceOf[StreamTableSink[Row]]
  }

  override def createStreamTableSource(
      properties: util.Map[String, String])
    : StreamTableSource[Row] = {

    val params: DescriptorProperties = new DescriptorProperties(true)
    params.putProperties(properties)

    // validate
    new SchemaValidator(true, true, true).validate(params)

    val tableSchema = params.getTableSchema(SCHEMA)

    // proctime
    val proctimeAttributeOpt = SchemaValidator.deriveProctimeAttribute(params)

    val fieldTypes = tableSchema.getFieldDataTypes.map(t => {
      if (t.getLogicalType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
        // force to use Timestamp because old planner only support Timestamp
        t.bridgedTo(classOf[Timestamp])
      } else {
        t
      }
    })
    val (names, types) = tableSchema.getFieldNames.zip(fromDataTypeToLegacyInfo(fieldTypes))
      .filter(_._1 != proctimeAttributeOpt.get()).unzip
    // rowtime
    val rowtimeDescriptors = SchemaValidator.deriveRowtimeAttributes(params)
    new MemoryTableSourceSinkUtil.UnsafeMemoryTableSource(
      TableSchema.builder().fields(tableSchema.getFieldNames, fieldTypes).build(),
      new RowTypeInfo(types, names),
      rowtimeDescriptors,
      proctimeAttributeOpt.get(),
      terminationCount)
  }

  override def requiredContext(): util.Map[String, String] = {
    val context: util.Map[String, String] = new util.HashMap[String, String]
    context.put(CONNECTOR_TYPE, "memory")
    context.put(CONNECTOR_PROPERTY_VERSION, "1") // backwards compatibility

    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()

    // schema
    properties.add(SCHEMA + ".#." + SCHEMA_TYPE)
    properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE)
    properties.add(SCHEMA + ".#." + SCHEMA_NAME)
    properties.add(SCHEMA + ".#." + SCHEMA_FROM)

    // time attributes
    properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME)
    properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE)
    properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM)
    properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS)
    properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED)
    properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE)
    properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS)
    properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED)
    properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY)

    // watermark
    properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
    properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
    properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);

    // computed column
    properties.add(SCHEMA + ".#." + EXPR)

    // table constraint
    properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_NAME);
    properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_COLUMNS);
    // comment
    properties.add(COMMENT);

    properties
  }
}
