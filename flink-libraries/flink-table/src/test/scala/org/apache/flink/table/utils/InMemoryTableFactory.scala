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

import java.util

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors.RowtimeValidator._
import org.apache.flink.table.descriptors.SchemaValidator._
import org.apache.flink.table.descriptors.{DescriptorProperties, SchemaValidator}
import org.apache.flink.table.factories.{StreamTableSinkFactory, StreamTableSourceFactory, TableFactory}
import org.apache.flink.table.sinks.StreamTableSink
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row

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
    new SchemaValidator(
      isStreamEnvironment = true,
      supportsSourceTimestamps = true,
      supportsSourceWatermarks = true).validate(params)

    val tableSchema = SchemaValidator.deriveTableSinkSchema(params)

    new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink()
      .configure(tableSchema.getColumnNames, tableSchema.getTypes)
      .asInstanceOf[StreamTableSink[Row]]
  }

  override def createStreamTableSource(
      properties: util.Map[String, String])
    : StreamTableSource[Row] = {

    val params: DescriptorProperties = new DescriptorProperties(true)
    params.putProperties(properties)

    // validate
    new SchemaValidator(
      isStreamEnvironment = true,
      supportsSourceTimestamps = true,
      supportsSourceWatermarks = true).validate(params)

    val tableSchema = params.getTableSchema(SCHEMA)

    // proctime
    val proctimeAttributeOpt = SchemaValidator.deriveProctimeAttribute(params)

    val (names, types) = tableSchema.getColumnNames.zip(tableSchema.getTypes)
      .filter(_._1 != proctimeAttributeOpt.get()).unzip
    // rowtime
    val rowtimeDescriptors = SchemaValidator.deriveRowtimeAttributes(params)
    new MemoryTableSourceSinkUtil.UnsafeMemoryTableSource(
      tableSchema,
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

    properties
  }
}
