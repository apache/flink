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
import org.apache.flink.table.connectors.{DiscoverableTableFactory, TableSinkFactory, TableSourceFactory}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.types.Row
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.{DescriptorProperties, SchemaValidator}
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_CLASS
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_FROM
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_SERIALIZED
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_TYPE
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_CLASS
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_DELAY
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_SERIALIZED
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_TYPE
import org.apache.flink.table.descriptors.SchemaValidator.SCHEMA
import org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FROM
import org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME
import org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_PROCTIME
import org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE
import org.apache.flink.table.sinks.TableSink

class InMemoryTableFactory extends TableSourceFactory[Row]
  with TableSinkFactory[Row] with DiscoverableTableFactory {
  override def createTableSink(properties: util.Map[String, String]): TableSink[Row] = {
    val params: DescriptorProperties = new DescriptorProperties(true)
    params.putProperties(properties)

    // validate
    new SchemaValidator(true).validate(params)

    val tableSchema = SchemaValidator.deriveTableSinkSchema(params);

    new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink()
      .configure(tableSchema.getColumnNames, tableSchema.getTypes)
  }

  override def createTableSource(properties: util.Map[String, String]): TableSource[Row] = {
    val params: DescriptorProperties = new DescriptorProperties(true)
    params.putProperties(properties)

    // validate
    new SchemaValidator(true).validate(params)

    val tableSchema = SchemaValidator.deriveTableSourceSchema(params);

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
      3)
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
