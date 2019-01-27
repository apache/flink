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

package org.apache.flink.table.factories.utils

import java.util

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.factories.utils.TestTableSinkFactory.CONNECTOR_TYPE_VALUE_TEST
import org.apache.flink.table.factories.{StreamTableSourceFactory, TableFactory}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.util.{TableProperties, TableSchemaUtil}
import org.apache.flink.types.Row
import java.lang.{Integer => JInt, Long => JLong}

/**
  * Table source factory for testing.
  */
class TestTableSourceFactory
  extends StreamTableSourceFactory[Row]
    with TableFactory {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TEST)
    context.put(CONNECTOR_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    properties.add("schema")
    // connector
    properties.add("format.path")
    properties.add("schema.#.name")
    properties.add("schema.#.field.#.name")
    properties
  }

  override def createStreamTableSource(
      properties: util.Map[String, String])
    : StreamTableSource[Row] = {
    val props = new TableProperties
    props.putProperties(properties)
    val schema = props.readSchemaFromProperties(Thread.currentThread().getContextClassLoader)
    val tableName = props.readTableNameFromProperties()
    TestingTableSource(tableName, schema, props)
  }
}

object TestTableSourceFactory {
  val CONNECTOR_TYPE_VALUE_TEST = "test"
  val FORMAT_TYPE_VALUE_TEST = "test"
}

case class TestingTableSource(
     tableName: String,
     schema: RichTableSchema,
     properties: TableProperties) extends StreamTableSource[Row] {
  override def getReturnType: DataType = {
    schema.getResultRowType
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {

    val data = new util.ArrayList[Row]
    data.add(Row.of(new JInt(1), new JLong(1L), "Hi"))
    data.add(Row.of(new JInt(2), new JLong(2L), "Hello"))
    data.add(Row.of(new JInt(3), new JLong(2L), "Hello world"))

    execEnv.fromCollection(data).returns(schema.getResultTypeInfo())
  }

  /** Returns the table schema of the table source */
  override def getTableSchema = TableSchemaUtil.fromDataType(getReturnType)

  override def explainSource(): String = ""
}
