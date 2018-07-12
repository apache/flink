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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator._
import org.apache.flink.table.descriptors.FormatDescriptorValidator._
import org.apache.flink.table.factories.utils.TestTableSinkFactory._
import org.apache.flink.table.factories.{TableFactory, TableSinkFactory}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.types.Row

/**
  * Test table sink factory.
  */
class TestTableSinkFactory extends TableSinkFactory[Row] with TableFactory {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TEST)
    context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST)
    context.put(CONNECTOR_PROPERTY_VERSION, "1")
    context.put(FORMAT_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    // connector
    properties.add("format.path")
    properties.add("schema.#.name")
    properties.add("schema.#.field.#.name")
    properties.add("failing")
    properties
  }

  override def createTableSink(properties: util.Map[String, String]): TableSink[Row] = {
    if (properties.get("failing") == "true") {
      throw new IllegalArgumentException("Error in this factory.")
    }
    new TableSink[Row] {
      override def getOutputType: TypeInformation[Row] = throw new UnsupportedOperationException()

      override def getFieldNames: Array[String] = throw new UnsupportedOperationException()

      override def getFieldTypes: Array[TypeInformation[_]] =
        throw new UnsupportedOperationException()

      override def configure(fieldNames: Array[String],
                             fieldTypes: Array[TypeInformation[_]]): TableSink[Row] =
        throw new UnsupportedOperationException()
    }
  }
}

object TestTableSinkFactory {
  val CONNECTOR_TYPE_VALUE_TEST = "test"
  val FORMAT_TYPE_VALUE_TEST = "test"
}

