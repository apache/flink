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
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors.FormatDescriptorValidator.{FORMAT_PROPERTY_VERSION, FORMAT_TYPE}
import org.apache.flink.table.factories.utils.TestTableSinkFactory.{CONNECTOR_TYPE_VALUE_TEST, FORMAT_TYPE_VALUE_TEST}
import org.apache.flink.table.factories.{TableFactory, TableSourceFactory}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.types.Row

/**
  * Table source factory for testing.
  */
class TestTableSourceFactory extends TableSourceFactory[Row] with TableFactory {

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

  override def createTableSource(properties: util.Map[String, String]): TableSource[Row] = {
    if (properties.get("failing") == "true") {
      throw new IllegalArgumentException("Error in this factory.")
    }
    new TableSource[Row] {
      override def getTableSchema: TableSchema = throw new UnsupportedOperationException()

      override def getReturnType: TypeInformation[Row] = throw new UnsupportedOperationException()
    }
  }
}

object TestTableSourceFactory {
  val CONNECTOR_TYPE_VALUE_TEST = "test"
  val FORMAT_TYPE_VALUE_TEST = "test"
}
