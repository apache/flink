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

package org.apache.flink.table.descriptors

import org.apache.flink.table.api.{BatchTableEnvironment, Table, ValidationException}
import org.apache.flink.table.factories.{BatchTableSourceFactory, TableFactoryService}
import org.apache.flink.table.sources.TableSource

class BatchTableSourceDescriptor(tableEnv: BatchTableEnvironment, connector: ConnectorDescriptor)
  extends TableSourceDescriptor {

  connectorDescriptor = Some(connector)

  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    // check for a format
    if (connector.needsFormat() && formatDescriptor.isEmpty) {
      throw new ValidationException(
        s"The connector '$connector' requires a format description.")
    } else if (!connector.needsFormat() && formatDescriptor.isDefined) {
      throw new ValidationException(
        s"The connector '$connector' does not require a format description " +
          s"but '${formatDescriptor.get}' found.")
    }
    super.addProperties(properties)
  }

  /**
    * Searches for the specified table source, configures it accordingly, and returns it.
    */
  def toTableSource: TableSource[_] = {
    val properties = new DescriptorProperties()
    addProperties(properties)
    val javaMap = properties.asMap
    TableFactoryService
      .find(classOf[BatchTableSourceFactory[_]], javaMap)
      .createBatchTableSource(javaMap)
  }

  /**
    * Searches for the specified table source, configures it accordingly, and returns it as a table.
    */
  def toTable: Table = {
    tableEnv.fromTableSource(toTableSource)
  }

  /**
    * Searches for the specified table source, configures it accordingly, and registers it as
    * a table under the given name.
    *
    * @param name table name to be registered in the table environment
    */
  def register(name: String): Unit = {
    tableEnv.registerTableSource(name, toTableSource)
  }

  /**
    * Specifies the format that defines how to read data from a connector.
    */
  def withFormat(format: FormatDescriptor): BatchTableSourceDescriptor = {
    formatDescriptor = Some(format)
    this
  }

  /**
    * Specifies the resulting table schema.
    */
  def withSchema(schema: Schema): BatchTableSourceDescriptor = {
    schemaDescriptor = Some(schema)
    this
  }
}
