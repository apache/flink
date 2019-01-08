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

import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.factories.TableFactoryUtil

/**
  * Common class for table's created with [[TableEnvironment.connect(ConnectorDescriptor)]].
  */
abstract class ConnectTableDescriptor[D <: ConnectTableDescriptor[D]](
    private val tableEnv: TableEnvironment,
    private val connectorDescriptor: ConnectorDescriptor)
  extends TableDescriptor
  with SchematicDescriptor[D]
  with RegistrableDescriptor { this: D =>

  private var formatDescriptor: Option[FormatDescriptor] = None
  private var schemaDescriptor: Option[Schema] = None

  /**
    * Searches for the specified table source, configures it accordingly, and registers it as
    * a table under the given name.
    *
    * @param name table name to be registered in the table environment
    */
  override def registerTableSource(name: String): Unit = {
    val tableSource = TableFactoryUtil.findAndCreateTableSource(tableEnv, this)
    tableEnv.registerTableSource(name, tableSource)
  }

  /**
    * Searches for the specified table sink, configures it accordingly, and registers it as
    * a table under the given name.
    *
    * @param name table name to be registered in the table environment
    */
  override def registerTableSink(name: String): Unit = {
    val tableSink = TableFactoryUtil.findAndCreateTableSink(tableEnv, this)
    tableEnv.registerTableSink(name, tableSink)
  }

  /**
    * Searches for the specified table source and sink, configures them accordingly, and registers
    * them as a table under the given name.
    *
    * @param name table name to be registered in the table environment
    */
  override def registerTableSourceAndSink(name: String): Unit = {
    registerTableSource(name)
    registerTableSink(name)
  }

  /**
    * Specifies the format that defines how to read data from a connector.
    */
  override def withFormat(format: FormatDescriptor): D = {
    formatDescriptor = Some(format)
    this
  }

  /**
    * Specifies the resulting table schema.
    */
  override def withSchema(schema: Schema): D = {
    schemaDescriptor = Some(schema)
    this
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {

    // this performs only basic validation
    // more validation can only happen within a factory
    if (connectorDescriptor.needsFormat() && formatDescriptor.isEmpty) {
      throw new ValidationException(
        s"The connector '$connectorDescriptor' requires a format description.")
    } else if (!connectorDescriptor.needsFormat() && formatDescriptor.isDefined) {
      throw new ValidationException(
        s"The connector '$connectorDescriptor' does not require a format description " +
          s"but '${formatDescriptor.get}' found.")
    }

    connectorDescriptor.addProperties(properties)
    formatDescriptor.foreach(_.addProperties(properties))
    schemaDescriptor.foreach(_.addProperties(properties))
  }
}
