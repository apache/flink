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

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.descriptors.DescriptorUtils.validateTableDescriptorProperties
import org.apache.flink.table.factories.{BatchTableSinkFactory, BatchTableSourceFactory, TableFactoryService}

/**
  * Descriptor for specifying a table source and/or sink in a batch environment.
  */
class BatchTableDescriptor(
    private val tableEnv: BatchTableEnvironment,
    private val connectorDescriptor: ConnectorDescriptor)
  extends TableDescriptor
  with SchematicDescriptor
  with RegistrableDescriptor {

  private var formatDescriptor: Option[FormatDescriptor] = None
  private var schemaDescriptor: Option[Schema] = None

  /**
    * Searches for the specified table source, configures it accordingly, and registers it as
    * a table under the given name.
    *
    * @param name table name to be registered in the table environment
    */
  override def registerTableSource(name: String): Unit = {
    val javaMap = getValidProperties.asMap
    val tableSource = TableFactoryService
      .find(classOf[BatchTableSourceFactory[_]], javaMap)
      .createBatchTableSource(javaMap)
    tableEnv.registerTableSource(name, tableSource)
  }

  /**
    * Searches for the specified table sink, configures it accordingly, and registers it as
    * a table under the given name.
    *
    * @param name table name to be registered in the table environment
    */
  override def registerTableSink(name: String): Unit = {
    val javaMap = getValidProperties.asMap
    val tableSink = TableFactoryService
      .find(classOf[BatchTableSinkFactory[_]], javaMap)
      .createBatchTableSink(javaMap)
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
  override def withFormat(format: FormatDescriptor): BatchTableDescriptor = {
    formatDescriptor = Some(format)
    this
  }

  /**
    * Specifies the resulting table schema.
    */
  override def withSchema(schema: Schema): BatchTableDescriptor = {
    schemaDescriptor = Some(schema)
    this
  }

  override def toString: String = {
    getValidProperties.toString
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    connectorDescriptor.addProperties(properties)
    formatDescriptor.foreach(_.addProperties(properties))
    schemaDescriptor.foreach(_.addProperties(properties))
  }

  private def getValidProperties: DescriptorProperties = {
    validateTableDescriptorProperties(
      isStreaming = false,
      this,
      connectorDescriptor,
      formatDescriptor)
  }
}
