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

package org.apache.flink.table.catalog

import java.util

import org.apache.flink.table.descriptors._

/**
  * A builder for creating an [[ExternalCatalogTable]].
  *
  * It takes [[Descriptor]]s which allow for declaring the communication to external
  * systems in an implementation-agnostic way. The classpath is scanned for suitable table
  * factories that match the desired configuration.
  *
  * Use the provided builder methods to configure the external catalog table accordingly.
  *
  * The following example shows how to read from a connector using a JSON format and
  * declaring it as a table source:
  *
  * {{{
  *   ExternalCatalogTableBuilder(
  *     new ExternalSystemXYZ()
  *       .version("0.11"))
  *   .withFormat(
  *     new Json()
  *       .jsonSchema("{...}")
  *       .failOnMissingField(false))
  *   .withSchema(
  *     new Schema()
  *       .field("user-name", "VARCHAR").from("u_name")
  *       .field("count", "DECIMAL")
  *   .supportsStreaming()
  *   .asTableSource()
  * }}}
  *
  * @param connectorDescriptor Connector descriptor describing the external system
  *
  * @deprecated use [[CatalogTableBuilder]]
  */
@Deprecated
@deprecated
class ExternalCatalogTableBuilder(connectorDescriptor: ConnectorDescriptor)
  extends TableDescriptor[ExternalCatalogTableBuilder](connectorDescriptor)
  with SchematicDescriptor[ExternalCatalogTableBuilder] {

  private var isBatch: Boolean = true
  private var isStreaming: Boolean = true

  private var schemaDescriptor: Option[Schema] = None
  private var statisticsDescriptor: Option[Statistics] = None
  private var metadataDescriptor: Option[Metadata] = None

  /**
    * Specifies the resulting table schema.
    */
  override def withSchema(schema: Schema): ExternalCatalogTableBuilder = {
    schemaDescriptor = Some(schema)
    this
  }

  /**
    * Specifies the statistics for this external table.
    */
  def withStatistics(statistics: Statistics): ExternalCatalogTableBuilder = {
    statisticsDescriptor = Some(statistics)
    this
  }

  /**
    * Specifies the metadata for this external table.
    */
  def withMetadata(metadata: Metadata): ExternalCatalogTableBuilder = {
    metadataDescriptor = Some(metadata)
    this
  }

  /**
    * Explicitly declares this external table for supporting only stream environments.
    */
  def supportsStreaming(): ExternalCatalogTableBuilder = {
    isBatch = false
    isStreaming = true
    this
  }

  /**
    * Explicitly declares this external table for supporting only batch environments.
    */
  def supportsBatch(): ExternalCatalogTableBuilder = {
    isBatch = true
    isStreaming = false
    this
  }

  /**
    * Explicitly declares this external table for supporting both batch and stream environments.
    */
  def supportsBatchAndStreaming(): ExternalCatalogTableBuilder = {
    isBatch = true
    isStreaming = true
    this
  }

  /**
    * Declares this external table as a table source and returns the
    * configured [[ExternalCatalogTable]].
    *
    * @return External catalog table
    */
  def asTableSource(): ExternalCatalogTable = {
    new ExternalCatalogTable(
      isBatch,
      isStreaming,
      true,
      false,
      toProperties)
  }

  /**
    * Declares this external table as a table sink and returns the
    * configured [[ExternalCatalogTable]].
    *
    * @return External catalog table
    */
  def asTableSink(): ExternalCatalogTable = {
    new ExternalCatalogTable(
      isBatch,
      isStreaming,
      false,
      true,
      toProperties)
  }

  /**
    * Declares this external table as both a table source and sink. It returns the
    * configured [[ExternalCatalogTable]].
    *
    * @return External catalog table
    */
  def asTableSourceAndSink(): ExternalCatalogTable = {
    new ExternalCatalogTable(
      isBatch,
      isStreaming,
      true,
      true,
      toProperties)
  }

  // ----------------------------------------------------------------------------------------------

  override protected def additionalProperties(): util.Map[String, String] = {
    val properties = new DescriptorProperties()
    schemaDescriptor.foreach(d => properties.putProperties(d.toProperties))
    statisticsDescriptor.foreach(d => properties.putProperties(d.toProperties))
    metadataDescriptor.foreach(d => properties.putProperties(d.toProperties))
    properties.asMap()
  }
}
