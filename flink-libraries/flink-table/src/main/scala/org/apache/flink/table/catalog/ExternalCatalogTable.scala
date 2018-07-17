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

import org.apache.flink.table.descriptors.DescriptorProperties.toScala
import org.apache.flink.table.descriptors.StatisticsValidator.{STATISTICS_COLUMNS, STATISTICS_ROW_COUNT, readColumnStats}
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator.{UPDATE_MODE, UPDATE_MODE_VALUE_APPEND, UPDATE_MODE_VALUE_RETRACT, UPDATE_MODE_VALUE_UPSERT}
import org.apache.flink.table.descriptors._
import org.apache.flink.table.plan.stats.TableStats

import scala.collection.JavaConverters._

/**
  * Defines a table in an [[ExternalCatalog]]. External catalog tables describe table sources
  * and/or sinks for both batch and stream environments.
  *
  * The catalog table takes descriptors which allow for declaring the communication to external
  * systems in an implementation-agnostic way. The classpath is scanned for suitable table factories
  * that match the desired configuration.
  *
  * Use the provided builder methods to configure the external catalog table accordingly.
  *
  * The following example shows how to read from a connector using a JSON format and
  * declaring it as a table source:
  *
  * {{{
  *   ExternalCatalogTable(
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
  *   .asTableSource()
  * }}}
  *
  * Note: For backwards-compatibility, the table is declared as a table source for batch and
  * streaming environment by default.
  *
  * See also [[org.apache.flink.table.factories.TableFactory]] for more information about how
  * to target suitable factories.
  *
  * @param connectorDescriptor describes the system to connect to
  */
class ExternalCatalogTable(val connectorDescriptor: ConnectorDescriptor)
  extends TableDescriptor
  with SchematicDescriptor
  with StreamableDescriptor {

  // for backwards-compatibility a table is a source by default
  private var isSource: Boolean = true
  private var isSink: Boolean = false
  // for backwards-compatibility a table supports both environments by default
  private var isBatch: Boolean = true
  private var isStreaming: Boolean = true
  private var formatDescriptor: Option[FormatDescriptor] = None
  private var schemaDescriptor: Option[Schema] = None
  private var statisticsDescriptor: Option[Statistics] = None
  private var metadataDescriptor: Option[Metadata] = None
  private var updateMode: Option[String] = None

  // ----------------------------------------------------------------------------------------------
  // Legacy code and internals
  // ----------------------------------------------------------------------------------------------

  /**
    * Defines a table in an [[ExternalCatalog]].
    *
    * @param connectorDescriptor   Describes the system to connect to.
    * @param formatDescriptor      Describes the data format of a connector.
    * @param schemaDescriptor      Describes the schema of the result table.
    * @param statisticsDescriptor  Describes the estimated statistics of the result table.
    * @param metadataDescriptor    Describes additional metadata of a table.
    * @deprecated                  Use [[ExternalCatalogTable(ConnectorDescriptor)]] instead and
    *                              specify the table with the provided builder methods.
    */
  @Deprecated
  @deprecated
  def this(
      connectorDescriptor: ConnectorDescriptor,
      formatDescriptor: Option[FormatDescriptor],
      schemaDescriptor: Option[Schema],
      statisticsDescriptor: Option[Statistics],
      metadataDescriptor: Option[Metadata]) {
    this(connectorDescriptor)
    this.formatDescriptor = formatDescriptor
    this.schemaDescriptor = schemaDescriptor
    this.statisticsDescriptor = statisticsDescriptor
    this.metadataDescriptor = metadataDescriptor
  }

  /**
    * Reads table statistics from the descriptors properties.
    *
    * @deprecated This method exists for backwards-compatibility only.
    */
  @Deprecated
  @deprecated
  def getTableStats: Option[TableStats] = {
    val normalizedProps = new DescriptorProperties()
    addProperties(normalizedProps)
    val rowCount = toScala(normalizedProps.getOptionalLong(STATISTICS_ROW_COUNT))
    rowCount match {
      case Some(cnt) =>
        val columnStats = readColumnStats(normalizedProps, STATISTICS_COLUMNS)
        Some(TableStats(cnt, columnStats.asJava))
      case None =>
        None
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Builder methods
  // ----------------------------------------------------------------------------------------------

  /**
    * Specifies the format that defines how to read data from a connector.
    */
  override def withFormat(format: FormatDescriptor): ExternalCatalogTable = {
    formatDescriptor = Some(format)
    this
  }

  /**
    * Specifies the resulting table schema.
    */
  override def withSchema(schema: Schema): ExternalCatalogTable = {
    schemaDescriptor = Some(schema)
    this
  }

  /**
    * Declares how to perform the conversion between a dynamic table and an external connector.
    *
    * In append mode, a dynamic table and an external connector only exchange INSERT messages.
    *
    * @see See also [[inRetractMode()]] and [[inUpsertMode()]].
    */
  override def inAppendMode(): ExternalCatalogTable = {
    updateMode = Some(UPDATE_MODE_VALUE_APPEND)
    this
  }

  /**
    * Declares how to perform the conversion between a dynamic table and an external connector.
    *
    * In retract mode, a dynamic table and an external connector exchange ADD and RETRACT messages.
    *
    * An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an
    * UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for
    * the updating (new) row.
    *
    * In this mode, a key must not be defined as opposed to upsert mode. However, every update
    * consists of two messages which is less efficient.
    *
    * @see See also [[inAppendMode()]] and [[inUpsertMode()]].
    */
  override def inRetractMode(): ExternalCatalogTable = {
    updateMode = Some(UPDATE_MODE_VALUE_RETRACT)
    this
  }

  /**
    * Declares how to perform the conversion between a dynamic table and an external connector.
    *
    * In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE messages.
    *
    * This mode requires a (possibly composite) unique key by which updates can be propagated. The
    * external connector needs to be aware of the unique key attribute in order to apply messages
    * correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as
    * DELETE messages.
    *
    * The main difference to a retract stream is that UPDATE changes are encoded with a single
    * message and are therefore more efficient.
    *
    * @see See also [[inAppendMode()]] and [[inRetractMode()]].
    */
  override def inUpsertMode(): ExternalCatalogTable = {
    updateMode = Some(UPDATE_MODE_VALUE_UPSERT)
    this
  }

  /**
    * Declares this external table as a table source.
    */
  def asTableSource(): ExternalCatalogTable = {
    isSource = true
    isSink = false
    this
  }

  /**
    * Declares this external table as a table sink.
    */
  def asTableSink(): ExternalCatalogTable = {
    isSource = false
    isSink = true
    this
  }

  /**
    * Declares this external table as both a table source and sink.
    */
  def asTableSourceAndSink(): ExternalCatalogTable = {
    isSource = true
    isSink = true
    this
  }

  /**
    * Explicitly declares this external table for supporting only stream environments.
    */
  def supportsStreaming(): ExternalCatalogTable = {
    isBatch = false
    isStreaming = true
    this
  }

  /**
    * Explicitly declares this external table for supporting only batch environments.
    */
  def supportsBatch(): ExternalCatalogTable = {
    isBatch = false
    isStreaming = true
    this
  }

  /**
    * Explicitly declares this external table for supporting both batch and stream environments.
    */
  def supportsBatchAndStreaming(): ExternalCatalogTable = {
    isBatch = true
    isStreaming = true
    this
  }

  /**
    * Specifies the statistics for this external table.
    */
  def withStatistics(statistics: Statistics): ExternalCatalogTable = {
    statisticsDescriptor = Some(statistics)
    this
  }

  /**
    * Specifies the metadata for this external table.
    */
  def withMetadata(metadata: Metadata): ExternalCatalogTable = {
    metadataDescriptor = Some(metadata)
    this
  }

  // ----------------------------------------------------------------------------------------------
  // Getters
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns whether this external table is declared as table source.
    */
  def isTableSource: Boolean = {
    isSource
  }

  /**
    * Returns whether this external table is declared as table sink.
    */
  def isTableSink: Boolean = {
    isSource
  }

  /**
    * Returns whether this external table is intended for batch environments.
    */
  def isBatchTable: Boolean = {
    isBatch
  }

  /**
    * Returns whether this external table is intended for stream environments.
    */
  def isStreamTable: Boolean = {
    isStreaming
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    connectorDescriptor.addProperties(properties)
    formatDescriptor.foreach(_.addProperties(properties))
    schemaDescriptor.foreach(_.addProperties(properties))
    statisticsDescriptor.foreach(_.addProperties(properties))
    metadataDescriptor.foreach(_.addProperties(properties))
    updateMode.foreach(mode => properties.putString(UPDATE_MODE, mode))
  }
}
