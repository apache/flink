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
import org.apache.flink.table.factories.TableFactory
import org.apache.flink.table.plan.stats.TableStats

import scala.collection.JavaConverters._

/**
  * Defines a table in an [[ExternalCatalog]]. External catalog tables describe table sources
  * and/or sinks for both batch and stream environments.
  *
  * See also [[TableFactory]] for more information about how to target suitable factories.
  *
  * Use [[ExternalCatalogTableBuilder]] to integrate with the normalized descriptor-based API.
  *
  * @param isBatch Flag whether this external table is intended for batch environments.
  * @param isStreaming Flag whether this external table is intended for streaming environments.
  * @param isSource Flag whether this external table is declared as table source.
  * @param isSink Flag whether this external table is declared as table sink.
  * @param properties Properties that describe the table and should match with a [[TableFactory]].
  */
class ExternalCatalogTable(
    private val isBatch: Boolean,
    private val isStreaming: Boolean,
    private val isSource: Boolean,
    private val isSink: Boolean,
    private val properties: java.util.Map[String, String])
  extends TableDescriptor {

  // ----------------------------------------------------------------------------------------------
  // Legacy code
  // ---------------------------------------------------------------------------------------------

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
  override private[flink] def addProperties(descriptorProperties: DescriptorProperties): Unit = {
    descriptorProperties.putProperties(properties)
  }
}

object ExternalCatalogTable {

  /**
    * Creates a builder for creating an [[ExternalCatalogTable]].
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
    *   .supportsStreaming()
    *   .asTableSource()
    * }}}
    *
    * @param connectorDescriptor Connector descriptor describing the external system
    * @return External catalog builder
    */
  def builder(connectorDescriptor: ConnectorDescriptor): ExternalCatalogTableBuilder = {
    new ExternalCatalogTableBuilder(connectorDescriptor)
  }
}

/**
  * Builder for an [[ExternalCatalogTable]].
  *
  * @param connectorDescriptor Connector descriptor describing the external system
  */
class ExternalCatalogTableBuilder(private val connectorDescriptor: ConnectorDescriptor)
  extends TableDescriptor
  with SchematicDescriptor[ExternalCatalogTableBuilder]
  with StreamableDescriptor[ExternalCatalogTableBuilder] {

  private var isBatch: Boolean = true
  private var isStreaming: Boolean = true

  private var formatDescriptor: Option[FormatDescriptor] = None
  private var schemaDescriptor: Option[Schema] = None
  private var statisticsDescriptor: Option[Statistics] = None
  private var metadataDescriptor: Option[Metadata] = None
  private var updateMode: Option[String] = None

  /**
    * Specifies the format that defines how to read data from a connector.
    */
  override def withFormat(format: FormatDescriptor): ExternalCatalogTableBuilder = {
    formatDescriptor = Some(format)
    this
  }

  /**
    * Specifies the resulting table schema.
    */
  override def withSchema(schema: Schema): ExternalCatalogTableBuilder = {
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
  override def inAppendMode(): ExternalCatalogTableBuilder = {
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
  override def inRetractMode(): ExternalCatalogTableBuilder = {
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
  override def inUpsertMode(): ExternalCatalogTableBuilder = {
    updateMode = Some(UPDATE_MODE_VALUE_UPSERT)
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
    isBatch = false
    isStreaming = true
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
      isSource = true,
      isSink = false,
      DescriptorProperties.toJavaMap(this))
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
      isSource = false,
      isSink = true,
      DescriptorProperties.toJavaMap(this))
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
      isSource = true,
      isSink = true,
      DescriptorProperties.toJavaMap(this))
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
