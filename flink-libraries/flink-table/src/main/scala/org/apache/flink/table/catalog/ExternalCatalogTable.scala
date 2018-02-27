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

import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.catalog.ExternalCatalogTable._
import org.apache.flink.table.descriptors.DescriptorProperties.toScala
import org.apache.flink.table.descriptors.MetadataValidator.{METADATA_COMMENT, METADATA_CREATION_TIME, METADATA_LAST_ACCESS_TIME}
import org.apache.flink.table.descriptors._
import org.apache.flink.table.plan.stats.TableStats

import scala.collection.JavaConverters._

/**
  * Defines a table in an [[ExternalCatalog]].
  *
  * For backwards compatibility this class supports both the legacy table type (see
  * [[org.apache.flink.table.annotation.TableType]]) and the factory-based (see
  * [[org.apache.flink.table.sources.TableSourceFactory]]) approach.
  *
  * @param connectorDesc describes the system to connect to
  * @param formatDesc describes the data format of a connector
  * @param schemaDesc describes the schema of the result table
  * @param statisticsDesc describes the estimated statistics of the result table
  * @param metadataDesc describes additional metadata of a table
  */
class ExternalCatalogTable(
    connectorDesc: ConnectorDescriptor,
    formatDesc: Option[FormatDescriptor],
    schemaDesc: Option[Schema],
    statisticsDesc: Option[Statistics],
    metadataDesc: Option[Metadata])
  extends TableSourceDescriptor {

  this.connectorDescriptor = Some(connectorDesc)
  this.formatDescriptor = formatDesc
  this.schemaDescriptor = schemaDesc
  this.statisticsDescriptor = statisticsDesc
  this.metaDescriptor = metadataDesc

  // expose statistics for external table source util
  override def getTableStats: Option[TableStats] = super.getTableStats

  // ----------------------------------------------------------------------------------------------
  // NOTE: the following code is used for backwards compatibility to the TableType approach
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns the legacy table type of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val tableType: String = {
    val props = new DescriptorProperties()
    connectorDesc.addProperties(props)
    toScala(props.getOptionalString(CONNECTOR_LEGACY_TYPE))
      .getOrElse(throw new TableException("Could not find a legacy table type to return."))
  }

  /**
    * Returns the legacy schema of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val schema: TableSchema = {
    val props = new DescriptorProperties()
    connectorDesc.addProperties(props)
    toScala(props.getOptionalTableSchema(CONNECTOR_LEGACY_SCHEMA))
      .getOrElse(throw new TableException("Could not find a legacy schema to return."))
  }

  /**
    * Returns the legacy properties of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val properties: JMap[String, String] = {
    // skip normalization
    val props = new DescriptorProperties(normalizeKeys = false)
    val legacyProps = new JHashMap[String, String]()
    connectorDesc.addProperties(props)
    props.asMap.asScala.flatMap { case (k, v) =>
      if (k.startsWith(CONNECTOR_LEGACY_PROPERTY)) {
        // remove "connector.legacy-property-"
        Some(legacyProps.put(k.substring(CONNECTOR_LEGACY_PROPERTY.length + 1), v))
      } else {
        None
      }
    }
    legacyProps
  }

  /**
    * Returns the legacy statistics of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val stats: TableStats = getTableStats.orNull

  /**
    * Returns the legacy comment of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val comment: String = {
    val normalizedProps = new DescriptorProperties()

    metadataDesc match {
      case Some(meta) =>
        meta.addProperties(normalizedProps)
        normalizedProps.getOptionalString(METADATA_COMMENT).orElse(null)
      case None =>
        null
    }
  }

  /**
    * Returns the legacy creation time of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val createTime: JLong = {
    val normalizedProps = new DescriptorProperties()

    metadataDesc match {
      case Some(meta) =>
        meta.addProperties(normalizedProps)
        normalizedProps.getOptionalLong(METADATA_CREATION_TIME).orElse(null)
      case None =>
        null
    }
  }

  /**
    * Returns the legacy last access time of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val lastAccessTime: JLong = {
    val normalizedProps = new DescriptorProperties()

    metadataDesc match {
      case Some(meta) =>
        meta.addProperties(normalizedProps)
        normalizedProps.getOptionalLong(METADATA_LAST_ACCESS_TIME).orElse(null)
      case None =>
        null
    }
  }

  /**
    * Defines a table in an [[ExternalCatalog]].
    *
    * @param tableType            Table type, e.g csv, hbase, kafka
    * @param schema               Schema of the table (column names and types)
    * @param properties           Properties of the table
    * @param stats                Statistics of the table
    * @param comment              Comment of the table
    * @param createTime           Create timestamp of the table
    * @param lastAccessTime       Timestamp of last access of the table
    * @deprecated Use a descriptor-based constructor instead.
    */
  @Deprecated
  @deprecated("Use a descriptor-based constructor instead.")
  def this(
    tableType: String,
    schema: TableSchema,
    properties: JMap[String, String] = new JHashMap(),
    stats: TableStats = null,
    comment: String = null,
    createTime: JLong = System.currentTimeMillis,
    lastAccessTime: JLong = -1L) = {

    this(
      toConnectorDescriptor(tableType, schema, properties),
      None,
      None,
      Some(toStatisticsDescriptor(stats)),
      Some(toMetadataDescriptor(comment, createTime, lastAccessTime)))
  }

  /**
    * Returns whether this external catalog table uses the legacy table type.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  def isLegacyTableType: Boolean = connectorDesc.isInstanceOf[TableTypeConnector]
}

object ExternalCatalogTable {

  val CONNECTOR_TYPE_VALUE = "legacy-table-type"
  val CONNECTOR_LEGACY_TYPE = "connector.legacy-type"
  val CONNECTOR_LEGACY_SCHEMA = "connector.legacy-schema"
  val CONNECTOR_LEGACY_PROPERTY = "connector.legacy-property"

  /**
    * Defines a table in an [[ExternalCatalog]].
    *
    * @param tableType            Table type, e.g csv, hbase, kafka
    * @param schema               Schema of the table (column names and types)
    * @param properties           Properties of the table
    * @param stats                Statistics of the table
    * @param comment              Comment of the table
    * @param createTime           Create timestamp of the table
    * @param lastAccessTime       Timestamp of last access of the table
    * @deprecated Use a descriptor-based constructor instead.
    */
  @Deprecated
  @deprecated("Use a descriptor-based constructor instead.")
  def apply(
    tableType: String,
    schema: TableSchema,
    properties: JMap[String, String] = new JHashMap(),
    stats: TableStats = null,
    comment: String = null,
    createTime: JLong = System.currentTimeMillis,
    lastAccessTime: JLong = -1L): ExternalCatalogTable = {

    new ExternalCatalogTable(
      tableType,
      schema,
      properties,
      stats,
      comment,
      createTime,
      lastAccessTime)
  }

  class TableTypeConnector(
      tableType: String,
      schema: TableSchema,
      legacyProperties: JMap[String, String])
    extends ConnectorDescriptor(CONNECTOR_TYPE_VALUE, version = 1, formatNeeded = false) {

    override protected def addConnectorProperties(properties: DescriptorProperties): Unit = {
      properties.putString(CONNECTOR_LEGACY_TYPE, tableType)
      properties.putTableSchema(CONNECTOR_LEGACY_SCHEMA, schema)
      legacyProperties.asScala.foreach { case (k, v) =>
          properties.putString(s"$CONNECTOR_LEGACY_PROPERTY-$k", v)
      }
    }
  }

  def toConnectorDescriptor(
      tableType: String,
      schema: TableSchema,
      properties: JMap[String, String])
    : ConnectorDescriptor = {

    new TableTypeConnector(tableType, schema, properties)
  }

  def toStatisticsDescriptor(stats: TableStats): Statistics = {
    val statsDesc = Statistics()
    if (stats != null) {
      statsDesc.tableStats(stats)
    }
    statsDesc
  }

  def toMetadataDescriptor(
      comment: String,
      createTime: JLong,
      lastAccessTime: JLong)
    : Metadata = {

    val metadataDesc = Metadata()
    if (comment != null) {
      metadataDesc.comment(comment)
    }
    metadataDesc
      .creationTime(createTime)
      .lastAccessTime(lastAccessTime)
  }
}
