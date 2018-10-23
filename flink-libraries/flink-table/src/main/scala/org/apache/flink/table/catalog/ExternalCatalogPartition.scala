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

import org.apache.flink.table.descriptors.DescriptorProperties._
import org.apache.flink.table.descriptors.StatisticsValidator._
import org.apache.flink.table.descriptors._
import org.apache.flink.table.plan.stats.PartitionStats

import scala.collection.JavaConverters._

/**
  * Partition definition of an external Catalog table
  *
  * @param partitionSpec partition specification
  * @param properties    partition properties
  */
class ExternalCatalogPartition(
    val partitionSpec: java.util.LinkedHashMap[String, String],
    private val properties: java.util.Map[String, String])
  extends Descriptor {

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
  def getStats: Option[PartitionStats] = {
    val normalizedProps = new DescriptorProperties()
    addProperties(normalizedProps)
    val rowCount = toScala(normalizedProps.getOptionalLong(STATISTICS_ROW_COUNT))
    rowCount match {
      case Some(cnt) =>
        val columnStats = readColumnStats(normalizedProps, STATISTICS_COLUMNS)
        Some(PartitionStats(cnt, columnStats.asJava))
      case None =>
        None
    }
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addProperties(descriptorProperties: DescriptorProperties): Unit = {
    descriptorProperties.putProperties(properties)
  }
}

object ExternalCatalogPartition {

  /**
    * Creates a builder for creating an [[ExternalCatalogPartition]].
    *
    * @param connectorDescriptor Connector descriptor describing the external system
    * @return External catalog partition builder
    */
  def builder(connectorDescriptor: ConnectorDescriptor): ExternalCatalogPartitionBuilder = {
    new ExternalCatalogPartitionBuilder(connectorDescriptor)
  }

}

/**
  * Builder for an [[ExternalCatalogPartition]].
  *
  * @param connectorDescriptor Connector descriptor describing the external system
  */
class ExternalCatalogPartitionBuilder(private val connectorDescriptor: ConnectorDescriptor)
  extends TableDescriptor {

  private var statisticsDescriptor: Option[Statistics] = None
  private var metadataDescriptor: Option[Metadata] = None
  private var partitionSpec: java.util.LinkedHashMap[String, String] = _

  /**
    * Specifies the statistics for this external partition.
    */
  def withStatistics(statistics: Statistics): ExternalCatalogPartitionBuilder = {
    statisticsDescriptor = Some(statistics)
    this
  }

  /**
    * Specifies the metadata for this external partition.
    */
  def withMetadata(metadata: Metadata): ExternalCatalogPartitionBuilder = {
    metadataDescriptor = Some(metadata)
    this
  }

  def withPartitionSpec(
    partitionSpec: java.util.LinkedHashMap[String, String]): ExternalCatalogPartitionBuilder = {
    require(partitionSpec != null && !partitionSpec.isEmpty)
    this.partitionSpec = partitionSpec
    this
  }

  /**
    * build the configured [[ExternalCatalogPartition]].
    *
    * @return External catalog table
    */
  def build(): ExternalCatalogPartition = {
    require(partitionSpec != null)
    new ExternalCatalogPartition(
      partitionSpec,
      DescriptorProperties.toJavaMap(this))
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    connectorDescriptor.addProperties(properties)
    statisticsDescriptor.foreach(_.addProperties(properties))
    metadataDescriptor.foreach(_.addProperties(properties))
  }
}
