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

import org.apache.flink.table.descriptors._
import org.apache.flink.table.plan.stats.TableStats

/**
  * Defines a table in an [[ExternalCatalog]].
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

}
