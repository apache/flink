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

import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.factories.{BatchTableSourceFactory, StreamTableSourceFactory, TableFactoryService}
import org.apache.flink.table.plan.schema.{BatchTableSourceTable, StreamTableSourceTable, TableSourceSinkTable, TableSourceTable}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.util.Logging

/**
  * The utility class is used to convert ExternalCatalogTable to TableSourceTable.
  */
object ExternalTableSourceUtil extends Logging {

  /**
    * Converts an [[ExternalCatalogTable]] instance to a [[TableSourceTable]] instance
    *
    * @param externalCatalogTable the [[ExternalCatalogTable]] instance which to convert
    * @return converted [[TableSourceTable]] instance from the input catalog table
    */
  def fromExternalCatalogTable(
      tableEnv: TableEnvironment,
      externalCatalogTable: ExternalCatalogTable)
    : TableSourceSinkTable[_, _] = {
    val properties = new DescriptorProperties()
    externalCatalogTable.addProperties(properties)
    val javaMap = properties.asMap
    tableEnv match {
      // check for a batch table source in this batch environment
      case _: BatchTableEnvironment =>
        val source = TableFactoryService
          .find(classOf[BatchTableSourceFactory[_]], javaMap)
          .createBatchTableSource(javaMap)
        val sourceTable = new BatchTableSourceTable(
          source,
          new FlinkStatistic(externalCatalogTable.getTableStats))
        new TableSourceSinkTable(Some(sourceTable), None)

      // check for a stream table source in this streaming environment
      case _: StreamTableEnvironment =>
        val source = TableFactoryService
          .find(classOf[StreamTableSourceFactory[_]], javaMap)
          .createStreamTableSource(javaMap)
        val sourceTable = new StreamTableSourceTable(
          source,
          new FlinkStatistic(externalCatalogTable.getTableStats))
        new TableSourceSinkTable(Some(sourceTable), None)

      case _ => throw new TableException("Unsupported table environment.")
    }
  }
}
