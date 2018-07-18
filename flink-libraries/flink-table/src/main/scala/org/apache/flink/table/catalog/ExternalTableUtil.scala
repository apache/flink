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

import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.factories._
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.util.Logging


/**
  * The utility class is used to convert [[ExternalCatalogTable]] to [[TableSourceSinkTable]].
  *
  * It uses [[TableFactoryService]] for discovering.
  */
object ExternalTableUtil extends Logging {

  /**
    * Converts an [[ExternalCatalogTable]] instance to a [[TableSourceTable]] instance
    *
    * @param externalCatalogTable the [[ExternalCatalogTable]] instance which to convert
    * @return converted [[TableSourceTable]] instance from the input catalog table
    */
  def fromExternalCatalogTable[T1, T2](
      tableEnv: TableEnvironment,
      externalCatalogTable: ExternalCatalogTable)
    : TableSourceSinkTable[T1, T2] = {

    val properties = new DescriptorProperties()
    externalCatalogTable.addProperties(properties)
    val javaMap = properties.asMap
    val statistics = new FlinkStatistic(externalCatalogTable.getTableStats)

    val source: Option[TableSourceTable[T1]] = tableEnv match {
      case _: BatchTableEnvironment if externalCatalogTable.isBatchTable =>
        createBatchTableSource(externalCatalogTable, javaMap, statistics)

      case _: StreamTableEnvironment if externalCatalogTable.isStreamTable =>
        createStreamTableSource(externalCatalogTable, javaMap, statistics)

      case _ =>
        throw new ValidationException(
          "External catalog table does not support the current environment for a table source.")
    }

    val sink: Option[TableSinkTable[T2]] = tableEnv match {
      case _: BatchTableEnvironment if externalCatalogTable.isBatchTable =>
        createBatchTableSink(externalCatalogTable, javaMap, statistics)

      case _: StreamTableEnvironment if externalCatalogTable.isStreamTable =>
        createStreamTableSink(externalCatalogTable, javaMap, statistics)

      case _ =>
        throw new ValidationException(
          "External catalog table does not support the current environment for a table sink.")
    }

    new TableSourceSinkTable[T1, T2](source, sink)
  }

  private def createBatchTableSource[T](
      externalCatalogTable: ExternalCatalogTable,
      javaMap: util.Map[String, String],
      statistics: FlinkStatistic)
    : Option[TableSourceTable[T]] = {

    if (!externalCatalogTable.isTableSource) {
      return None
    }
    val source = TableFactoryService
      .find(classOf[BatchTableSourceFactory[T]], javaMap)
      .createBatchTableSource(javaMap)
    val table = new BatchTableSourceTable(
      source,
      statistics)
    Some(table)
  }

  private def createStreamTableSource[T](
      externalCatalogTable: ExternalCatalogTable,
      javaMap: util.Map[String, String],
      statistics: FlinkStatistic)
    : Option[TableSourceTable[T]] = {

    if (!externalCatalogTable.isTableSource) {
      return None
    }
    val source = TableFactoryService
      .find(classOf[StreamTableSourceFactory[T]], javaMap)
      .createStreamTableSource(javaMap)
    val table = new StreamTableSourceTable(
      source,
      statistics)
    Some(table)
  }

  private def createStreamTableSink[T](
      externalCatalogTable: ExternalCatalogTable,
      javaMap: util.Map[String, String],
      statistics: FlinkStatistic)
    : Option[TableSinkTable[T]] = {

    if (!externalCatalogTable.isTableSink) {
      return None
    }
    val sink = TableFactoryService
      .find(classOf[StreamTableSinkFactory[T]], javaMap)
      .createStreamTableSink(javaMap)
    val table = new TableSinkTable(
      sink,
      statistics)
    Some(table)
  }

  private def createBatchTableSink[T](
      externalCatalogTable: ExternalCatalogTable,
      javaMap: util.Map[String, String],
      statistics: FlinkStatistic)
    : Option[TableSinkTable[T]] = {

    if (!externalCatalogTable.isTableSink) {
      return None
    }
    val sink = TableFactoryService
      .find(classOf[BatchTableSinkFactory[T]], javaMap)
      .createBatchTableSink(javaMap)
    val table = new TableSinkTable(
      sink,
      statistics)
    Some(table)
  }
}
