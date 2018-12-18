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
import org.apache.flink.table.factories._
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sinks.{BatchTableSink, StreamTableSink}
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource}
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
    * @param externalTable the [[ExternalCatalogTable]] instance which to convert
    * @return converted [[TableSourceTable]] instance from the input catalog table
    */
  def fromExternalCatalogTable[T1, T2](
      tableEnv: TableEnvironment,
      externalTable: ExternalCatalogTable)
    : TableSourceSinkTable[T1, T2] = {

    val statistics = new FlinkStatistic(externalTable.getTableStats)

    val source: Option[TableSourceTable[T1]] = if (externalTable.isTableSource) {
      Some(createTableSource(tableEnv, externalTable, statistics))
    } else {
      None
    }

    val sink: Option[TableSinkTable[T2]] = if (externalTable.isTableSink) {
      Some(createTableSink(tableEnv, externalTable, statistics))
    } else {
      None
    }

    new TableSourceSinkTable[T1, T2](source, sink)
  }

  private def createTableSource[T](
      tableEnv: TableEnvironment,
      externalTable: ExternalCatalogTable,
      statistics: FlinkStatistic)
    : TableSourceTable[T] = tableEnv match {

    case _: BatchTableEnvironment if externalTable.isBatchTable =>
      val source = TableFactoryUtil.findAndCreateTableSource(tableEnv, externalTable)
      new BatchTableSourceTable[T](source.asInstanceOf[BatchTableSource[T]], statistics)

    case _: StreamTableEnvironment if externalTable.isStreamTable =>
      val source = TableFactoryUtil.findAndCreateTableSource(tableEnv, externalTable)
      new StreamTableSourceTable[T](source.asInstanceOf[StreamTableSource[T]], statistics)

    case _ =>
      throw new ValidationException(
        "External catalog table does not support the current environment for a table source.")
  }

  private def createTableSink[T](
      tableEnv: TableEnvironment,
      externalTable: ExternalCatalogTable,
      statistics: FlinkStatistic)
    : TableSinkTable[T] = tableEnv match {

    case _: BatchTableEnvironment if externalTable.isBatchTable =>
      val sink = TableFactoryUtil.findAndCreateTableSink(tableEnv, externalTable)
      new TableSinkTable[T](sink.asInstanceOf[BatchTableSink[T]], statistics)

    case _: StreamTableEnvironment if externalTable.isStreamTable =>
      val sink = TableFactoryUtil.findAndCreateTableSink(tableEnv, externalTable)
      new TableSinkTable[T](sink.asInstanceOf[StreamTableSink[T]], statistics)

    case _ =>
      throw new ValidationException(
        "External catalog table does not support the current environment for a table sink.")
  }
}
