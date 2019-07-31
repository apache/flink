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
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala
import org.apache.flink.table.util.Logging

/**
  * The utility class is used to convert [[ExternalCatalogTable]].
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
  def fromExternalCatalogTable[T](isStreamingMode: Boolean, externalTable: ExternalCatalogTable)
    : Option[TableSourceTable[T]] = {

    val statistics = new FlinkStatistic(toScala(externalTable.getTableStats))

    if (externalTable.isTableSource) {
      Some(createTableSource(isStreamingMode, externalTable, statistics))
    } else {
      None
    }
  }

  private def createTableSource[T](
      isStreamingMode: Boolean,
      externalTable: ExternalCatalogTable,
      statistics: FlinkStatistic)
    : TableSourceTable[T] = {
    val source = if (isModeCompatibleWithTable(isStreamingMode, externalTable)) {
      TableFactoryUtil.findAndCreateTableSource(externalTable)
    } else {
      throw new ValidationException(
        "External catalog table does not support the current environment for a table source.")
    }

    new TableSourceTable[T](source.asInstanceOf[TableSource[T]], isStreamingMode, statistics)
  }

  private def isModeCompatibleWithTable[T](
      isStreamingMode: Boolean,
      externalTable: ExternalCatalogTable)
    : Boolean = {
    !isStreamingMode && externalTable.isBatchTable || isStreamingMode && externalTable.isStreamTable
  }
}
