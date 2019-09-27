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

package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.sources.TableSourceUtil
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}

/**
  * Abstract class which define the interfaces required to convert a [[TableSource]] to
  * a Calcite Table
  *
  * @param tableSource The [[TableSource]] for which is converted to a Calcite Table.
  * @param isStreamingMode A flag that tells if the current table is in stream mode.
  * @param statistic The table statistics.
  */
class TableSourceTable[T](
    val tableSource: TableSource[T],
    val isStreamingMode: Boolean,
    val statistic: FlinkStatistic,
    val selectedFields: Option[Array[Int]])
  extends FlinkTable {

  def this(tableSource: TableSource[T], isStreamingMode: Boolean, statistic: FlinkStatistic) {
    this(tableSource, isStreamingMode, statistic, None)
  }

  // TODO implements this
  // TableSourceUtil.validateTableSource(tableSource)

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    TableSourceUtil.getRelDataType(
      tableSource,
      selectedFields,
      streaming = isStreamingMode,
      typeFactory.asInstanceOf[FlinkTypeFactory])
  }

  /**
    * Creates a copy of this table, changing statistic.
    *
    * @param statistic A new FlinkStatistic.
    * @return Copy of this table, substituting statistic.
    */
  override def copy(statistic: FlinkStatistic): TableSourceTable[T] = {
    new TableSourceTable(tableSource, isStreamingMode, statistic)
  }

  /**
    * Returns statistics of current table.
    */
  override def getStatistic: FlinkStatistic = statistic

  /**
    * Replaces table source with the given one, and create a new table source table.
    *
    * @param tableSource tableSource to replace.
    * @return new TableSourceTable
    */
  def replaceTableSource(tableSource: TableSource[T]): TableSourceTable[T] = {
    new TableSourceTable[T](tableSource, isStreamingMode, statistic)
  }
}
