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

package org.apache.flink.table.plan.schema

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{StreamTableSource, TableSource, TableSourceUtil}

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}

/**
  * Class which implements the logic to convert a [[StreamTableSource]] to Calcite Table
  */
class StreamTableSourceTable[T](
    tableSource: StreamTableSource[T],
    statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends TableSourceTable(tableSource, statistic) {

  // TODO implements this
  // TableSourceUtil.validateTableSource(tableSource)

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    TableSourceUtil.getRelDataType(
      tableSource,
      None,
      streaming = true,
      typeFactory.asInstanceOf[FlinkTypeFactory])
  }

  /**
    * Creates a copy of this table, changing statistic.
    *
    * @param statistic A new FlinkStatistic.
    * @return Copy of this table, substituting statistic.
    */
  override def copy(statistic: FlinkStatistic) = new StreamTableSourceTable(tableSource, statistic)

  /**
    * replace table source with the given one, and create a new table source table.
    *
    * @param tableSource tableSource to replace.
    * @return new TableSourceTable
    */
  override def replaceTableSource(tableSource: TableSource[T]) =
    new StreamTableSourceTable(tableSource.asInstanceOf[StreamTableSource[T]], statistic)
}
