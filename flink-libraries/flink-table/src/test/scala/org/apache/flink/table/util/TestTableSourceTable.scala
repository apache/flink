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

package org.apache.flink.table.util

import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{StreamTableSource, TableSource}

/**
 * Test class for TableSourceTable.
 * @param tableSource
 * @param statistic
 */
class TestTableSourceTable(
    override val tableSource: TableSource,
    override val statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends TableSourceTable(tableSource, statistic) {
  /**
   * replace table source with the given one, and create a new table source table.
   *
   * @param tableSource tableSource to replace.
   * @return new TableSourceTable
   */
  override def replaceTableSource(tableSource: TableSource) = ???

  /**
   * Creates a copy of this table, changing statistic.
   *
   * @param statistic A new FlinkStatistic.
   * @return Copy of this table, substituting statistic.
   */
  override def copy(statistic: FlinkStatistic) = new TestTableSourceTable(tableSource, statistic)

  override def getRowType(relDataTypeFactory: RelDataTypeFactory) =
    relDataTypeFactory.asInstanceOf[FlinkTypeFactory]
        .buildLogicalRowType(
          tableSource.getTableSchema, tableSource.isInstanceOf[StreamTableSource[_]])
}
