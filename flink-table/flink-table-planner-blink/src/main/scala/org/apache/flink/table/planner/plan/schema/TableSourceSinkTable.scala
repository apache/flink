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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.stats.FlinkStatistic

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}

/**
  * Wrapper for both a [[TableSourceTable]] and [[TableSinkTable]] under a common name.
  *
  * @param tableSourceTable table source table (if available)
  * @param tableSinkTable table sink table (if available)
  * @tparam T1 type of the table source table
  * @tparam T2 type of the table sink table
  */
class TableSourceSinkTable[T1, T2](
    val tableSourceTable: Option[TableSourceTable[T1]],
    val tableSinkTable: Option[TableSinkTable[T2]])
  extends FlinkTable {

  // In the streaming case, the table schema of source and sink can differ because of extra
  // rowtime/proctime fields. We will always return the source table schema if tableSourceTable
  // is not None, otherwise return the sink table schema. We move the Calcite validation logic of
  // the sink table schema into Flink. This allows us to have different schemas as source and sink
  // of the same table.
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    tableSourceTable.map(_.getRowType(typeFactory))
      .orElse(tableSinkTable.map(_.getRowType(typeFactory)))
      .getOrElse(throw new TableException("Unable to get row type of table source sink table."))
  }

  override def getStatistic: FlinkStatistic = {
    tableSourceTable.map(_.getStatistic)
      .orElse(tableSinkTable.map(_.getStatistic))
      .getOrElse(throw new TableException("Unable to get statistics of table source sink table."))
  }

  override def copy(statistic: FlinkStatistic): FlinkTable = {
    new TableSourceSinkTable[T1, T2](
      tableSourceTable.map(source => source.copy(statistic)),
      tableSinkTable.map(sink => sink.copy(statistic).asInstanceOf[TableSinkTable[T2]]))
  }

  // Look up the tableSourceTable and tableSinkTable to find proper table type, this method must be
  // invoked every time to decide if the table source or sink can be deterministic.
  override def unwrap[T](clazz: Class[T]): T = {
    if (clazz.isInstance(this)) {
      clazz.cast(this)
    } else if (tableSourceTable.nonEmpty && clazz.isInstance(tableSourceTable.get)) {
      clazz.cast(tableSourceTable.get)
    } else if (tableSinkTable.nonEmpty && clazz.isInstance(tableSinkTable.get)) {
      clazz.cast(tableSinkTable.get)
    } else {
      null.asInstanceOf[T]
    }
  }
}
