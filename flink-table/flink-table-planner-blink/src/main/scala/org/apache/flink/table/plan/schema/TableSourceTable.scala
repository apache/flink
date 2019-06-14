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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.table.{JHashSet, JSet}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{TableSource, TableSourceUtil}

import java.util

/**
  * Abstract class which define the interfaces required to convert a [[TableSource]] to
  * a Calcite Table
  */
class TableSourceTable[T](
    val tableSource: TableSource[T],
    val isStreaming: Boolean)
  extends FlinkTable {

  lazy val statistic: FlinkStatistic = buildStatistic()

  // TODO implements this
  // TableSourceUtil.validateTableSource(tableSource)

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    TableSourceUtil.getRelDataType(
      tableSource,
      None,
      streaming = false,
      typeFactory.asInstanceOf[FlinkTypeFactory])
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
    new TableSourceTable(tableSource, isStreaming)
  }

  private def buildStatistic(): FlinkStatistic = {
    // combine primary key and unique keys together
    val ukSet = new JHashSet[JSet[String]]()
    val schema = tableSource.getTableSchema
    if (schema.getPrimaryKey.isPresent) {
      val pk = util.Arrays.asList(schema.getPrimaryKey.get(): _*)
      ukSet.add(new JHashSet[String](pk))
    }
    if (schema.getUniqueKeys.isPresent) {
      schema.getUniqueKeys.get().foreach { uk =>
        ukSet.add(new JHashSet[String](util.Arrays.asList(uk: _*)))
      }
    }
    // build FlinkStatistic
    val builder = FlinkStatistic.builder()
      .tableStats(tableSource.getTableStats.orElse(null))
    if (!ukSet.isEmpty) {
      builder.uniqueKeys(ukSet)
    }
    builder.build()
  }
}
