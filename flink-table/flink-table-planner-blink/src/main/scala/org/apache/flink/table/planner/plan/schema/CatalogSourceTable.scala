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

import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions
import org.apache.flink.table.planner.calcite.FlinkToRelContext

import org.apache.calcite.plan.{RelOptSchema, RelOptTable}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * A [[FlinkPreparingTableBase]] implementation which defines the interfaces required to translate
  * the Calcite [[RelOptTable]] to the Flink specific [[TableSourceTable]].
  *
  * <p>This table is only used to translate the catalog table into [[TableSourceTable]]
  * during the last phrase of sql-to-rel conversion, it is overdue once the sql node was converted
  * to relational expression.
  *
  * @param tableSource The [[TableSource]] for which is converted to a Calcite Table
  * @param isStreamingMode A flag that tells if the current table is in stream mode
  * @param catalogTable Catalog table where this table source table comes from
  */
class CatalogSourceTable[T](
    relOptSchema: RelOptSchema,
    names: JList[String],
    rowType: RelDataType,
    statistic: FlinkStatistic,
    val tableSource: TableSource[T],
    val isStreamingMode: Boolean,
    val catalogTable: CatalogTable)
  extends FlinkPreparingTableBase(relOptSchema, rowType, names, statistic) {

  Preconditions.checkNotNull(tableSource)
  Preconditions.checkNotNull(statistic)
  Preconditions.checkNotNull(catalogTable)

  lazy val columnExprs: Map[String, String] = {
    catalogTable.getSchema
      .getTableColumns
      .filter(column => column.isGenerated)
      .map(column => (column.getName, column.getExpr.get()))
      .toMap
  }

  override def getQualifiedName: JList[String] = explainSourceAsString(tableSource)

  override def toRel(context: RelOptTable.ToRelContext): RelNode = {
    val cluster = context.getCluster
    val tableSourceTable = new TableSourceTable[T](
      relOptSchema,
      names,
      rowType,
      statistic,
      tableSource,
      isStreamingMode,
      catalogTable)
    if (columnExprs.isEmpty) {
      LogicalTableScan.create(cluster, tableSourceTable)
    } else {
      // Get row type of physical fields.
      val physicalFields = getRowType
        .getFieldList
        .filter(f => !columnExprs.contains(f.getName))
        .map(f => f.getIndex)
        .toArray
      // Copy this table with physical scan row type.
      val newRelTable = tableSourceTable.copy(tableSource, statistic, physicalFields)
      val scan = LogicalTableScan.create(cluster, newRelTable)
      val toRelContext = context.asInstanceOf[FlinkToRelContext]
      val relBuilder = toRelContext.createRelBuilder()
      val fieldNames = rowType.getFieldNames.asScala
      val fieldExprs = fieldNames
        .map { name =>
          if (columnExprs.contains(name)) {
            columnExprs(name)
          } else {
            name
          }
        }.toArray
      val rexNodes = toRelContext
        .createSqlExprToRexConverter(newRelTable.getRowType)
        .convertToRexNodes(fieldExprs)
      relBuilder.push(scan)
        .projectNamed(rexNodes.toList, fieldNames, true)
        .build()
    }
  }
}
