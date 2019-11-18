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
import org.apache.flink.table.sources.{TableSource, TableSourceValidation}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions
import org.apache.flink.table.api.{TableException, WatermarkSpec}
import org.apache.flink.table.planner.calcite.FlinkToRelContext

import org.apache.calcite.plan.{RelOptSchema, RelOptTable}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Abstract class which define the implementations required to translate
  * the Calcite [[RelOptTable]] to the Flink specific relational expression with [[TableSource]],
  * i.e. The computed column and watermark strategy specifications.
  *
  * <p>It also defines the [[copy]] method used for push down rules.
  *
  * @param tableSource The [[TableSource]] for which is converted to a Calcite Table
  * @param isStreamingMode A flag that tells if the current table is in stream mode
  * @param catalogTable Catalog table where this table source table comes from
  */
class TableSourceTable[T](
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

  val watermarkSpec: Option[WatermarkSpec] = catalogTable
    .getSchema
    // we only support single watermark currently
    .getWatermarkSpecs.asScala.headOption

  if (TableSourceValidation.hasRowtimeAttribute(tableSource) && watermarkSpec.isDefined) {
    throw new TableException(
        "If watermark is specified in DDL, the underlying TableSource of connector shouldn't" +
          " return an non-empty list of RowtimeAttributeDescriptor" +
          " via DefinedRowtimeAttributes interface.")
  }

  override def toRel(context: RelOptTable.ToRelContext): RelNode = {
    val cluster = context.getCluster
    if (columnExprs.isEmpty) {
      LogicalTableScan.create(cluster, this)
    } else {
      if (!context.isInstanceOf[FlinkToRelContext]) {
        // If the transform comes from a RelOptRule,
        // returns the scan directly.
        return LogicalTableScan.create(cluster, this)
      }
      // Get row type of physical fields.
      val physicalFields = getRowType
        .getFieldList
        .filter(f => !columnExprs.contains(f.getName))
        .toList
      val scanRowType = relOptSchema.getTypeFactory.createStructType(physicalFields)
      // Copy this table with physical scan row type.
      val newRelTable = copy(tableSource, statistic, scanRowType)
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
        .createSqlExprToRexConverter(scanRowType)
        .convertToRexNodes(fieldExprs)
      relBuilder.push(scan)
        .projectNamed(rexNodes.toList, fieldNames, true)
        .build()
    }
  }

  override def getQualifiedName: JList[String] = explainSourceAsString(tableSource)

  /**
    * Creates a copy of this table, changing table source and statistic.
    *
    * @param tableSource tableSource to replace
    * @param statistic New FlinkStatistic to replace
    * @return New TableSourceTable instance with specified table source and [[FlinkStatistic]]
    */
  def copy(tableSource: TableSource[_], statistic: FlinkStatistic): TableSourceTable[T] = {
    copy(tableSource, statistic, rowType)
  }

  /**
    * Creates a copy of this table, changing table source, statistic, rowType and selected fields.
    *
    * @param tableSource tableSource to replace
    * @param statistic New FlinkStatistic to replace
    * @param rowType New row type of this table
    * @return New TableSourceTable instance with specified table source, [[FlinkStatistic]],
    *         row type and selected fields
    */
  def copy(tableSource: TableSource[_], statistic: FlinkStatistic,
      rowType: RelDataType): TableSourceTable[T] = {
    new TableSourceTable[T](relOptSchema, names, rowType, statistic,
      tableSource.asInstanceOf[TableSource[T]], isStreamingMode, catalogTable)
  }
}
