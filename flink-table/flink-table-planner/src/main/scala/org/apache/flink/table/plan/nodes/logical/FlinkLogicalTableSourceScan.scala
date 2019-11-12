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

package org.apache.flink.table.plan.nodes.logical

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.{FilterableTableSource, TableSource, TableSourceUtil}

import scala.collection.JavaConverters._

class FlinkLogicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    val tableSource: TableSource[_],
    val selectedFields: Option[Array[Int]])
  extends TableScan(cluster, traitSet, table)
  with FlinkLogicalRel {

  def copy(
      traitSet: RelTraitSet,
      tableSource: TableSource[_],
      selectedFields: Option[Array[Int]]): FlinkLogicalTableSourceScan = {
    new FlinkLogicalTableSourceScan(cluster, traitSet, getTable, tableSource, selectedFields)
  }

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val streamingTable = table.unwrap(classOf[TableSourceTable[_]]).isStreamingMode

    TableSourceUtil.getRelDataType(tableSource, selectedFields, streamingTable, flinkTypeFactory)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)

    val adjustedCnt: Double = tableSource match {
      case f: FilterableTableSource[_] if f.isFilterPushedDown =>
        // ensure we prefer FilterableTableSources with pushed-down filters.
        rowCnt - 1.0
      case _ =>
        rowCnt
    }

    planner.getCostFactory.makeCost(
      adjustedCnt,
      adjustedCnt,
      adjustedCnt * estimateRowSize(getRowType))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val terms = super.explainTerms(pw)
        .item("fields", tableSource.getTableSchema.getFieldNames.mkString(", "))

    val sourceDesc = tableSource.explainSource()
    if (sourceDesc.nonEmpty) {
      terms.item("source", sourceDesc)
    } else {
      terms
    }
  }

  override def toString: String = {
    val tableName = getTable.getQualifiedName
    val s = s"table:$tableName, fields:(${getRowType.getFieldNames.asScala.toList.mkString(", ")})"

    val sourceDesc = tableSource.explainSource()
    if (sourceDesc.nonEmpty) {
      s"Scan($s, source:$sourceDesc)"
    } else {
      s"Scan($s)"
    }
  }
}

class FlinkLogicalTableSourceScanConverter
  extends ConverterRule(
    classOf[LogicalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableSourceScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel[TableScan](0)
    scan.getTable.unwrap(classOf[TableSourceTable[_]]) != null
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[TableScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)

    new FlinkLogicalTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      scan.getTable.unwrap(classOf[TableSourceTable[_]]).tableSource,
      None
    )
  }
}

object FlinkLogicalTableSourceScan {
  val CONVERTER = new FlinkLogicalTableSourceScanConverter
}
