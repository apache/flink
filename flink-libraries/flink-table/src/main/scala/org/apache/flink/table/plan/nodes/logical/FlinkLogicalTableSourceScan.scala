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
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.{DefinedProctimeAttribute, DefinedRowtimeAttribute, TableSource}

import scala.collection.JavaConverters._

class FlinkLogicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    val tableSource: TableSource[_])
  extends TableScan(cluster, traitSet, table)
  with FlinkLogicalRel {

  def copy(traitSet: RelTraitSet, tableSource: TableSource[_]): FlinkLogicalTableSourceScan = {
    new FlinkLogicalTableSourceScan(cluster, traitSet, getTable, tableSource)
  }

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val fieldNames = TableEnvironment.getFieldNames(tableSource).toList
    val fieldTypes = TableEnvironment.getFieldTypes(tableSource.getReturnType).toList

    val fieldCnt = fieldNames.length

    val rowtime = tableSource match {
      case timeSource: DefinedRowtimeAttribute if timeSource.getRowtimeAttribute != null =>
        val rowtimeAttribute = timeSource.getRowtimeAttribute
        Some((fieldCnt, rowtimeAttribute))
      case _ =>
        None
    }

    val proctime = tableSource match {
      case timeSource: DefinedProctimeAttribute if timeSource.getProctimeAttribute != null =>
        val proctimeAttribute = timeSource.getProctimeAttribute
        Some((fieldCnt + (if (rowtime.isDefined) 1 else 0), proctimeAttribute))
      case _ =>
        None
    }

    flinkTypeFactory.buildLogicalRowType(
      fieldNames,
      fieldTypes,
      rowtime,
      proctime)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val terms = super.explainTerms(pw)
        .item("fields", TableEnvironment.getFieldNames(tableSource).mkString(", "))

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
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    tableSourceTable match {
      case _: TableSourceTable[_] => true
      case _ => false
    }
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[TableScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val tableSource = scan.getTable.unwrap(classOf[TableSourceTable[_]]).tableSource

    new FlinkLogicalTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      tableSource
    )
  }
}

object FlinkLogicalTableSourceScan {
  val CONVERTER = new FlinkLogicalTableSourceScanConverter
}
