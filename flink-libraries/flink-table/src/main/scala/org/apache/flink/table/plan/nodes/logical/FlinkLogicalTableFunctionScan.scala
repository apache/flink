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

import java.lang.reflect.Type
import java.util.{List => JList, Set => JSet}

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical.{LogicalTableFunctionScan, LogicalTableScan}
import org.apache.calcite.rel.metadata.RelColumnMapping
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.TableSourceTable

class FlinkLogicalTableFunctionScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: JList[RelNode],
    rexCall: RexNode,
    elementType: Type,
    rowType: RelDataType,
    columnMappings: JSet[RelColumnMapping])
  extends TableFunctionScan(
    cluster,
    traitSet,
    inputs,
    rexCall,
    elementType,
    rowType,
    columnMappings)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet,
      inputs: JList[RelNode],
      rexCall: RexNode,
      elementType: Type,
      rowType: RelDataType,
      columnMappings: JSet[RelColumnMapping]): TableFunctionScan = {

    new FlinkLogicalTableFunctionScan(
      cluster,
      traitSet,
      inputs,
      rexCall,
      elementType,
      rowType,
      columnMappings)
  }
}

class FlinkLogicalTableFunctionScanConverter
  extends ConverterRule(
    classOf[LogicalTableFunctionScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableFunctionScanConverter") {

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[LogicalTableFunctionScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    new FlinkLogicalTableFunctionScan(
      rel.getCluster,
      traitSet,
      scan.getInputs,
      scan.getCall,
      scan.getElementType,
      scan.getRowType,
      scan.getColumnMappings
    )
  }
}

object FlinkLogicalTableFunctionScan {
  val CONVERTER = new FlinkLogicalTableFunctionScanConverter
}
