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

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableFunctionScan
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rel.metadata.RelColumnMapping
import org.apache.calcite.rex.{RexCall, RexNode, RexVisitorImpl}
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.FlinkConventions

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

  /**
    * This rule do not match to [[TemporalTableFunction]]. We do not support reading from
    * [[TemporalTableFunction]]s as TableFunctions. We expect them to be rewritten into
    * [[org.apache.flink.table.plan.nodes.datastream.DataStreamScan]] followed by for
    * example [[org.apache.flink.table.plan.nodes.datastream.DataStreamTemporalTableJoin]].
    */
  override def matches(call: RelOptRuleCall): Boolean = {
    val logicalTableFunction: LogicalTableFunctionScan = call.rel(0)

    !isTemporalTableFunctionCall(logicalTableFunction)
  }

  private def isTemporalTableFunctionCall(logicalTableFunction: LogicalTableFunctionScan)
    : Boolean = {

    if (!logicalTableFunction.getCall.isInstanceOf[RexCall]) {
      return false
    }
    val rexCall = logicalTableFunction.getCall().asInstanceOf[RexCall]
    if (!rexCall.getOperator.isInstanceOf[TableSqlFunction]) {
      return false
    }
    val tableFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    tableFunction.getTableFunction.isInstanceOf[TemporalTableFunction]
  }

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
