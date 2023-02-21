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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.utils.TableSqlFunction
import org.apache.flink.table.planner.plan.nodes.FlinkConventions

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.TableFunctionScan
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rel.metadata.RelColumnMapping
import org.apache.calcite.rex.{RexCall, RexNode}

import java.lang.reflect.Type
import java.util

import scala.collection.JavaConversions._

/**
 * Sub-class of [[TableFunctionScan]] that is a relational expression which calls a table-valued
 * function in Flink.
 */
class FlinkLogicalTableFunctionScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: util.List[RelNode],
    rexCall: RexNode,
    elementType: Type,
    rowType: RelDataType,
    columnMappings: util.Set[RelColumnMapping])
  extends TableFunctionScan(
    cluster,
    traitSet,
    inputs,
    rexCall,
    elementType,
    rowType,
    columnMappings)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      inputs: util.List[RelNode],
      rexCall: RexNode,
      elementType: Type,
      rowType: RelDataType,
      columnMappings: util.Set[RelColumnMapping]): TableFunctionScan = {

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

class FlinkLogicalTableFunctionScanConverter(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val logicalTableFunction: LogicalTableFunctionScan = call.rel(0)

    !isTemporalTableFunctionCall(logicalTableFunction)
  }

  private def isTemporalTableFunctionCall(
      logicalTableFunction: LogicalTableFunctionScan): Boolean = {

    if (!logicalTableFunction.getCall.isInstanceOf[RexCall]) {
      return false
    }
    val rexCall = logicalTableFunction.getCall.asInstanceOf[RexCall]
    val functionDefinition = rexCall.getOperator match {
      case tsf: TableSqlFunction => tsf.udtf
      case bsf: BridgingSqlFunction => bsf.getDefinition
      case _ => return false
    }
    functionDefinition.isInstanceOf[TemporalTableFunction]
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[LogicalTableFunctionScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    val newInputs = scan.getInputs.map(input => RelOptRule.convert(input, FlinkConventions.LOGICAL))

    new FlinkLogicalTableFunctionScan(
      scan.getCluster,
      traitSet,
      newInputs,
      scan.getCall,
      scan.getElementType,
      scan.getRowType,
      scan.getColumnMappings
    )
  }

}

object FlinkLogicalTableFunctionScan {
  val CONVERTER = new FlinkLogicalTableFunctionScanConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalTableFunctionScan],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalTableFunctionScanConverter"))
}
