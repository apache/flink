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

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableFunctionScan
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rel.metadata.RelColumnMapping
import org.apache.calcite.rex.{RexLiteral, RexNode, RexUtil}
import org.apache.calcite.sql.SemiJoinType
import org.apache.calcite.util.ImmutableBitSet

import java.lang.reflect.Type
import java.util

/**
  * Sub-class of [[TableFunctionScan]] that is a relational expression
  * which calls a table-valued function in Flink.
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

class FlinkLogicalTableFunctionScanConverter
  extends ConverterRule(
    classOf[LogicalTableFunctionScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableFunctionScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    // TODO This rule do not match to TemporalTableFunction
    super.matches(call)
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[LogicalTableFunctionScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()

    val constantTableFunction = RexUtil.isConstant(scan.getCall) && scan.getInputs.isEmpty
    if (constantTableFunction) {
      convertConstantFunctionTableScan(scan, traitSet)
    } else {
      createFlinkLogicalTableScan(scan, traitSet)
    }
  }

  def createFlinkLogicalTableScan(
      scan: LogicalTableFunctionScan,
      traitSet: RelTraitSet): FlinkLogicalTableFunctionScan = {
    new FlinkLogicalTableFunctionScan(
      scan.getCluster,
      traitSet,
      scan.getInputs,
      scan.getCall,
      scan.getElementType,
      scan.getRowType,
      scan.getColumnMappings
    )
  }

  /**
    * Converts [[LogicalTableFunctionScan]] with constant RexCall to
    * {{{
    *                    [[FlinkLogicalCorrelate]]
    *                          /       \
    * empty [[FlinkLogicalValues]]  [[FlinkLogicalTableFunctionScan]]
    * }}}
    */
  def convertConstantFunctionTableScan(
      scan: LogicalTableFunctionScan,
      traitSet: RelTraitSet): RelNode = {
    val cluster = scan.getCluster

    // create correlate left
    val values = new FlinkLogicalValues(
      cluster,
      traitSet,
      cluster.getTypeFactory.createStructType(ImmutableList.of(), ImmutableList.of()),
      ImmutableList.of(ImmutableList.of[RexLiteral]())
    )

    // create correlate right
    val newScan = createFlinkLogicalTableScan(scan, traitSet)

    new FlinkLogicalCorrelate(
      cluster,
      traitSet,
      values,
      newScan,
      cluster.createCorrel(), // a dummy CorrelationId
      ImmutableBitSet.of(),
      SemiJoinType.INNER)
  }
}

object FlinkLogicalTableFunctionScan {
  val CONVERTER = new FlinkLogicalTableFunctionScanConverter
}
