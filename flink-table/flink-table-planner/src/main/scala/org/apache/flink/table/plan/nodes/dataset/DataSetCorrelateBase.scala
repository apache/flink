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
package org.apache.flink.table.plan.nodes.dataset

import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.CommonCorrelate
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexNode}

/**
  * Base RelNode for data set correlate.
  */
abstract class DataSetCorrelateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    relRowType: RelDataType,
    joinRowType: RelDataType,
    joinType: JoinRelType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, inputNode)
  with CommonCorrelate
  with DataSetRel {

  override def deriveRowType() = relRowType

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(getInput) * 1.5
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * 0.5)
  }

  override def toString: String = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    correlateToString(joinRowType, rexCall, sqlFunction, getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
      .item("invocation", scan.getCall)
      .item("correlate", correlateToString(
        inputNode.getRowType,
        rexCall, sqlFunction,
        getExpressionString))
      .item("select", selectToString(relRowType))
      .item("rowType", relRowType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }
}
