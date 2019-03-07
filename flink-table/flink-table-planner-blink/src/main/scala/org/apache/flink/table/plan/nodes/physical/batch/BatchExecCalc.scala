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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.plan.util.CalcUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexProgram

/**
  * Batch physical RelNode for [[Calc]].
  */
class BatchExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    rowRelDataType: RelDataType,
    calcProgram: RexProgram)
  extends Calc(cluster, traitSet, inputRel, calcProgram)
  with BatchPhysicalRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new BatchExecCalc(cluster, traitSet, child, getRowType, program)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("select", CalcUtil.selectionToString(calcProgram, getExpressionString))
      .itemIf("where", CalcUtil.conditionToString(calcProgram, getExpressionString),
        calcProgram.getCondition != null)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    CalcUtil.computeCost(calcProgram, planner, metadata, this)
  }

}
