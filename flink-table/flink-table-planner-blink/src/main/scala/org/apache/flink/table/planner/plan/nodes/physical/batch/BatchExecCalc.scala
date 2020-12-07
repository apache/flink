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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram

/**
  * Batch physical RelNode for [[Calc]].
  */
class BatchExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends BatchExecCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new BatchExecCalc(cluster, traitSet, child, program, outputRowType)
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val condition = if (calcProgram.getCondition != null) {
      Some(calcProgram.expandLocalRef(calcProgram.getCondition))
    } else {
      None
    }
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val ctx = CodeGeneratorContext(config)
    val operator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      inputTransform,
      outputType,
      calcProgram,
      condition,
      opName = "BatchCalc"
    )

    ExecNodeUtil.createOneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      operator,
      InternalTypeInfo.of(outputType),
      inputTransform.getParallelism,
      0)
  }
}
