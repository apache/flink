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

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonCalc
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Calc]].
  */
class BatchExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends CommonCalc(cluster, traitSet, inputRel, calcProgram)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new BatchExecCalc(cluster, traitSet, child, program, outputRowType)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]
    val condition = if (calcProgram.getCondition != null) {
      Some(calcProgram.expandLocalRef(calcProgram.getCondition))
    } else {
      None
    }
    val outputType = FlinkTypeFactory.toInternalRowType(getRowType)
    val ctx = CodeGeneratorContext(config)
    val operator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      cluster,
      inputTransform,
      outputType,
      config,
      calcProgram,
      condition,
      opName = "BatchCalc"
    )

    new OneInputTransformation(
      inputTransform,
      RelExplainUtil.calcToString(calcProgram, getExpressionString),
      operator,
      outputType.toTypeInfo,
      config.getConf.getInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM))
  }
}
