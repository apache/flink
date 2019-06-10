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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonCalc
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.util.RelExplainUtil
import org.apache.flink.table.runtime.AbstractProcessStreamOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Calc]].
  */
class StreamExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends CommonCalc(cluster, traitSet, inputRel, calcProgram)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamExecCalc(cluster, traitSet, child, program, outputRowType)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] =
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]
    // materialize time attributes in condition
    val condition = if (calcProgram.getCondition != null) {
      Some(calcProgram.expandLocalRef(calcProgram.getCondition))
    } else {
      None
    }

    // TODO deal time indicators.
//    val condition = if (calcProgram.getCondition != null) {
//      val materializedCondition = RelTimeIndicatorConverter.convertExpression(
//        calcProgram.expandLocalRef(calcProgram.getCondition),
//        input.getRowType,
//        cluster.getRexBuilder)
//      Some(materializedCondition)
//    } else {
//      None
//    }

    val ctx = CodeGeneratorContext(config).setOperatorBaseClass(
      classOf[AbstractProcessStreamOperator[BaseRow]])
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val substituteStreamOperator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      cluster,
      inputTransform,
      outputType,
      config,
      calcProgram,
      condition,
      retainHeader = true,
      "StreamExecCalc"
    )
    new OneInputTransformation(
      inputTransform,
      RelExplainUtil.calcToString(calcProgram, getExpressionString),
      substituteStreamOperator,
      BaseRowTypeInfo.of(outputType),
      inputTransform.getParallelism)
  }
}
