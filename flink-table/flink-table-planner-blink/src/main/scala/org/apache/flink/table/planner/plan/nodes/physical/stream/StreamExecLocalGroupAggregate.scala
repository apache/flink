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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.PartialFinalType
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.{KeySelectorUtil, _}
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchLocalGroupAggFunction
import org.apache.flink.table.runtime.operators.bundle.MapBundleOperator
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for unbounded local group aggregate.
  *
  * @see [[StreamExecGroupAggregateBase]] for more info.
  */
class StreamExecLocalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val aggInfoList: AggregateInfoList,
    val partialFinalType: PartialFinalType)
  extends StreamExecGroupAggregateBase(cluster, traitSet, inputRel)
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecLocalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      grouping,
      aggCalls,
      aggInfoList,
      partialFinalType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType),
        grouping.nonEmpty)
      .itemIf("partialFinalType", partialFinalType, partialFinalType != PartialFinalType.NONE)
      .item("select", RelExplainUtil.streamGroupAggregationToString(
        inputRowType,
        getRowType,
        aggInfoList,
        grouping,
        isLocal = true))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val inRowType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val outRowType = FlinkTypeFactory.toLogicalRowType(outputRowType)

    val needRetraction = !ChangelogPlanUtils.inputInsertOnly(this)

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(planner.getTableConfig),
      planner.getRelBuilder,
      inRowType.getChildren,
      // the local aggregate result will be buffered, so need copy
      copyInputField = true)

    generator
      .needAccumulate()
      .needMerge(mergedAccOffset = 0, mergedAccOnHeap = true)

    if (needRetraction) {
      generator.needRetract()
    }

    val aggsHandler = generator.generateAggsHandler("GroupAggsHandler", aggInfoList)
    val aggFunction = new MiniBatchLocalGroupAggFunction(aggsHandler)

    val inputTypeInfo = inputTransformation.getOutputType.asInstanceOf[RowDataTypeInfo]
    val selector = KeySelectorUtil.getRowDataSelector(grouping, inputTypeInfo)

    val operator = new MapBundleOperator(
      aggFunction,
      AggregateUtil.createMiniBatchTrigger(planner.getTableConfig),
      selector.asInstanceOf[KeySelector[RowData, RowData]])

    val transformation = new OneInputTransformation(
      inputTransformation,
      getRelDetailedDescription,
      operator,
      RowDataTypeInfo.of(outRowType),
      inputTransformation.getParallelism)

    if (inputsContainSingleton()) {
      transformation.setParallelism(1)
      transformation.setMaxParallelism(1)
    }

    transformation
  }
}
