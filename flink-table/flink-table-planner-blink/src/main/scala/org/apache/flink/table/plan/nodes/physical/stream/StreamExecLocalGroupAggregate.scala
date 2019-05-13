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
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.PartialFinalType
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util._
import org.apache.flink.table.runtime.aggregate.MiniBatchLocalGroupAggFunction
import org.apache.flink.table.runtime.bundle.MapBundleOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.api.java.functions.KeySelector

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for unbounded global group aggregate.
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
  with StreamExecNode[BaseRow] {

  override def producesUpdates = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions = true

  override def producesRetractions: Boolean = false

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

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val inRowType = FlinkTypeFactory.toInternalRowType(getInput.getRowType)
    val outRowType = FlinkTypeFactory.toInternalRowType(outputRowType)

    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableEnv.getConfig),
      tableEnv.getRelBuilder,
      inRowType.getFieldTypes,
      needRetraction,
      // the local aggregate result will be buffered, so need copy
      copyInputField = true)
    generator.withMerging(mergedAccOffset = 0, mergedAccOnHeap = true)

    val aggsHandler = generator.generateAggsHandler("GroupAggsHandler", aggInfoList)
    val aggFunction = new MiniBatchLocalGroupAggFunction(aggsHandler)

    val inputTypeInfo = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val selector = KeySelectorUtil.getBaseRowSelector(grouping, inputTypeInfo)

    val operator = new MapBundleOperator(
      aggFunction,
      AggregateUtil.createMiniBatchTrigger(tableEnv.getConfig),
      selector.asInstanceOf[KeySelector[BaseRow, BaseRow]])

    val transformation = new OneInputTransformation(
      inputTransformation,
      "LocalGroupAggregate",
      operator,
      outRowType.toTypeInfo,
      inputTransformation.getParallelism)

    transformation
  }
}
