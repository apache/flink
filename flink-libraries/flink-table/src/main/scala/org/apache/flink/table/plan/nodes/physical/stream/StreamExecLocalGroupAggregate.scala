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

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.types.TypeConverters
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.PartialFinalType
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.{AggregateInfoList, AggregateNameUtil, AggregateUtil, StreamExecUtil}
import org.apache.flink.table.runtime.aggregate.MiniBatchLocalGroupAggFunction
import org.apache.flink.table.runtime.bundle.BundleOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.Pair

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._

/**
  *
  * Flink RelNode for data stream unbounded local group aggregate
  *
  * @param cluster          Cluster of the RelNode, represent for an environment of related
  *                         relational expressions during the optimization of a query.
  * @param traitSet         Trait set of the RelNode
  * @param inputNode        The input RelNode of aggregation
  * @param aggInfoList      The information list about the node's aggregates
  * @param outputDataType   The type of the rows emitted by this RelNode
  * @param groupings        The position (in the input Row) of the grouping keys
  * @param partialFinal     Whether the aggregate is partial agg or final agg or normal agg
  */
class StreamExecLocalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val aggInfoList: AggregateInfoList,
    val outputDataType: RelDataType,
    val groupings: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val partialFinal: PartialFinalType)
  extends SingleRel(cluster, traitSet, inputNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  val inputRelDataType: RelDataType = getInput.getRowType

  override def deriveRowType(): RelDataType = outputDataType

  def getGroupings: Array[Int] = groupings

  override def producesUpdates = false

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecLocalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      aggInfoList,
      outputDataType,
      groupings,
      aggCalls,
      partialFinal)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",
        AggregateNameUtil.groupingToString(inputRelDataType, groupings), groupings.nonEmpty)
      .item(
        "select", AggregateNameUtil.streamAggregationToString(
          inputRelDataType,
          getRowType,
          aggInfoList,
          groupings,
          isLocal = true))
  }

  @VisibleForTesting
  def explainAgg: JList[Pair[String, AnyRef]] = {
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(Pair.of("groupBy", AggregateNameUtil.groupingToString(inputRelDataType, groupings)))
    values.add(Pair.of("select", AggregateNameUtil.aggregationToString(
      inputRelDataType,
      groupings,
      Array.empty[Int],
      getRowType,
      aggInfoList.aggInfos.map(_.agg),
      aggInfoList.aggInfos.map(_.function),
      isMerge = false,
      isGlobal = false,
      aggInfoList.distinctInfos)))
    values.add(Pair.of("aggs", aggInfoList
      .aggInfos
      .map(e => e.function.toString)
      .mkString(", ")))
    values
  }

  override def isDeterministic: Boolean = AggregateUtil.isDeterministic(aggCalls)

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val inputRowType = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(outputDataType)

    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableEnv.getConfig, supportReference = true),
      tableEnv.getRelBuilder,
      inputRowType.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo),
      needRetraction,
      needMerge = true,
      tableEnv.getConfig.getNullCheck,
      // the local aggregate result will be buffered, so need copy
      copyInputField = true)

    val aggsHandler = generator.generateAggsHandler("GroupAggsHandler", aggInfoList)
    val aggFunction = new MiniBatchLocalGroupAggFunction(aggsHandler)
    val accTypes = aggInfoList.getAccTypes
    // serialize as GenericRow, deserialize as BinaryRow
    val valueTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)

    val inputTypeInfo = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val selector = StreamExecUtil.getKeySelector(groupings, inputTypeInfo)

    val operator = new BundleOperator(
      aggFunction,
      AggregateUtil.getMiniBatchTrigger(tableEnv.getConfig),
      selector.getProducedType,
      valueTypeInfo,
      selector,
      tableEnv.getConfig.getConf.getBoolean(
        TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))

    val transformation = new OneInputTransformation(
      inputTransformation,
      getOperatorName,
      operator,
      outRowType,
      inputTransformation.getParallelism)

    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName: String = {
    s"LocalGroupAggregate(${
      if (groupings.nonEmpty) {
        s"groupBy: (${AggregateNameUtil.groupingToString(inputRelDataType, groupings)}), "
      } else {
        ""
      }
    }select:(${
      AggregateNameUtil.streamAggregationToString(
        inputRelDataType,
        getRowType,
        aggInfoList,
        groupings,
        isLocal = true)}))"
  }
}

