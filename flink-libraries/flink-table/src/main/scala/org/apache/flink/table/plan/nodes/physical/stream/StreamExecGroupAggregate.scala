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
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.types.TypeConverters
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.PartialFinalType
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.AggregateUtil.transformToStreamAggregateInfoList
import org.apache.flink.table.plan.util.{AggregateInfoList, AggregateNameUtil, AggregateUtil, StreamExecUtil}
import org.apache.flink.table.runtime.KeyedProcessOperator
import org.apache.flink.table.runtime.aggregate.{GroupAggFunction, MiniBatchGroupAggFunction}
import org.apache.flink.table.runtime.bundle.KeyedBundleOperator
import org.apache.flink.table.typeutils._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.{ImmutableBitSet, Pair}

import java.util
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._

/**
  *
  * Flink RelNode for data stream unbounded group aggregate
  *
  * @param cluster          Cluster of the RelNode, represent for an environment of related
  *                         relational expressions during the optimization of a query.
  * @param traitSet         Trait set of the RelNode
  * @param inputNode        The input RelNode of aggregation
  * @param aggCalls         List of calls to aggregate functions
  * @param outputDataType   The type emitted by this RelNode
  * @param groupSet        The position (in the input Row) of the grouping keys
  * @param partialFinal    Whether the aggregate is partial agg or final agg or normal agg
  */
class StreamExecGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val aggCalls: Seq[AggregateCall],
    outputDataType: RelDataType,
    groupSet: ImmutableBitSet,
    var partialFinal: PartialFinalType = PartialFinalType.NORMAL)
  extends SingleRel(cluster, traitSet, inputNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputDataType

  def setPartialFinal(partialFinal: PartialFinalType): Unit = this.partialFinal = partialFinal

  def getGroupSet: ImmutableBitSet = groupSet

  def getGroupings: Array[Int] = groupings

  val groupings: Array[Int] = groupSet.toArray

  val aggInfoList: AggregateInfoList = {
    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)
    val modifiedMono = FlinkRelMetadataQuery.reuseOrCreate(cluster.getMetadataQuery)
      .getRelModifiedMonotonicity(this)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      groupings.length, needRetraction, modifiedMono, aggCalls)
    transformToStreamAggregateInfoList(
      aggCalls,
      input.getRowType,
      needRetractionArray,
      needInputCount = needRetraction,
      isStateBackendDataViews = true)
  }

  val inputRelDataType: RelDataType = getInput.getRowType

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      aggCalls,
      outputDataType,
      groupSet,
      partialFinal)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",
        AggregateNameUtil.groupingToString(inputRelDataType, groupings), groupings.nonEmpty)
      .item("select", AggregateNameUtil.streamAggregationToString(
        inputRelDataType,
        getRowType,
        aggInfoList,
        groupings))
  }

  @VisibleForTesting
  def explainAgg: JList[Pair[String, AnyRef]] = {
    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)
    val modifiedMono = cluster.getMetadataQuery.asInstanceOf[FlinkRelMetadataQuery]
      .getRelModifiedMonotonicity(this)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      groupings.length, needRetraction, modifiedMono, aggCalls)
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(Pair.of("groupBy", AggregateNameUtil.groupingToString(inputRelDataType, groupings)))
    values.add(Pair.of("select", AggregateNameUtil.aggregationToString(
      inputRelDataType,
      groupings,
      getRowType,
      aggCalls,
      Nil)))
    values.add(Pair.of("aggWithRetract", util.Arrays.toString(needRetractionArray)))
    values
  }

  override def isDeterministic: Boolean = AggregateUtil.isDeterministic(aggCalls)

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val tableConfig = tableEnv.getConfig

    if (groupings.length > 0 && tableConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn("No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val outRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(outputDataType)
    val inputRowType = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableConfig, supportReference = true),
      tableEnv.getRelBuilder,
      inputRowType.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo),
      needRetraction,
      needMerge = false,
      tableConfig.getNullCheck,
      // TODO: gemini state backend do not copy key currently, we have to copy input field
      // TODO: copy is not need when state backend is rocksdb, improve this in future
      // TODO: but other operators do not copy this input field.....
      copyInputField = true)

    val aggsHandler = generator.generateAggsHandler("GroupAggsHandler", aggInfoList)
    val accTypes = aggInfoList.getAccTypes.map(_.toInternalType)
    val aggValueTypes = aggInfoList.getActualValueTypes.map(_.toInternalType)
    val inputCountIndex = aggInfoList.getCount1AccIndex

    val operator = if (tableConfig.getConf.contains(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)) {
      val aggFunction = new MiniBatchGroupAggFunction(
        inputRowType.toInternalType,
        aggsHandler,
        accTypes,
        aggValueTypes,
        inputCountIndex,
        generateRetraction,
        groupings.isEmpty)

      // input element are all binary row as they are came from network
      val inputType = new BaseRowTypeInfo(inputRowType.getFieldTypes: _*)
      // miniBatch group agg stores list of input as bundle buffer value
      val valueType = new ListTypeInfo[BaseRow](inputType)

      new KeyedBundleOperator(
        aggFunction,
        AggregateUtil.getMiniBatchTrigger(tableConfig),
        valueType,
        tableConfig.getConf.getBoolean(
          TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
    } else {
      val aggFunction = new GroupAggFunction(
        aggsHandler,
        accTypes,
        aggValueTypes,
        inputCountIndex,
        generateRetraction,
        groupings.isEmpty,
        tableConfig)

      val operator = new KeyedProcessOperator[BaseRow, BaseRow, BaseRow](aggFunction)
      operator.setRequireState(true)
      operator
    }

    val selector = StreamExecUtil.getKeySelector(groupings, inputRowType)

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      getOperatorName,
      operator,
      outRowType,
      inputTransformation.getParallelism)

    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    if (groupings.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  private def getOperatorName: String = {
    s"GroupAggregate(${
      if (groupings.nonEmpty) {
        s"groupBy: (${AggregateNameUtil.groupingToString(inputRelDataType, groupings)}), "
      } else {
        ""
      }
    }select: (${
      AggregateNameUtil.streamAggregationToString(
        inputRelDataType,
        getRowType,
        aggInfoList,
        groupings)}))"
  }
}

