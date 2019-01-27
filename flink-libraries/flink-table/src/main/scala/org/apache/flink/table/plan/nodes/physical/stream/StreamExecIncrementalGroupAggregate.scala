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
import org.apache.flink.table.api.types.{DataType, TypeConverters}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedAggsHandleFunction}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.{AggregateInfoList, AggregateNameUtil, AggregateUtil, FlinkRexUtil, StreamExecUtil}
import org.apache.flink.table.runtime.aggregate.MiniBatchIncrementalGroupAggFunction
import org.apache.flink.table.runtime.bundle.KeyedBundleOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.tools.RelBuilder

/**
  *
  * Flink RelNode for data stream unbounded incremental group aggregate. The incremental aggregate
  * accept
  *
  * @param cluster          Cluster of the RelNode, represent for an environment of related
  *                         relational expressions during the optimization of a query.
  * @param traitSet         Trait set of the RelNode
  * @param inputNode        The input RelNode of aggregation
  * @param aggInputRowType  the type of real input of this aggregation,
  *                         which is the input node of partial local aggregate
  * @param outputDataType   the output rel datatype of this aggregate
  * @param partialAggInfoList   The information list about the node's local aggregates
  *                             which use heap dataviews
  * @param finalAggInfoList     The information list about the node's global aggregates
  *                             which use state dataviews
  * @param finalAggCalls    The aggregate calls of final aggregate
  * @param shuffleKey       The shuffleKey is used to partition data from preceding operator
  * @param groupKey         The groupKey is used to reduce accumulator by the key
  */
class StreamExecIncrementalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggInputRowType: RelDataType,
    outputDataType: RelDataType,
    val partialAggInfoList: AggregateInfoList,
    finalAggInfoList: AggregateInfoList,
    val finalAggCalls: Seq[AggregateCall],
    val shuffleKey: Array[Int],
    val groupKey: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputDataType

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesUpdates = false

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecIncrementalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      aggInputRowType,
      outputDataType,
      partialAggInfoList,
      finalAggInfoList,
      finalAggCalls,
      shuffleKey,
      groupKey)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("shuffleKey", AggregateNameUtil.groupingToString(inputNode.getRowType, shuffleKey))
      .item("groupKey", AggregateNameUtil.groupingToString(inputNode.getRowType, groupKey))
      .item("select", AggregateNameUtil.streamAggregationToString(
        inputNode.getRowType,
        getRowType,
        finalAggInfoList,
        groupKey,
        shuffleKey = Some(shuffleKey)))
  }

  override def isDeterministic: Boolean = {
    (partialAggInfoList.getActualAggregateCalls ++
      finalAggInfoList.getActualAggregateCalls ++
      finalAggCalls)
      .forall(c => FlinkRexUtil.isDeterministicOperator(c.getAggregation))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val inputRowType = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(outputDataType)
    val opName = getOperatorName

    val partialAggsHandler = generateAggsHandler(
      "PartialGroupAggsHandler",
      partialAggInfoList,
      mergedAccOffset = shuffleKey.length,
      partialAggInfoList.getAccTypes,
      tableEnv.getConfig,
      tableEnv.getRelBuilder,
      // the partial aggregate accumulators will be buffered, so need copy
      inputFieldCopy = true)

    val finalAggsHandler = generateAggsHandler(
      "FinalGroupAggsHandler",
      finalAggInfoList,
      mergedAccOffset = 0,
      partialAggInfoList.getAccTypes,
      tableEnv.getConfig,
      tableEnv.getRelBuilder,
      // the final aggregate accumulators is not buffered
      inputFieldCopy = false)

    val shuffleKeySelector = StreamExecUtil.getKeySelector(shuffleKey, inputRowType)
    val groupKeySelector = StreamExecUtil.getKeySelector(
      groupKey,
      shuffleKeySelector.getProducedType.asInstanceOf[BaseRowTypeInfo])

    val aggFunction = new MiniBatchIncrementalGroupAggFunction(
      partialAggsHandler,
      finalAggsHandler,
      groupKeySelector)

    val partialAccTypes = partialAggInfoList.getAccTypes.map(
      TypeConverters.createExternalTypeInfoFromDataType)
    // the bundle buffer value type is partial acc type which contains mapview type
    val valueTypeInfo = new BaseRowTypeInfo(partialAccTypes: _*)
    val operator = new KeyedBundleOperator(
      aggFunction,
      AggregateUtil.getMiniBatchTrigger(tableEnv.getConfig),
      valueTypeInfo,
      tableEnv.getConfig.getConf.getBoolean(
        TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      opName,
      operator,
      outRowType,
      inputTransformation.getParallelism)

    if (shuffleKey.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    // set KeyType and Selector for state
    ret.setStateKeySelector(shuffleKeySelector)
    ret.setStateKeyType(shuffleKeySelector.getProducedType)
    ret
  }

  def generateAggsHandler(
    name: String,
    aggInfoList: AggregateInfoList,
    mergedAccOffset: Int,
    mergedAccExternalTypes: Array[DataType],
    config: TableConfig,
    relBuilder: RelBuilder,
    inputFieldCopy: Boolean): GeneratedAggsHandleFunction = {

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(config, supportReference = true),
      relBuilder,
      FlinkTypeFactory.toInternalFieldTypes(aggInputRowType),
      needRetract = false,
      needMerge = true,
      config.getNullCheck,
      inputFieldCopy)

    generator
    .withMerging(mergedAccOffset, mergedAccOnHeap = true, mergedAccExternalTypes)
    .generateAggsHandler(name, aggInfoList)
  }

  private def getOperatorName: String = {
    val shuffleKeyToStr =
      s"shuffleKey: (${AggregateNameUtil.groupingToString(inputNode.getRowType, shuffleKey)})"
    val groupKeyToStr =
      s"groupKey: (${AggregateNameUtil.groupingToString(inputNode.getRowType, groupKey)})"
    val selectToStr = s"select: (${AggregateNameUtil.streamAggregationToString(
      inputNode.getRowType,
      getRowType,
      finalAggInfoList,
      groupKey,
      shuffleKey = Some(shuffleKey))})"
    s"IncrementalGroupAggregate($shuffleKeyToStr, $groupKeyToStr, $selectToStr)"
  }
}

