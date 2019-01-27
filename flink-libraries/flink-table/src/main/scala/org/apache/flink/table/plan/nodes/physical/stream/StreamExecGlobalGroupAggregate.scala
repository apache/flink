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
import org.apache.flink.table.api.types.{DataType, TypeConverters}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedAggsHandleFunction}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.PartialFinalType
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.{AggregateInfoList, AggregateNameUtil, AggregateUtil, FlinkRexUtil, StreamExecUtil}
import org.apache.flink.table.runtime.aggregate.MiniBatchGlobalGroupAggFunction
import org.apache.flink.table.runtime.bundle.KeyedBundleOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Pair

import java.util.{ArrayList => JArrayList, List => JList}

/**
  *
  * Flink RelNode for data stream unbounded global group aggregate
  *
  * @param cluster          Cluster of the RelNode, represent for an environment of related
  *                         relational expressions during the optimization of a query.
  * @param traitSet         Trait set of the RelNode
  * @param inputNode        The input RelNode of aggregation which is local/incremental aggregate
  * @param localAggInfoList      The information list about the node's local aggregates
  *                              which use heap dataviews
  * @param globalAggInfoList      The information list about the node's global aggregates
  *                               which use state dataviews
  * @param outputDataType   The type emitted by this RelNode
  * @param groupings        The position (in the input Row) of the grouping keys
  * @param partialFinal     Whether the aggregate is partial agg or final agg or normal agg
  */
class StreamExecGlobalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val localAggInfoList: AggregateInfoList,
    val globalAggInfoList: AggregateInfoList,
    val aggInputRowType: RelDataType,
    outputDataType: RelDataType,
    val groupings: Array[Int],
    val partialFinal: PartialFinalType)
  extends SingleRel(cluster, traitSet, inputNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputDataType

  def getGroupings: Array[Int] = groupings

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGlobalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      localAggInfoList,
      globalAggInfoList,
      aggInputRowType,
      outputDataType,
      groupings,
      partialFinal)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",
        AggregateNameUtil.groupingToString(inputNode.getRowType, groupings), groupings.nonEmpty)
      .item("select", AggregateNameUtil.streamAggregationToString(
        inputNode.getRowType,
        getRowType,
        globalAggInfoList,
        groupings,
        isGlobal = true))
  }

  @VisibleForTesting
  def explainAgg: JList[Pair[String, AnyRef]] = {
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(
      Pair.of("groupBy", AggregateNameUtil.groupingToString(inputNode.getRowType, groupings)))
    values.add(Pair.of("select", AggregateNameUtil.aggregationToString(
      inputNode.getRowType,
      groupings,
      Array.empty[Int],
      getRowType,
      globalAggInfoList.getActualAggregateCalls,
      globalAggInfoList.getActualFunctions,
      isMerge = true,
      isGlobal = true,
      globalAggInfoList.distinctInfos)))
    values.add(Pair.of("aggs", globalAggInfoList
      .aggInfos
      .map(e => e.function.toString)
      .mkString(", ")))
    values
  }

  override def isDeterministic: Boolean = {
    (localAggInfoList.getActualAggregateCalls ++
      globalAggInfoList.getActualAggregateCalls)
      .forall(c => FlinkRexUtil.isDeterministicOperator(c.getAggregation))
  }

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

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)

    val localAggsHandler = generateAggsHandler(
      "LocalGroupAggsHandler",
      localAggInfoList,
      mergedAccOffset = groupings.length,
      mergedAccOnHeap = true,
      localAggInfoList.getAccTypes,
      tableConfig,
      tableEnv.getRelBuilder,
      // the local aggregate result will be buffered, so need copy
      inputFieldCopy = true)

    val globalAggsHandler = generateAggsHandler(
      "GlobalGroupAggsHandler",
      globalAggInfoList,
      mergedAccOffset = 0,
      mergedAccOnHeap = true,
      localAggInfoList.getAccTypes,
      tableConfig,
      tableEnv.getRelBuilder,
      // if global aggregate result will be put into state, then not need copy
      // but this global aggregate result will be put into a buffered map first,
      // then multiput to state, so it need copy
      inputFieldCopy = true)

    val globalAccTypes = globalAggInfoList.getAccTypes.map(_.toInternalType)
    val globalAggValueTypes = globalAggInfoList.getActualValueTypes.map(_.toInternalType)
    val inputCountIndex = globalAggInfoList.getCount1AccIndex

    val operator = if (tableConfig.getConf.contains(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)) {
      val aggFunction = new MiniBatchGlobalGroupAggFunction(
        localAggsHandler,
        globalAggsHandler,
        globalAccTypes,
        globalAggValueTypes,
        inputCountIndex,
        generateRetraction,
        groupings.isEmpty)

      val localAccTypes = localAggInfoList.getAccTypes.map(
        TypeConverters.createExternalTypeInfoFromDataType)
      // the bundle buffer value type is local acc type which contains mapview type
      val valueTypeInfo = new BaseRowTypeInfo(localAccTypes: _*)
      new KeyedBundleOperator(
        aggFunction,
        AggregateUtil.getMiniBatchTrigger(tableConfig),
        valueTypeInfo,
        tableConfig.getConf.getBoolean(
          TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
    } else {
      throw new TableException("Local-Global optimization is only worked in miniBatch mode")
    }

    val inputTypeInfo = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val selector = StreamExecUtil.getKeySelector(groupings, inputTypeInfo)

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      getOperatorName,
      operator,
      outRowType,
      inputTransformation.getParallelism)

    if (groupings.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  def generateAggsHandler(
    name: String,
    aggInfoList: AggregateInfoList,
    mergedAccOffset: Int,
    mergedAccOnHeap: Boolean,
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
      .withMerging(mergedAccOffset, mergedAccOnHeap, mergedAccExternalTypes)
      .generateAggsHandler(name, aggInfoList)
  }

  private def getOperatorName: String = {
    s"GlobalGroupAggregate(${
      if (!groupings.isEmpty) {
        s"groupBy: (${AggregateNameUtil.groupingToString(inputNode.getRowType, groupings)}), "
      } else {
        ""
      }
    }select:(${AggregateNameUtil.streamAggregationToString(
      inputNode.getRowType,
      getRowType,
      globalAggInfoList,
      groupings,
      isGlobal = true)}))"
  }
}

