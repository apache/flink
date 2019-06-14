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

import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.EqualiserCodeGenerator
import org.apache.flink.table.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.Rank
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util._
import org.apache.flink.table.runtime.rank._
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Rank]].
  */
class StreamExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    partitionKey: ImmutableBitSet,
    orderKey: RelCollation,
    rankType: RankType,
    rankRange: RankRange,
    rankNumberType: RelDataTypeField,
    outputRankNumber: Boolean)
  extends Rank(
    cluster,
    traitSet,
    inputRel,
    partitionKey,
    orderKey,
    rankType,
    rankRange,
    rankNumberType,
    outputRankNumber)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  /** please uses [[getStrategy]] instead of this field */
  private var strategy: RankProcessStrategy = _

  def getStrategy(forceRecompute: Boolean = false): RankProcessStrategy = {
    if (strategy == null || forceRecompute) {
      strategy = RankProcessStrategy.analyzeRankProcessStrategy(
        inputRel, partitionKey, orderKey, cluster.getMetadataQuery)
    }
    strategy
  }

  override def producesUpdates = true

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = {
    getStrategy(forceRecompute = true) == RetractStrategy
  }

  override def consumesRetractions = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecRank(
      cluster,
      traitSet,
      inputs.get(0),
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = inputRel.getRowType
    pw.input("input", getInput)
      .item("strategy", getStrategy())
      .item("rankType", rankType)
      .item("rankRange", rankRange.toString(inputRowType.getFieldNames))
      .item("partitionBy", RelExplainUtil.fieldToString(partitionKey.toArray, inputRowType))
      .item("orderBy", RelExplainUtil.collationToString(orderKey, inputRowType))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val tableConfig = tableEnv.getConfig
    rankType match {
      case RankType.ROW_NUMBER => // ignore
      case RankType.RANK =>
        throw new TableException("RANK() on streaming table is not supported currently")
      case RankType.DENSE_RANK =>
        throw new TableException("DENSE_RANK() on streaming table is not supported currently")
      case k =>
        throw new TableException(s"Streaming tables do not support $k rank function.")
    }

    val inputRowTypeInfo = BaseRowTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val fieldCollations = orderKey.getFieldCollations
    val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations)
    val sortKeySelector = KeySelectorUtil.getBaseRowSelector(sortFields, inputRowTypeInfo)
    val sortKeyType = sortKeySelector.getProducedType
    val sortKeyComparator = ComparatorCodeGenerator.gen(tableConfig, "StreamExecSortComparator",
      sortFields.indices.toArray, sortKeyType.getLogicalTypes, sortDirections, nullsIsLast)
    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val cacheSize = tableConfig.getConf.getLong(TableConfigOptions.SQL_EXEC_TOPN_CACHE_SIZE)
    val minIdleStateRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxIdleStateRetentionTime = tableConfig.getMaxIdleStateRetentionTime

    val processFunction = getStrategy(true) match {
      case AppendFastStrategy =>
        new AppendOnlyTopNFunction(
          minIdleStateRetentionTime,
          maxIdleStateRetentionTime,
          inputRowTypeInfo,
          sortKeyComparator,
          sortKeySelector,
          rankType,
          rankRange,
          generateRetraction,
          outputRankNumber,
          cacheSize)

      case UpdateFastStrategy(primaryKeys) =>
        val rowKeySelector = KeySelectorUtil.getBaseRowSelector(primaryKeys, inputRowTypeInfo)
        new UpdatableTopNFunction(
          minIdleStateRetentionTime,
          maxIdleStateRetentionTime,
          inputRowTypeInfo,
          rowKeySelector,
          sortKeyComparator,
          sortKeySelector,
          rankType,
          rankRange,
          generateRetraction,
          outputRankNumber,
          cacheSize)

      // TODO Use UnaryUpdateTopNFunction after SortedMapState is merged
      case RetractStrategy =>
        val equaliserCodeGen = new EqualiserCodeGenerator(inputRowTypeInfo.getLogicalTypes)
        val generatedEqualiser = equaliserCodeGen.generateRecordEqualiser("RankValueEqualiser")

        new RetractableTopNFunction(
          minIdleStateRetentionTime,
          maxIdleStateRetentionTime,
          inputRowTypeInfo,
          sortKeyComparator,
          sortKeySelector,
          rankType,
          rankRange,
          generatedEqualiser,
          generateRetraction,
          outputRankNumber)
    }
    val rankOpName = getOperatorName
    val operator = new KeyedProcessOperator(processFunction)
    processFunction.setKeyContext(operator)
    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val outputRowTypeInfo = BaseRowTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getRowType))
    val ret = new OneInputTransformation(
      inputTransform,
      rankOpName,
      operator,
      outputRowTypeInfo,
      getResource.getParallelism)

    if (getResource.getMaxParallelism > 0) {
      ret.setMaxParallelism(getResource.getMaxParallelism)
    }

    // set KeyType and Selector for state
    val selector = KeySelectorUtil.getBaseRowSelector(partitionKey.toArray, inputRowTypeInfo)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  private def getOperatorName: String = {
    val inputRowType = inputRel.getRowType
    var result = getStrategy().toString
    result += s"(orderBy: (${RelExplainUtil.collationToString(orderKey, inputRowType)})"
    if (partitionKey.nonEmpty) {
      val partitionKeys = partitionKey.toArray
      result += s", partitionBy: (${RelExplainUtil.fieldToString(partitionKeys, inputRowType)})"
    }
    result += s", ${getRowType.getFieldNames.mkString(", ")}"
    result += s", ${rankRange.toString(inputRowType.getFieldNames)})"
    result
  }
}
