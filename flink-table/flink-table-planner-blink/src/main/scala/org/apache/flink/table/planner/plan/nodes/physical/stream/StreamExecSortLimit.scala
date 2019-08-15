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
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.planner.plan.utils.{AppendFastStrategy, KeySelectorUtil, RankProcessStrategy, RelExplainUtil, RetractStrategy, SortUtil, UpdateFastStrategy}
import org.apache.flink.table.runtime.keyselector.NullBinaryRowKeySelector
import org.apache.flink.table.runtime.operators.rank.{AppendOnlyTopNFunction, ConstantRankRange, RankType, RetractableTopNFunction, UpdatableTopNFunction}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Sort]].
  *
  * This RelNode take the `limit` elements beginning with the first `offset` elements.
  **/
class StreamExecSortLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation,
    offset: RexNode,
    fetch: RexNode)
  extends Sort(cluster, traitSet, inputRel, sortCollation, offset, fetch)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  private val limitStart: Long = SortUtil.getLimitStart(offset)
  private val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

  /** please uses [[getStrategy]] instead of this field */
  private var strategy: RankProcessStrategy = _

  def getStrategy(forceRecompute: Boolean = false): RankProcessStrategy = {
    if (strategy == null || forceRecompute) {
      strategy = RankProcessStrategy.analyzeRankProcessStrategy(
        inputRel, ImmutableBitSet.of(), sortCollation, cluster.getMetadataQuery)
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

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamExecSortLimit(cluster, traitSet, newInput, newCollation, offset, fetch)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, getRowType))
      .item("offset", limitStart)
      .item("fetch", RelExplainUtil.fetchToString(fetch))
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val inputRows = mq.getRowCount(this.getInput)
    if (inputRows == null) {
      inputRows
    } else {
      val rowCount = (inputRows - limitStart).max(1.0)
      if (fetch != null) {
        rowCount.min(RexLiteral.intValue(fetch))
      } else {
        rowCount
      }
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {
    if (fetch == null) {
      throw new TableException(
        "FETCH is missed, which on streaming table is not supported currently")
    }

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]
    val inputRowTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val fieldCollations = sortCollation.getFieldCollations
    val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations)
    val sortKeySelector = KeySelectorUtil.getBaseRowSelector(sortFields, inputRowTypeInfo)
    val sortKeyType = sortKeySelector.getProducedType
    val tableConfig = planner.getTableConfig
    val sortKeyComparator = ComparatorCodeGenerator.gen(
      tableConfig,
      "StreamExecSortComparator",
      sortFields.indices.toArray,
      sortKeyType.getLogicalTypes,
      sortDirections,
      nullsIsLast)
    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val cacheSize = tableConfig.getConfiguration.getLong(StreamExecRank.TABLE_EXEC_TOPN_CACHE_SIZE)
    val minIdleStateRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxIdleStateRetentionTime = tableConfig.getMaxIdleStateRetentionTime

    // rankStart begin with 1
    val rankRange = new ConstantRankRange(limitStart + 1, limitEnd)
    val rankType = RankType.ROW_NUMBER
    val outputRankNumber = false

    // Use RankFunction underlying StreamExecSortLimit
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
    val operator = new KeyedProcessOperator(processFunction)
    processFunction.setKeyContext(operator)

    val outputRowTypeInfo = BaseRowTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getRowType))

    val ret = new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      operator,
      outputRowTypeInfo,
      inputTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    val selector = NullBinaryRowKeySelector.INSTANCE
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
