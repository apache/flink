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
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.RelExplainUtil._
import org.apache.flink.table.plan.util.{RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.keyselector.NullBinaryRowKeySelector
import org.apache.flink.table.runtime.rank.{AppendOnlyTopNFunction, ConstantRankRange, RankType, RetractableTopNFunction}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Sort]].
  *
  * This node will output `limit` records beginning with the first `offset` records without sort.
  */
class StreamExecLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    offset: RexNode,
    fetch: RexNode)
  extends Sort(
    cluster,
    traitSet,
    inputRel,
    RelCollations.EMPTY,
    offset,
    fetch)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  private lazy val limitStart: Long = SortUtil.getLimitStart(offset)
  private lazy val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamExecLimit(cluster, traitSet, newInput, offset, fetch)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("offset", limitStart)
      .item("fetch", RelExplainUtil.fetchToString(fetch))
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
    if (fetch == null) {
      throw new TableException(
        "FETCH is missed, which on streaming table is not supported currently.")
    }
    val inputRowTypeInfo = BaseRowTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val tableConfig = tableEnv.getConfig
    val minIdleStateRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxIdleStateRetentionTime = tableConfig.getMaxIdleStateRetentionTime

    // rankStart begin with 1
    val rankRange = new ConstantRankRange(limitStart + 1, limitEnd)
    val rankType = RankType.ROW_NUMBER
    val outputRankNumber = false
    // Use TopNFunction underlying StreamExecLimit currently
    val sortKeySelector = NullBinaryRowKeySelector.INSTANCE
    val sortKeyComparator = ComparatorCodeGenerator.gen(
      tableConfig, "AlwaysEqualsComparator", Array(), Array(), Array(), Array())

    val processFunction = if (generateRetraction) {
      val cacheSize = tableConfig.getConf.getLong(TableConfigOptions.SQL_EXEC_TOPN_CACHE_SIZE)
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
    } else {
      val equaliserCodeGen = new EqualiserCodeGenerator(inputRowTypeInfo.getLogicalTypes)
      val generatedEqualiser = equaliserCodeGen.generateRecordEqualiser("LimitValueEqualiser")
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

    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val outputRowTypeInfo = BaseRowTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getRowType))

    // as input node is singleton exchange, its parallelism is 1.
    val ret = new OneInputTransformation(
      inputTransform,
      s"Limit(offset: $limitStart, fetch: ${fetchToString(fetch)})",
      operator,
      outputRowTypeInfo,
      getResource.getParallelism)

    if (getResource.getMaxParallelism > 0) {
      ret.setMaxParallelism(getResource.getMaxParallelism)
    }

    val selector = NullBinaryRowKeySelector.INSTANCE
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
