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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector
import org.apache.flink.table.runtime.operators.rank.{AppendOnlyTopNFunction, ComparableRecordComparator, ConstantRankRange, RankType, RetractableTopNFunction}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

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
  with LegacyStreamExecNode[RowData] {

  private lazy val limitStart: Long = SortUtil.getLimitStart(offset)
  private lazy val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

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

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    if (fetch == null) {
      throw new TableException(
        "FETCH is missed, which on streaming table is not supported currently.")
    }
    val inputRowTypeInfo = InternalTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    val tableConfig = planner.getTableConfig
    val minIdleStateRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxIdleStateRetentionTime = tableConfig.getMaxIdleStateRetentionTime

    // rankStart begin with 1
    val rankRange = new ConstantRankRange(limitStart + 1, limitEnd)
    val rankType = RankType.ROW_NUMBER
    val outputRankNumber = false
    // Use TopNFunction underlying StreamExecLimit currently
    val sortKeySelector = EmptyRowDataKeySelector.INSTANCE
    val sortKeyComparator = ComparatorCodeGenerator.gen(
      tableConfig, "AlwaysEqualsComparator", Array(), Array(), Array(), Array())

    val processFunction = if (ChangelogPlanUtils.inputInsertOnly(this)) {
      val cacheSize = tableConfig.getConfiguration.getLong(
        StreamExecRank.TABLE_EXEC_TOPN_CACHE_SIZE)
      new AppendOnlyTopNFunction(
        minIdleStateRetentionTime,
        maxIdleStateRetentionTime,
        inputRowTypeInfo,
        sortKeyComparator,
        sortKeySelector,
        rankType,
        rankRange,
        generateUpdateBefore,
        outputRankNumber,
        cacheSize)
    } else {
      val equaliserCodeGen = new EqualiserCodeGenerator(inputRowTypeInfo.toRowFieldTypes)
      val generatedEqualiser = equaliserCodeGen.generateRecordEqualiser("LimitValueEqualiser")
      val comparator = new ComparableRecordComparator(
        sortKeyComparator,
        Array(),
        Array(),
        Array(),
        Array())
      new RetractableTopNFunction(
        minIdleStateRetentionTime,
        maxIdleStateRetentionTime,
        inputRowTypeInfo,
        comparator,
        sortKeySelector,
        rankType,
        rankRange,
        generatedEqualiser,
        generateUpdateBefore,
        outputRankNumber)
    }
    val operator = new KeyedProcessOperator(processFunction)
    processFunction.setKeyContext(operator)

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val outputRowTypeInfo = InternalTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getRowType))

    // as input node is singleton exchange, its parallelism is 1.
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

    val selector = EmptyRowDataKeySelector.INSTANCE
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
