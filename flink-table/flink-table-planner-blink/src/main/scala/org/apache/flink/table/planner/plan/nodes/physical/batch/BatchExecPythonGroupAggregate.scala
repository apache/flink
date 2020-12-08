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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.ManagedMemoryUseCase
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{LegacyBatchExecNode, ExecEdge}
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecPythonGroupAggregate.ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchExecJoinRuleBase
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, RelExplainUtil}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type.{HASH_DISTRIBUTED, SINGLETON}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelCollationTraitDef, RelCollations, RelNode, RelWriter}
import org.apache.calcite.util.{ImmutableIntList, Util}

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for aggregate (Python user defined aggregate function).
  */
class BatchExecPythonGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    val aggInputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    aggFunctions: Array[UserDefinedFunction])
  extends BatchExecGroupAggregateBase(
    cluster,
    null,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    grouping,
    auxGrouping,
    aggCalls.zip(aggFunctions),
    isMerge = false,
    isFinal = true)
  with LegacyBatchExecNode[RowData]
  with CommonPythonAggregate {

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw)
      .itemIf("groupBy",
              RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("auxGrouping",
              RelExplainUtil.fieldToString(auxGrouping, inputRowType), auxGrouping.nonEmpty)
      .item("select", RelExplainUtil.groupAggregationToString(
        inputRowType,
        outputRowType,
        grouping,
        auxGrouping,
        aggCalls.zip(aggFunctions),
        isMerge = false,
        isGlobal = true))

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val canSatisfy = requiredDistribution.getType match {
      case SINGLETON => grouping.length == 0
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val groupKeysList = ImmutableIntList.of(grouping.indices.toArray: _*)
        if (requiredDistribution.requireStrict) {
          shuffleKeys == groupKeysList
        } else if (Util.startsWith(shuffleKeys, groupKeysList)) {
          // If required distribution is not strict, Hash[a] can satisfy Hash[a, b].
          // so return true if shuffleKeys(Hash[a, b]) start with groupKeys(Hash[a])
          true
        } else {
          // If partialKey is enabled, try to use partial key to satisfy the required distribution
          val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
          val partialKeyEnabled = tableConfig.getConfiguration.getBoolean(
            BatchExecJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)
          partialKeyEnabled && groupKeysList.containsAll(shuffleKeys)
        }
      case _ => false
    }
    if (!canSatisfy) {
      return None
    }

    val inputRequiredDistribution = requiredDistribution.getType match {
      case SINGLETON => requiredDistribution
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val groupKeysList = ImmutableIntList.of(grouping.indices.toArray: _*)
        if (requiredDistribution.requireStrict) {
          FlinkRelDistribution.hash(grouping, requireStrict = true)
        } else if (Util.startsWith(shuffleKeys, groupKeysList)) {
          // Hash [a] can satisfy Hash[a, b]
          FlinkRelDistribution.hash(grouping, requireStrict = false)
        } else {
          // use partial key to satisfy the required distribution
          FlinkRelDistribution.hash(shuffleKeys.map(grouping(_)).toArray, requireStrict = false)
        }
    }

    val providedCollation = if (grouping.length == 0) {
      RelCollations.EMPTY
    } else {
      val providedFieldCollations = grouping.map(FlinkRelOptUtil.ofRelFieldCollation).toList
      RelCollations.of(providedFieldCollations)
    }
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
      getTraitSet.replace(requiredDistribution).replace(requiredCollation)
    } else {
      getTraitSet.replace(requiredDistribution)
    }
    val newInput = RelOptRule.convert(getInput, inputRequiredDistribution)
    Some(copy(newProvidedTraitSet, Seq(newInput)))
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecPythonGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      aggInputRowType,
      grouping,
      auxGrouping,
      aggCalls,
      aggFunctions)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputEdges: util.List[ExecEdge] = List(
    ExecEdge.builder()
      .damBehavior(ExecEdge.DamBehavior.END_INPUT)
      .build())

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val input = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)

    val ret = createPythonOneInputTransformation(
      input,
      inputType,
      outputType,
      getConfig(planner.getExecEnv, planner.getTableConfig))
    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }
    ret
  }

  private[this] def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      inputRowType: RowType,
      outputRowType: RowType,
      config: Configuration): OneInputTransformation[RowData, RowData] = {

    val (pythonUdafInputOffsets, pythonFunctionInfos) =
      extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls)

    val pythonOperator = getPythonAggregateFunctionOperator(
      config,
      inputRowType,
      outputRowType,
      pythonUdafInputOffsets,
      pythonFunctionInfos)

    new OneInputTransformation(
      inputTransform,
      "BatchExecPythonGroupAggregate",
      pythonOperator,
      InternalTypeInfo.of(outputRowType),
      inputTransform.getParallelism)
  }

  private[this] def getPythonAggregateFunctionOperator(
      config: Configuration,
      inputRowType: RowType,
      outputRowType: RowType,
      udafInputOffsets: Array[Int],
      pythonFunctionInfos: Array[PythonFunctionInfo]): OneInputStreamOperator[RowData, RowData] = {
    val clazz = loadClass(ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME)

    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Int]],
      classOf[Array[Int]],
      classOf[Array[Int]])
    ctor.newInstance(
      config,
      pythonFunctionInfos,
      inputRowType,
      outputRowType,
      grouping,
      grouping ++ auxGrouping,
      udafInputOffsets).asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }
}

object BatchExecPythonGroupAggregate {
  val ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME: String =
    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch." +
      "BatchArrowPythonGroupAggregateFunctionOperator"
}
