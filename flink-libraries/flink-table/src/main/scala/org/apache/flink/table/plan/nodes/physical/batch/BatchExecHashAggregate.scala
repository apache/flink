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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{RowType, TypeConverters}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.{AggregateNameUtil, FlinkRelOptUtil}
import org.apache.flink.table.runtime.OneInputSubstituteStreamOperator
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.{ImmutableIntList, Util}

import scala.collection.JavaConversions._

class BatchExecHashAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean)
  extends BatchExecHashAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputType,
    grouping,
    auxGrouping,
    isMerge = isMerge,
    isFinal = true) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecHashAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputType,
      grouping,
      auxGrouping,
      isMerge)
  }
  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val pushDownDistribution = requiredDistribution.getType match {
      case SINGLETON => if (grouping.length == 0) requiredDistribution else null
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val groupKeysList = ImmutableIntList.of(grouping.indices.toArray: _*)
        if (requiredDistribution.requireStrict) {
          if (shuffleKeys == groupKeysList) {
            FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList)
          } else {
            null
          }
        } else if (Util.startsWith(shuffleKeys, groupKeysList)) {
          // If required distribution is not strict, Hash[a] can satisfy Hash[a, b].
          // If partitionKeys satisfies shuffleKeys (the shuffle between this node and
          // its output is not necessary), just push down partitionKeys into input.
          FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList, requireStrict = false)
        } else {
          val tableConfig = FlinkRelOptUtil.getTableConfig(this)
          if (tableConfig.getConf.getBoolean(
            TableConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED) &&
              groupKeysList.containsAll(shuffleKeys)) {
            // If partialKey is enabled, push down partialKey requirement into input.
           FlinkRelDistribution.hash(
             shuffleKeys.map(k => Integer.valueOf(grouping(k))), requireStrict = false)
          } else {
            null
          }
        }
      case _ => null
    }
    if (pushDownDistribution == null) {
      return null
    }
    val newInput = RelOptRule.convert(getInput, pushDownDistribution)
    val newProvidedTraitSet = getTraitSet.replace(requiredDistribution)
    copy(newProvidedTraitSet, Seq(newInput))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("isMerge", isMerge)
      .itemIf("groupBy", AggregateNameUtil.groupingToString(inputType, grouping), grouping.nonEmpty)
      .itemIf("auxGrouping",
        AggregateNameUtil.groupingToString(inputType, auxGrouping), auxGrouping.nonEmpty)
      .item("select", AggregateNameUtil.aggregationToString(
        inputType,
        grouping,
        auxGrouping,
        rowRelDataType,
        aggCallToAggFunction.map(_._1),
        aggCallToAggFunction.map(_._2),
        isMerge,
        isGlobal = true))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.FULL_DAM

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val ctx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val outputRowType = FlinkTypeFactory.toInternalRowType(getRowType)
    val inputType = TypeConverters.createInternalTypeFromTypeInfo(
      input.getOutputType).asInstanceOf[RowType]
    val generatedOperator = if (grouping.isEmpty) {
      codegenWithoutKeys(isMerge, isFinal, ctx, tableEnv, inputType, outputRowType, "NoGrouping")
    } else {
      val reservedManagedMem =
        getResource.getReservedManagedMem * NodeResourceUtil.SIZE_IN_MB
      val maxManagedMem =
        getResource.getMaxManagedMem * NodeResourceUtil.SIZE_IN_MB
      codegenWithKeys(
        ctx,
        tableEnv,
        inputType,
        outputRowType,
        reservedManagedMem,
        maxManagedMem)
    }
    val operator = new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      TypeConverters.toBaseRowTypeInfo(outputRowType),
      getResource.getParallelism)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setDamBehavior(getDamBehavior)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    transformation
  }

  private def getOperatorName = {
    val aggregateNamePrefix = if (isMerge) "Global" else "Complete"
    getAggOperatorName(aggregateNamePrefix + "HashAggregate")
  }

}
