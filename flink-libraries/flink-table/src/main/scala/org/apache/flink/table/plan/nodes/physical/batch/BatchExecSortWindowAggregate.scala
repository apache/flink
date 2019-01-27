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
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{RowType, TypeConverters}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.runtime.OneInputSubstituteStreamOperator
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder

class BatchExecSortWindowAggregate(
    window: LogicalWindow,
    inputTimestampIndex: Int,
    inputTimestampType: RelDataType,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    enableAssignPane: Boolean = false,
    isMerge: Boolean)
  extends BatchExecSortWindowAggregateBase(
    window,
    inputTimestampIndex,
    inputTimestampType,
    namedProperties,
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    enableAssignPane,
    isMerge,
    isFinal = true) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecSortWindowAggregate(
      window,
      inputTimestampIndex,
      inputTimestampType,
      namedProperties,
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputRelDataType,
      grouping,
      auxGrouping,
      enableAssignPane,
      isMerge)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val (windowSize: Long, slideSize: Long) = getWindowDef(window)
    val windowStart = 0L

    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val outputRowType = FlinkTypeFactory.toInternalRowType(getRowType)
    val ctx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val groupBufferLimitSize = NodeResourceUtil.getWindowAggBufferLimitSize(
      tableEnv.getConfig.getConf)

    val inputType = TypeConverters.createInternalTypeFromTypeInfo(
      input.getOutputType).asInstanceOf[RowType]
    val generatedOperator = if (grouping.isEmpty) {
      codegenWithoutKeys(ctx, tableEnv,
        inputType, outputRowType,
        groupBufferLimitSize, windowStart, windowSize, slideSize)
    } else {
      codegenWithKeys(ctx, tableEnv,
        inputType, outputRowType,
        groupBufferLimitSize, windowStart, windowSize, slideSize)
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

    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName = {
    val aggregateNamePrefix = if (isMerge) "Global" else "Complete"
    aggregateNamePrefix + "WindowSortAggregateBatchExec"
  }

}
