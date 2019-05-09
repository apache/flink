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
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder

import java.util

/**
  * Batch physical RelNode for (global) hash-based aggregate operator.
  *
  * @see [[BatchExecGroupAggregateBase]] for more info.
  */
class BatchExecHashAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    val aggInputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    isMerge: Boolean)
  extends BatchExecHashAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    aggInputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    isMerge = isMerge,
    isFinal = true) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecHashAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      aggInputRowType,
      grouping,
      auxGrouping,
      aggCallToAggFunction,
      isMerge)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("isMerge", isMerge)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("auxGrouping", RelExplainUtil.fieldToString(auxGrouping, inputRowType),
        auxGrouping.nonEmpty)
      .item("select", RelExplainUtil.groupAggregationToString(
        inputRowType,
        getRowType,
        grouping,
        auxGrouping,
        aggCallToAggFunction,
        isMerge,
        isGlobal = true))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior = DamBehavior.FULL_DAM

  override def getOperatorName: String = {
    val aggregateNamePrefix = if (isMerge) "Global" else "Complete"
    aggOperatorName(aggregateNamePrefix + "HashAggregate")
  }

  override def getParallelism(input: StreamTransformation[BaseRow], conf: TableConfig): Int = {
    if (isFinal && grouping.length == 0) 1 else input.getParallelism
  }
}
