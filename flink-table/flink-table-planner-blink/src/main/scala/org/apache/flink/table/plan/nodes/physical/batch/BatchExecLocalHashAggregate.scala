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
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for local hash-based aggregate operator.
  *
  * @see [[BatchExecGroupAggregateBase]] for more info.
  */
class BatchExecLocalHashAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)])
  extends BatchExecHashAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    inputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    isMerge = false,
    isFinal = false) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      auxGrouping,
      aggCallToAggFunction)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("auxGrouping", RelExplainUtil.fieldToString(auxGrouping, inputRowType),
        auxGrouping.nonEmpty)
      .item("select", RelExplainUtil.groupAggregationToString(
        inputRowType,
        outputRowType,
        grouping,
        auxGrouping,
        aggCallToAggFunction,
        isMerge = false,
        isGlobal = false))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = {
    if (grouping.length == 0) DamBehavior.FULL_DAM else DamBehavior.MATERIALIZING
  }

  override def getOperatorName: String = aggOperatorName("LocalHashAggregate")

  override def getParallelism(input: StreamTransformation[BaseRow], conf: TableConfig): Int =
    input.getParallelism
}
