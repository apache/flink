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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

abstract class BatchExecWindowAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    window: LogicalWindow,
    namedProperties: Seq[PlannerNamedWindowProperty],
    enableAssignPane: Boolean = true,
    val isMerge: Boolean,
    val isFinal: Boolean)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel {

  if (grouping.isEmpty && auxGrouping.nonEmpty) {
    throw new TableException("auxGrouping should be empty if grouping is emtpy.")
  }

  def getGrouping: Array[Int] = grouping

  def getAuxGrouping: Array[Int] = auxGrouping

  def getWindow: LogicalWindow = window

  def getNamedProperties: Seq[PlannerNamedWindowProperty] = namedProperties

  def getAggCallList: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)

  override def deriveRowType(): RelDataType = outputRowType

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("auxGrouping", RelExplainUtil.fieldToString(auxGrouping, inputRowType),
        auxGrouping.nonEmpty)
      .item("window", window)
      .itemIf("properties", namedProperties.map(_.name).mkString(", "), namedProperties.nonEmpty)
      .item("select", RelExplainUtil.windowAggregationToString(
        inputRowType,
        grouping,
        auxGrouping,
        outputRowType,
        aggCallToAggFunction,
        enableAssignPane,
        isMerge = isMerge,
        isGlobal = isFinal))
  }

}
