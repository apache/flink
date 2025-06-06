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

import org.apache.flink.table.planner.JList
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.{conditionsToString, conditionToString, preferExpressionFormat}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.generated.FilterCondition

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexNode

import java.util

/**
 * Stream physical RelNode which normalizes a changelog stream which maybe an upsert stream or a
 * changelog stream containing duplicate events. This node normalize such stream into a regular
 * changelog stream that contains INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE records without
 * duplication.
 */
class StreamPhysicalChangelogNormalize(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val uniqueKeys: Array[Int],
    val filterCondition: RexNode = null)
  extends SingleRel(cluster, traitSet, input)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalChangelogNormalize(
      cluster,
      traitSet,
      inputs.get(0),
      uniqueKeys,
      filterCondition)
  }

  def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      uniqueKeys: Array[Int],
      filterCondition: RexNode): RelNode = {
    new StreamPhysicalChangelogNormalize(cluster, traitSet, input, uniqueKeys, filterCondition)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val fieldNames = getRowType.getFieldNames
    val conditions = JavaScalaConversionUtil.toJava(List(filterCondition))
    super
      .explainTerms(pw)
      .item("key", uniqueKeys.map(fieldNames.get).mkString(", "))
      .itemIf(
        "condition",
        conditionsToString(
          Option(conditions),
          input.getRowType,
          getExpressionString,
          preferExpressionFormat(pw),
          convertToExpressionDetail(pw.getDetailLevel)
        ),
        filterCondition != null
      )
  }

  override def translateToExecNode(): ExecNode[_] = {
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    new StreamExecChangelogNormalize(
      unwrapTableConfig(this),
      uniqueKeys,
      generateUpdateBefore,
      filterCondition,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
