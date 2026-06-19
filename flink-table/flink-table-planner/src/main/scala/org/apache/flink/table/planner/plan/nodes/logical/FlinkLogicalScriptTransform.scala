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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.hive.LogicalScriptTransform
import org.apache.flink.table.runtime.script.ScriptTransformIOInfo

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config

import java.util.{List => JList}

/** A FlinkLogicalRel to represent the sql semantic "TRANSFORM c1, c2, xx USING 'script'". */
class FlinkLogicalScriptTransform(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    child: RelNode,
    val inputIndexes: Array[Int],
    val script: String,
    val scriptInputOutSchema: ScriptTransformIOInfo,
    val outDataType: RelDataType)
  extends SingleRel(cluster, traits, child)
  with FlinkLogicalRel {

  override def copy(trainSet: RelTraitSet, inputs: JList[RelNode]): RelNode =
    new FlinkLogicalScriptTransform(
      getCluster,
      trainSet,
      inputs.get(0),
      inputIndexes,
      script,
      scriptInputOutSchema,
      outDataType)

  override protected def deriveRowType: RelDataType = outDataType
}

class FlinkLogicalScriptTransformBatchConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val scriptTransform = rel.asInstanceOf[LogicalScriptTransform]
    val newInput = RelOptRule.convert(scriptTransform.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalScriptTransform.create(
      newInput,
      scriptTransform.getFieldIndices,
      scriptTransform.getScript,
      scriptTransform.getScriptInputOutSchema,
      scriptTransform.getRowType
    )
  }
}

object FlinkLogicalScriptTransform {
  val BATCH_CONVERTER: RelOptRule = new FlinkLogicalScriptTransformBatchConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalScriptTransform],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalScriptTransformBatchConverter"))

  def create(
      input: RelNode,
      inputIndexes: Array[Int],
      script: String,
      scriptInputOutSchema: ScriptTransformIOInfo,
      outDataType: RelDataType): FlinkLogicalScriptTransform = {
    val cluster = input.getCluster
    val traits = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalScriptTransform(
      cluster,
      traits,
      input,
      inputIndexes,
      script,
      scriptInputOutSchema,
      outDataType)
  }
}
