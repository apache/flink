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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecScriptTransform
import org.apache.flink.table.planner.plan.utils.RelExplainUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.script.ScriptTransformIOInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

import scala.collection.JavaConversions._

/** Batch physical RelNode for the sql semantic "TRANSFORM c1, c2, xx USING 'script'. */
class BatchPhysicalScriptTransformationExec(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    val fieldIndices: Array[Int],
    val script: String,
    val scriptInputOutSchema: ScriptTransformIOInfo,
    val outputRowType: RelDataType)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchPhysicalScriptTransformationExec(
      cluster,
      traitSet,
      inputs.get(0),
      fieldIndices,
      script,
      scriptInputOutSchema,
      outputRowType
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
    // explain the input of the script
    pw.item("script-inputs", RelExplainUtil.fieldToString(fieldIndices, input.getRowType))
      .item("script-outputs", getRowType.getFieldNames.mkString(", "))
      .item("script", script)
      .item("script-io-info", scriptInputOutSchema)
    pw
  }

  override def deriveRowType: RelDataType = outputRowType

  override def translateToExecNode(): ExecNode[_] = {
    new BatchExecScriptTransform(
      unwrapTableConfig(this),
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(inputRel.getRowType),
      FlinkTypeFactory.toLogicalRowType(outputRowType),
      getRelDetailedDescription,
      fieldIndices,
      script,
      scriptInputOutSchema)
  }
}
