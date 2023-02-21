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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecDynamicFilteringDataCollector
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

class BatchPhysicalDynamicFilteringDataCollector(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    outputType: RelDataType,
    val dynamicFilteringFieldIndices: Array[Int]
) extends SingleRel(cluster, traitSet, input)
  with BatchPhysicalRel {

  override def deriveRowType(): RelDataType = outputType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchPhysicalDynamicFilteringDataCollector(
      cluster,
      traitSet,
      inputs.get(0),
      outputType,
      dynamicFilteringFieldIndices)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .item(
        "fields",
        dynamicFilteringFieldIndices
          .map(i => getInput.getRowType.getFieldNames.get(i))
          .mkString(","))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new BatchExecDynamicFilteringDataCollector(
      JavaScalaConversionUtil.toJava(dynamicFilteringFieldIndices.map(i => Integer.valueOf(i))),
      unwrapTableConfig(this),
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
