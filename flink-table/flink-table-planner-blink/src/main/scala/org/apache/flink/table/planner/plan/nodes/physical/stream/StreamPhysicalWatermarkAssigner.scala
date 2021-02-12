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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode for [[WatermarkAssigner]].
 */
class StreamPhysicalWatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputRel: RelNode,
    rowtimeFieldIndex: Int,
    watermarkExpr: RexNode)
  extends WatermarkAssigner(cluster, traits, inputRel, rowtimeFieldIndex, watermarkExpr)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      rowtime: Int,
      watermark: RexNode): RelNode = {
    new StreamPhysicalWatermarkAssigner(cluster, traitSet, input, rowtime, watermark)
  }

  /**
   * Fully override this method to have a better display name of this RelNode.
   */
  override def explainTerms(pw: RelWriter): RelWriter = {
    val inFieldNames = inputRel.getRowType.getFieldNames.toList
    val rowtimeFieldName = inFieldNames(rowtimeFieldIndex)
    pw.input("input", getInput())
      .item("rowtime", rowtimeFieldName)
      .item("watermark", getExpressionString(
        watermarkExpr,
        inFieldNames,
        None,
        preferExpressionFormat(pw)))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecWatermarkAssigner(
      watermarkExpr,
      rowtimeFieldIndex,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
