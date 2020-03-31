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
import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalWatermarkAssigner, WatermarkAssigner}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import java.util

/**
  * Sub-class of [[WatermarkAssigner]] that is a relational operator
  * which generates [[org.apache.flink.streaming.api.watermark.Watermark]].
  */
class FlinkLogicalWatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    rowtimeFieldIndex: Option[Int],
    watermarkDelay: Option[Long])
  extends WatermarkAssigner(cluster, traits, input, rowtimeFieldIndex, watermarkDelay)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalWatermarkAssigner(
      cluster, traitSet, inputs.get(0), rowtimeFieldIndex, watermarkDelay)
  }

}

class FlinkLogicalWatermarkAssignerConverter extends ConverterRule(
  classOf[LogicalWatermarkAssigner],
  Convention.NONE,
  FlinkConventions.LOGICAL,
  "FlinkLogicalWatermarkAssignerConverter") {

  override def convert(rel: RelNode): RelNode = {
    val watermark = rel.asInstanceOf[LogicalWatermarkAssigner]
    val newInput = RelOptRule.convert(watermark.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalWatermarkAssigner.create(
      newInput,
      watermark.rowtimeFieldIndex,
      watermark.watermarkDelay)
  }
}

object FlinkLogicalWatermarkAssigner {
  val CONVERTER = new FlinkLogicalWatermarkAssignerConverter

  def create(
      input: RelNode,
      rowtimeFieldIndex: Option[Int],
      watermarkDelay: Option[Long]): FlinkLogicalWatermarkAssigner = {
    val cluster = input.getCluster
    val traitSet = cluster.traitSet().replace(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalWatermarkAssigner(cluster, traitSet, input, rowtimeFieldIndex, watermarkDelay)
  }
}
