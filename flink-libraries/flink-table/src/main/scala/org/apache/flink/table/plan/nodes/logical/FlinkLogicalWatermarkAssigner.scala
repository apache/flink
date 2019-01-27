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

package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.{LogicalWatermarkAssigner, WatermarkAssigner}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import java.util

class FlinkLogicalWatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    rowtimeField: String,
    watermarkOffset: Long)
  extends WatermarkAssigner(cluster, traits, input, rowtimeField, watermarkOffset)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalWatermarkAssigner(
        cluster, traitSet, inputs.get(0), rowtimeField, watermarkOffset)
  }

  override def isDeterministic: Boolean = true
}

class FlinkLogicalWatermarkAssignerConverter extends ConverterRule(
  classOf[LogicalWatermarkAssigner],
  Convention.NONE,
  FlinkConventions.LOGICAL,
  "FlinkLogicalWatermarkAssignerConverter"){

  override def convert(rel: RelNode): RelNode = {
    val watermark = rel.asInstanceOf[LogicalWatermarkAssigner]
    val traitSet = FlinkRelMetadataQuery.traitSet(rel).replace(FlinkConventions.LOGICAL).simplify()
    val newInput = RelOptRule.convert(watermark.getInput, FlinkConventions.LOGICAL)
    new FlinkLogicalWatermarkAssigner(
      rel.getCluster,
      traitSet,
      newInput,
      watermark.rowtimeField,
      watermark.watermarkOffset
    )
  }
}
object FlinkLogicalWatermarkAssigner {
  val CONVERTER = new FlinkLogicalWatermarkAssignerConverter
}


