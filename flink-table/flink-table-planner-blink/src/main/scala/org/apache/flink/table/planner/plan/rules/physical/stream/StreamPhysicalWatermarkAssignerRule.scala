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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner

/**
  * Rule that converts [[FlinkLogicalWatermarkAssigner]] to [[StreamPhysicalWatermarkAssigner]].
  */
class StreamPhysicalWatermarkAssignerRule
  extends ConverterRule(
    classOf[FlinkLogicalWatermarkAssigner],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamPhysicalWatermarkAssignerRule") {

  override def convert(rel: RelNode): RelNode = {
    val watermarkAssigner = rel.asInstanceOf[FlinkLogicalWatermarkAssigner]
    val convertInput = RelOptRule.convert(
      watermarkAssigner.getInput, FlinkConventions.STREAM_PHYSICAL)
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    new StreamPhysicalWatermarkAssigner(
      watermarkAssigner.getCluster,
      traitSet,
      convertInput,
      watermarkAssigner.rowtimeFieldIndex,
      watermarkAssigner.watermarkExpr
    )
  }
}

object StreamPhysicalWatermarkAssignerRule {
  val INSTANCE = new StreamPhysicalWatermarkAssignerRule
}
