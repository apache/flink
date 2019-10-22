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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode

import java.util

/**
  * Sub-class of [[WatermarkAssigner]] that is a relational operator
  * which generates [[org.apache.flink.streaming.api.watermark.Watermark]].
  * This class corresponds to Calcite logical rel.
  */
final class LogicalWatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    rowtimeFieldIndex: Option[Int],
    watermarkDelay: Option[Long])
  extends WatermarkAssigner(cluster, traits, input, rowtimeFieldIndex, watermarkDelay) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalWatermarkAssigner(cluster, traits, inputs.get(0), rowtimeFieldIndex, watermarkDelay)
  }
}

