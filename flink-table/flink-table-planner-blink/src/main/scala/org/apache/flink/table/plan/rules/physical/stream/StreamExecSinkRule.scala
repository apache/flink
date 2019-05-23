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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSink
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecSink

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.RelNode

class StreamExecSinkRule extends ConverterRule(
    classOf[FlinkLogicalSink],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecSinkRule") {

  def convert(rel: RelNode): RelNode = {
    val sinkNode = rel.asInstanceOf[FlinkLogicalSink]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    // TODO Take PartitionableSink into consideration after FLINK-11993 is done
    val newInput = RelOptRule.convert(sinkNode.getInput, FlinkConventions.STREAM_PHYSICAL)

    new StreamExecSink(
      rel.getCluster,
      newTrait,
      newInput,
      sinkNode.sink,
      sinkNode.sinkName)
  }
}

object StreamExecSinkRule {

  val INSTANCE: RelOptRule = new StreamExecSinkRule

}
