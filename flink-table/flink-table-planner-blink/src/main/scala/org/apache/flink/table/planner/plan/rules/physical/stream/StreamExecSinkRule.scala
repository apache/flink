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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSink
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSink
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.sinks.PartitionableTableSink

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import scala.collection.JavaConversions._

class StreamExecSinkRule extends ConverterRule(
    classOf[FlinkLogicalSink],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecSinkRule") {

  def convert(rel: RelNode): RelNode = {
    val sinkNode = rel.asInstanceOf[FlinkLogicalSink]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    var requiredTraitSet = sinkNode.getInput.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    sinkNode.sink match {
      case partitionSink: PartitionableTableSink
        if partitionSink.getPartitionFieldNames != null &&
          partitionSink.getPartitionFieldNames.nonEmpty =>
        val partitionFields = partitionSink.getPartitionFieldNames
        val partitionIndices = partitionFields
          .map(partitionSink.getTableSchema.getFieldNames.indexOf(_))
        // validate
        partitionIndices.foreach { idx =>
          if (idx < 0) {
            throw new TableException(s"Partitionable sink ${sinkNode.sinkName} field " +
              s"${partitionFields.get(idx)} must be in the schema.")
          }
        }

        if (partitionSink.configurePartitionGrouping(false)) {
          throw new TableException("Partition grouping in stream mode is not supported yet!")
        }

        if (!partitionSink.isInstanceOf[DataStreamTableSink[_]]) {
          requiredTraitSet = requiredTraitSet.plus(
            FlinkRelDistribution.hash(partitionIndices
              .map(Integer.valueOf), requireStrict = false))
        }
      case _ =>
    }
    val newInput = RelOptRule.convert(sinkNode.getInput, requiredTraitSet)

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
