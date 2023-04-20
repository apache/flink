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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLegacySink
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink
import org.apache.flink.table.sinks.PartitionableTableSink

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config

import scala.collection.JavaConversions._

class StreamPhysicalLegacySinkRule(config: Config) extends ConverterRule(config) {

  def convert(rel: RelNode): RelNode = {
    val sink = rel.asInstanceOf[FlinkLogicalLegacySink]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    var requiredTraitSet = sink.getInput.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    if (sink.catalogTable != null && sink.catalogTable.isPartitioned) {
      sink.sink match {
        case partitionSink: PartitionableTableSink =>
          partitionSink.setStaticPartition(sink.staticPartitions)
          val dynamicPartFields = sink.catalogTable.getPartitionKeys
            .filter(!sink.staticPartitions.contains(_))

          if (dynamicPartFields.nonEmpty) {
            val dynamicPartIndices =
              dynamicPartFields.map(partitionSink.getTableSchema.getFieldNames.indexOf(_))

            // TODO This option is hardcoded to remove the dependency of planner from
            //  flink-connector-files. We should move this option out of FileSystemConnectorOptions
            val shuffleEnable = sink.catalogTable.getOptions
              .getOrDefault("sink.shuffle-by-partition.enable", "false")

            if (shuffleEnable.toBoolean) {
              requiredTraitSet = requiredTraitSet.plus(
                FlinkRelDistribution.hash(
                  dynamicPartIndices
                    .map(Integer.valueOf),
                  requireStrict = false))
            }

            if (partitionSink.configurePartitionGrouping(false)) {
              throw new TableException("Partition grouping in stream mode is not supported yet!")
            }
          }
        case _ =>
          throw new TableException(
            "We need PartitionableTableSink to write data to" +
              s" partitioned table: ${sink.sinkName}")
      }
    }

    val newInput = RelOptRule.convert(sink.getInput, requiredTraitSet)

    new StreamPhysicalLegacySink(
      rel.getCluster,
      newTrait,
      newInput,
      sink.hints,
      sink.sink,
      sink.sinkName)
  }
}

object StreamPhysicalLegacySinkRule {
  val INSTANCE: RelOptRule = new StreamPhysicalLegacySinkRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalLegacySink],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamPhysicalLegacySinkRule"))
}
