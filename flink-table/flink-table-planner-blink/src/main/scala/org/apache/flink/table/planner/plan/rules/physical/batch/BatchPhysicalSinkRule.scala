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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning
import org.apache.flink.table.filesystem.FileSystemOptions
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.abilities.sink.{PartitioningSpec, SinkAbilitySpec}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSink
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.{RelCollationTraitDef, RelCollations, RelNode}

import scala.collection.JavaConversions._
import scala.collection.mutable

class BatchPhysicalSinkRule extends ConverterRule(
    classOf[FlinkLogicalSink],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchPhysicalSinkRule") {

  def convert(rel: RelNode): RelNode = {
    val sink = rel.asInstanceOf[FlinkLogicalSink]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    var requiredTraitSet = sink.getInput.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val abilitySpecs: mutable.ArrayBuffer[SinkAbilitySpec] =
      mutable.ArrayBuffer(sink.abilitySpecs: _*)
    if (sink.catalogTable != null && sink.catalogTable.isPartitioned) {
      sink.tableSink match {
        case partitionSink: SupportsPartitioning =>
          if (sink.staticPartitions.nonEmpty) {
            val partitioningSpec = new PartitioningSpec(sink.staticPartitions)
            partitioningSpec.apply(partitionSink)
            abilitySpecs += partitioningSpec
          }

          val dynamicPartFields = sink.catalogTable.getPartitionKeys
              .filter(!sink.staticPartitions.contains(_))
          val fieldNames = sink.catalogTable
            .getResolvedSchema
            .toPhysicalRowDataType
            .getLogicalType.asInstanceOf[RowType]
            .getFieldNames

          if (dynamicPartFields.nonEmpty) {
            val dynamicPartIndices =
              dynamicPartFields.map(fieldNames.indexOf(_))

            val shuffleEnable = sink
                .catalogTable
                .getOptions
                .get(FileSystemOptions.SINK_SHUFFLE_BY_PARTITION.key())

            if (shuffleEnable != null && shuffleEnable.toBoolean) {
              requiredTraitSet = requiredTraitSet.plus(
                FlinkRelDistribution.hash(dynamicPartIndices
                    .map(Integer.valueOf), requireStrict = false))
            }

            if (partitionSink.requiresPartitionGrouping(true)) {
              // we shouldn't do partition grouping if the input already defines collation
              val relCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
              if (relCollation == null || relCollation.getFieldCollations.isEmpty) {
                // default to asc.
                val fieldCollations = dynamicPartIndices.map(FlinkRelOptUtil.ofRelFieldCollation)
                requiredTraitSet = requiredTraitSet.plus(RelCollations.of(fieldCollations: _*))
              } else {
                // tell sink not to expect grouping
                partitionSink.requiresPartitionGrouping(false)
              }
            }
          }
        case _ => throw new TableException(
          s"'${sink.tableIdentifier.asSummaryString()}' is a partitioned table, " +
            s"but the underlying [${sink.tableSink.asSummaryString()}] DynamicTableSink " +
            s"doesn't implement SupportsPartitioning interface.")
      }
    }

    val newInput = RelOptRule.convert(sink.getInput, requiredTraitSet)

    new BatchPhysicalSink(
      rel.getCluster,
      newTrait,
      newInput,
      sink.hints,
      sink.tableIdentifier,
      sink.catalogTable,
      sink.tableSink,
      abilitySpecs.toArray)
  }
}

object BatchPhysicalSinkRule {
  val INSTANCE = new BatchPhysicalSinkRule
}
