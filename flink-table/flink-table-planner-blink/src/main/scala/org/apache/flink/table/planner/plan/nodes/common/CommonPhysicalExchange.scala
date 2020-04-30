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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.cost.FlinkCost._
import org.apache.flink.table.planner.plan.cost.FlinkCostFactory
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Exchange
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter}

import scala.collection.JavaConverters._

/**
  * Base class for flink [[Exchange]].
  */
abstract class CommonPhysicalExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relNode: RelNode,
    relDistribution: RelDistribution)
  extends Exchange(cluster, traitSet, relNode, relDistribution)
  with FlinkPhysicalRel {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val inputRows = mq.getRowCount(input)
    if (inputRows == null) {
      return null
    }
    val inputSize = mq.getAverageRowSize(input) * inputRows
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    relDistribution.getType match {
      case RelDistribution.Type.SINGLETON =>
        val cpuCost = (SINGLETON_CPU_COST + SERIALIZE_DESERIALIZE_CPU_COST) * inputRows
        costFactory.makeCost(inputRows, cpuCost, 0, inputSize, 0)
      case RelDistribution.Type.RANDOM_DISTRIBUTED =>
        val cpuCost = (RANDOM_CPU_COST + SERIALIZE_DESERIALIZE_CPU_COST) * inputRows
        costFactory.makeCost(inputRows, cpuCost, 0, inputSize, 0)
      case RelDistribution.Type.RANGE_DISTRIBUTED =>
        val cpuCost = (RANGE_PARTITION_CPU_COST + SERIALIZE_DESERIALIZE_CPU_COST) * inputRows
        // all input data has to write into external disk because of DataExchangeMode.BATCH
        val diskIoCost = inputSize
        // all input data has to transfer almost 2 times
        val networkCost = 2 * inputSize
        // ignore the memory cost because it is so small
        costFactory.makeCost(inputRows, cpuCost, diskIoCost, networkCost, 0)
      case RelDistribution.Type.BROADCAST_DISTRIBUTED =>
        // network cost of Broadcast = size * (parallelism of other input) which we don't have here.
        val nParallelism = Math.max(1,
          (inputSize / SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE).toInt)
        val cpuCost = nParallelism.toDouble * inputRows * SERIALIZE_DESERIALIZE_CPU_COST
        val networkCost = nParallelism * inputSize
        costFactory.makeCost(inputRows, cpuCost, 0, networkCost, 0)
      case RelDistribution.Type.HASH_DISTRIBUTED =>
        // hash shuffle
        val cpuCost = (HASH_CPU_COST * relDistribution.getKeys.size +
          SERIALIZE_DESERIALIZE_CPU_COST ) * inputRows
        costFactory.makeCost(inputRows, cpuCost, 0, inputSize, 0)
      case RelDistribution.Type.ANY =>
        // the specific partitioner may be ForwardPartitioner or RebalancePartitioner
        // decided by StreamGraph now. use inputRows as cpuCost here.
        costFactory.makeCost(inputRows, SERIALIZE_DESERIALIZE_CPU_COST * inputRows, 0, inputSize, 0)
      case _ =>
        throw new UnsupportedOperationException(
          s"not support RelDistribution: ${relDistribution.getType} now!")
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("distribution", distributionToString())
  }

  private def distributionToString(): String = {
    val flinkRelDistribution = relDistribution.asInstanceOf[FlinkRelDistribution]
    val inputFieldNames = getInput.getRowType.getFieldNames
    val exchangeName = relDistribution.getType.shortName
    val fieldNames = relDistribution.getType match {
      case RelDistribution.Type.RANGE_DISTRIBUTED =>
        flinkRelDistribution.getFieldCollations.get.asScala.map { fieldCollation =>
          val name = inputFieldNames.get(fieldCollation.getFieldIndex)
          s"$name ${fieldCollation.getDirection.shortString}"
        }.asJava
      case _ =>
        flinkRelDistribution.getKeys.asScala.map(inputFieldNames.get(_)).asJava
    }
    if (fieldNames.isEmpty) exchangeName else exchangeName + fieldNames
  }

}
