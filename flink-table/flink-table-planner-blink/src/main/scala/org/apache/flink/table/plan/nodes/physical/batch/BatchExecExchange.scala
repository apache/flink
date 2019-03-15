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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.plan.nodes.common.CommonPhysicalExchange

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}

/**
  * This RelNode represents a change of partitioning of the input elements.
  *
  * This does not create a physical transformation If its relDistribution' type is not range,
  * it only affects how upstream operations are connected to downstream operations.
  *
  * But if the type is range, this relNode will create some physical transformation because it
  * need calculate the data distribution. To calculate the data distribution, the received stream
  * will split in two process stream. For the first process stream, it will go through the sample
  * and statistics to calculate the data distribution in pipeline mode. For the second process
  * stream will been bocked. After the first process stream has been calculated successfully,
  * then the two process stream  will union together. Thus it can partitioner the record based
  * the data distribution. Then The RelNode will create the following transformations.
  *
  * +---------------------------------------------------------------------------------------------+
  * |                                                                                             |
  * | +-----------------------------+                                                             |
  * | | StreamTransformation        | ------------------------------------>                       |
  * | +-----------------------------+                                     |                       |
  * |                 |                                                   |                       |
  * |                 |                                                   |                       |
  * |                 |forward & PIPELINED                                |                       |
  * |                \|/                                                  |                       |
  * | +--------------------------------------------+                      |                       |
  * | | OneInputTransformation[LocalSample, n]     |                      |                       |
  * | +--------------------------------------------+                      |                       |
  * |                      |                                              |forward & BATCH        |
  * |                      |forward & PIPELINED                           |                       |
  * |                     \|/                                             |                       |
  * | +--------------------------------------------------+                |                       |
  * | |OneInputTransformation[SampleAndHistogram, 1]     |                |                       |
  * | +--------------------------------------------------+                |                       |
  * |                        |                                            |                       |
  * |                        |broadcast & PIPELINED                       |                       |
  * |                        |                                            |                       |
  * |                       \|/                                          \|/                      |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               TwoInputTransformation[AssignRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                       |                                                     |
  * |                                       |custom & PIPELINED                                   |
  * |                                      \|/                                                    |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               OneInputTransformation[RemoveRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                                                                             |
  * +---------------------------------------------------------------------------------------------+
  */
class BatchExecExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, inputRel, relDistribution)
  with BatchPhysicalRel {

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): BatchExecExchange = {
    new BatchExecExchange(cluster, traitSet, newInput, relDistribution)
  }

}

