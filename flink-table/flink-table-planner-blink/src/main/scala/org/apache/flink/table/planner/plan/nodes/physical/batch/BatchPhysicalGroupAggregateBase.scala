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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy
import org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelNode, SingleRel}

/**
 * Batch physical RelNode for aggregate.
 *
 * <P>There are two differences between this node and [[Aggregate]]:
 * 1. This node supports two-stage aggregation to reduce data-shuffling:
 * local-aggregation and global-aggregation.
 * local-aggregation produces a partial result for each group before shuffle in stage 1,
 * and then the partially aggregated results are shuffled to global-aggregation
 * which produces the final result in stage 2.
 * Two-stage aggregation is enabled only if all aggregate functions are mergeable.
 * (e.g. SUM, AVG, MAX)
 * 2. This node supports auxiliary group keys which will not be computed as key and
 * does not also affect the correctness of the final result. [[Aggregate]] does not distinguish
 * group keys and auxiliary group keys, and combines them as a complete `groupSet`.
 */
abstract class BatchPhysicalGroupAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val grouping: Array[Int],
    val auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    val isMerge: Boolean,
    val isFinal: Boolean)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel {

  if (grouping.isEmpty && auxGrouping.nonEmpty) {
    throw new TableException("auxGrouping should be empty if grouping is empty.")
  }

  override def deriveRowType(): RelDataType = outputRowType

  def getAggCallList: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)

  def getAggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)] = aggCallToAggFunction

  protected def isEnforceTwoStageAgg: Boolean = {
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
    getAggPhaseStrategy(tableConfig) == AggregatePhaseStrategy.TWO_PHASE
  }

}
