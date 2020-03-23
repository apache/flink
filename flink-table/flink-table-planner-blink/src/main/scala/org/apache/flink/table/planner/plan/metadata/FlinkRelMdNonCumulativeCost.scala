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

package org.apache.flink.table.planner.plan.metadata

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata._
import org.apache.calcite.util.BuiltInMethod

/**
  * FlinkRelMdNonCumulativeCost supplies a implementation of
  * [[RelMetadataQuery#getNonCumulativeCost]] for the standard logical algebra.
  */
class FlinkRelMdNonCumulativeCost private
  extends MetadataHandler[BuiltInMetadata.NonCumulativeCost] {

  def getDef: MetadataDef[BuiltInMetadata.NonCumulativeCost] = BuiltInMetadata.NonCumulativeCost.DEF

  def getNonCumulativeCost(rel: RelNode, mq: RelMetadataQuery): RelOptCost = {
    val planner = if (FlinkRelMdNonCumulativeCost.THREAD_PLANNER.get() != null) {
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.get()
    } else {
      rel.getCluster.getPlanner
    }
    rel.computeSelfCost(planner, mq)
  }

}

object FlinkRelMdNonCumulativeCost {

  private val INSTANCE = new FlinkRelMdNonCumulativeCost

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.NON_CUMULATIVE_COST.method, INSTANCE)

  val THREAD_PLANNER = new ThreadLocal[RelOptPlanner]()

}
