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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery

import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner, RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexBuilder

import java.util
import java.util.concurrent.atomic.AtomicInteger

/**
  * Flink specific [[RelOptCluster]] to use [[FlinkRelMetadataQuery]]
  * instead of [[RelMetadataQuery]].
  */
class FlinkRelOptCluster(
    planner: RelOptPlanner,
    typeFactory: RelDataTypeFactory,
    rexBuilder: RexBuilder,
    nextCorrel: AtomicInteger,
    mapCorrelToRel: util.Map[String, RelNode])
  extends RelOptCluster(planner, typeFactory, rexBuilder, nextCorrel, mapCorrelToRel) {

  private var fmq: FlinkRelMetadataQuery = _

  /**
    * Returns the current [[FlinkRelMetadataQuery]] instead of [[RelMetadataQuery]].
    *
    * <p>This method might be changed or moved in future.
    * If you have a [[RelOptRuleCall]] available,
    * for example if you are in a [[RelOptRule#onMatch(RelOptRuleCall)]]
    * method, then use [[RelOptRuleCall#getMetadataQuery()]] instead.
    */
  override def getMetadataQuery: RelMetadataQuery = {
    if (fmq == null) {
      fmq = FlinkRelMetadataQuery.instance()
    }
    fmq
  }

  /**
    * Should be called whenever the current [[FlinkRelMetadataQuery]] becomes
    * invalid. Typically invoked from [[RelOptRuleCall#transformTo]].
    */
  override def invalidateMetadataQuery(): Unit = fmq = null
}

object FlinkRelOptCluster {
  /** Creates a FlinkRelOptCluster instance. */
  def create(planner: RelOptPlanner, rexBuilder: RexBuilder): RelOptCluster =
    new FlinkRelOptCluster(
      planner,
      rexBuilder.getTypeFactory,
      rexBuilder,
      new AtomicInteger(0),
      new util.HashMap[String, RelNode])
}
