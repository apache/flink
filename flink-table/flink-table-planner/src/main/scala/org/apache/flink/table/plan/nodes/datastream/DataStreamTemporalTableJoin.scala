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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.util.Preconditions.checkState

/**
  * RelNode for a stream join with [[org.apache.flink.table.functions.TemporalTableFunction]].
  */
class DataStreamTemporalTableJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    joinCondition: RexNode,
    joinInfo: JoinInfo,
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    schema: RowSchema,
    ruleDescription: String)
  extends DataStreamJoin(
    cluster,
    traitSet,
    leftNode,
    rightNode,
    joinCondition,
    joinInfo,
    JoinRelType.INNER,
    leftSchema,
    rightSchema,
    schema,
    ruleDescription) {

  override def needsUpdatesAsRetraction: Boolean = true

  override def producesRetractions: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    checkState(inputs.size() == 2)
    new DataStreamTemporalTableJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinInfo,
      leftSchema,
      rightSchema,
      schema,
      ruleDescription)
  }

  override protected def createTranslator(
      planner: StreamPlanner): DataStreamJoinToCoProcessTranslator = {
    DataStreamTemporalJoinToCoProcessTranslator.create(
      this.toString,
      planner.getConfig,
      schema.typeInfo,
      leftSchema,
      rightSchema,
      joinInfo,
      cluster.getRexBuilder)
  }}
