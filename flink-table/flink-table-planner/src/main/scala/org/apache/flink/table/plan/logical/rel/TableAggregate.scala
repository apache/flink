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

package org.apache.flink.table.plan.logical.rel

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.{ImmutableBitSet, Pair}
import org.apache.flink.table.plan.nodes.CommonTableAggregate

/**
  * Relational operator that represents a table aggregate. A TableAggregate is similar to the
  * [[org.apache.calcite.rel.core.Aggregate]] but may output 0 or more records for a group.
  */
abstract class TableAggregate(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  input: RelNode,
  indicator: Boolean,
  groupSet: ImmutableBitSet,
  groupSets: util.List[ImmutableBitSet],
  val aggCalls: util.List[AggregateCall])
  extends SingleRel(cluster, traitSet, input)
    with CommonTableAggregate {

  private[flink] def getIndicator: Boolean = indicator

  private[flink] def getGroupSet: ImmutableBitSet = groupSet

  private[flink] def getGroupSets: util.List[ImmutableBitSet] = groupSets

  private[flink] def getAggCallList: util.List[AggregateCall] = aggCalls

  private[flink] def getNamedAggCalls: util.List[Pair[AggregateCall, String]] = {
    super.getNamedAggCalls(aggCalls, deriveRowType(), indicator, groupSet)
  }

  override def deriveRowType(): RelDataType = {
    deriveTableAggRowType(cluster, input, groupSet, aggCalls)
  }

  private[flink] def getCorrespondingAggregate: Aggregate = {
    new LogicalAggregate(
      cluster,
      traitSet,
      getInput,
      indicator,
      groupSet,
      groupSets,
      aggCalls
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("group", groupSet)
      .item("tableAggregate", aggCalls)
  }
}
