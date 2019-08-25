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

package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetJoin
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin

import scala.collection.JavaConversions._

class DataSetJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]

    val joinInfo = join.analyzeCondition

    // joins require an equi-condition or a conjunctive predicate with at least one equi-condition
    !joinInfo.pairs().isEmpty
  }

  override def convert(rel: RelNode): RelNode = {

    val join: FlinkLogicalJoin = rel.asInstanceOf[FlinkLogicalJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.DATASET)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), FlinkConventions.DATASET)
    val joinInfo = join.analyzeCondition

    new DataSetJoin(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      rel.getRowType,
      join.getCondition,
      join.getRowType,
      joinInfo,
      joinInfo.pairs.toList,
      join.getJoinType,
      null,
      description)
  }

}

object DataSetJoinRule {
  val INSTANCE: RelOptRule = new DataSetJoinRule
}
