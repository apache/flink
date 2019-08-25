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
import org.apache.calcite.rel.rules.UnionToDistinctRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetUnion
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalUnion

import scala.collection.JavaConverters._

class DataSetUnionRule
  extends ConverterRule(
    classOf[FlinkLogicalUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetUnionRule") {

  /**
   * Only translate UNION ALL.
   * Note: A distinct Union are translated into
   * an Aggregate on top of a UNION ALL by [[UnionToDistinctRule]]
   */
  override def matches(call: RelOptRuleCall): Boolean = {
    val union: FlinkLogicalUnion = call.rel(0).asInstanceOf[FlinkLogicalUnion]
    union.all
  }

  def convert(rel: RelNode): RelNode = {
    val union: FlinkLogicalUnion = rel.asInstanceOf[FlinkLogicalUnion]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)

    val newInputs = union
      .getInputs
      .asScala
      .map(RelOptRule.convert(_, FlinkConventions.DATASET))
      .asJava

    new DataSetUnion(
      rel.getCluster,
      traitSet,
      newInputs,
      rel.getRowType)
  }
}

object DataSetUnionRule {
  val INSTANCE: RelOptRule = new DataSetUnionRule
}
