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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalValues
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalValues

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config

/** Rule that converts [[FlinkLogicalValues]] to [[BatchPhysicalValues]]. */
class BatchPhysicalValuesRule(config: Config) extends ConverterRule(config) {

  def convert(rel: RelNode): RelNode = {
    val values: FlinkLogicalValues = rel.asInstanceOf[FlinkLogicalValues]
    val providedTraitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    new BatchPhysicalValues(rel.getCluster, providedTraitSet, values.getTuples, rel.getRowType)
  }
}

object BatchPhysicalValuesRule {
  val INSTANCE: RelOptRule = new BatchPhysicalValuesRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalValues],
      FlinkConventions.LOGICAL,
      FlinkConventions.BATCH_PHYSICAL,
      "BatchPhysicalValuesRule"))
}
