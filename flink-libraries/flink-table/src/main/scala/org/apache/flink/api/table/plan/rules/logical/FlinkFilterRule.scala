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

package org.apache.flink.api.table.plan.rules.logical

import org.apache.calcite.plan.{Convention, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.flink.api.table.plan.nodes.logical.{FlinkFilter, FlinkConvention}

class FlinkFilterRule
  extends ConverterRule(
      classOf[LogicalFilter],
      Convention.NONE,
      FlinkConvention.INSTANCE,
      "FlinkFilterRule")
  {

    def convert(rel: RelNode): RelNode = {
      val filter: LogicalFilter = rel.asInstanceOf[LogicalFilter]
      val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConvention.INSTANCE)
      val convInput: RelNode = RelOptRule.convert(filter.getInput, FlinkConvention.INSTANCE)

      new FlinkFilter(
        rel.getCluster,
        traitSet,
        convInput,
        filter.getCondition)
    }
  }

object FlinkFilterRule {
  val INSTANCE: RelOptRule = new FlinkFilterRule
}
