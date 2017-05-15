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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamOverAggregate
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalOverWindow
import org.apache.flink.table.plan.schema.RowSchema

class DataStreamOverAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalOverWindow],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamOverAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val logicWindow: FlinkLogicalOverWindow = rel.asInstanceOf[FlinkLogicalOverWindow]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convertInput: RelNode =
      RelOptRule.convert(logicWindow.getInput, FlinkConventions.DATASTREAM)

    val inputRowType = convertInput.asInstanceOf[RelSubset].getOriginal.getRowType

    new DataStreamOverAggregate(
      logicWindow,
      rel.getCluster,
      traitSet,
      convertInput,
      new RowSchema(rel.getRowType),
      new RowSchema(inputRowType))
  }
}

object DataStreamOverAggregateRule {
  val INSTANCE: RelOptRule = new DataStreamOverAggregateRule
}

