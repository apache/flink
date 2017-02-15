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
import org.apache.calcite.plan.{Convention, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalWindow
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.nodes.datastream.DataStreamConvention
import org.apache.flink.table.plan.nodes.datastream.DataStreamOverAggregate

import scala.collection.JavaConversions._

/**
  * Rule to convert a LogicalWindow into a DataStreamOverAggregate.
  */
class DataStreamOverAggregateRule
  extends ConverterRule(
    classOf[LogicalWindow],
    Convention.NONE,
    DataStreamConvention.INSTANCE,
    "DataStreamOverAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val logicWindow: LogicalWindow = rel.asInstanceOf[LogicalWindow]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convertInput: RelNode =
      RelOptRule.convert(logicWindow.getInput, DataStreamConvention.INSTANCE)

    val inputRowType = convertInput.asInstanceOf[RelSubset].getOriginal.getRowType

    new DataStreamOverAggregate(
      logicWindow,
      rel.getCluster,
      traitSet,
      convertInput,
      rel.getRowType,
      inputRowType)
  }
}

object DataStreamOverAggregateRule {
  val INSTANCE: RelOptRule = new DataStreamOverAggregateRule
}

