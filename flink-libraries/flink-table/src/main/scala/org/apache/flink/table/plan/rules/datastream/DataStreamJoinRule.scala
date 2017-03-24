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

import org.apache.calcite.plan.{ Convention, RelOptRule, RelOptRuleCall, RelTraitSet }
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.Alias
//import org.apache.flink.table.plan.logical.rel.
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.flink.table.plan.nodes.datastream.DataStreamConvention
import scala.collection.JavaConversions._
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelTraitSet
import org.apache.flink.table.plan.nodes.datastream.DataStreamJoin
import org.apache.calcite.rel.RelNode

import org.apache.calcite.rel.convert.ConverterRule

class DataStreamJoinRule
    extends ConverterRule(
      classOf[LogicalJoin],
      Convention.NONE,
      DataStreamConvention.INSTANCE,
      "DataStreamJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
        
    super.matches(call)
  }

   
  override def convert(rel: RelNode): RelNode = {
    val calc: LogicalJoin = rel.asInstanceOf[LogicalJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)

    val left: RelNode = RelOptRule.convert(calc.getLeft(), DataStreamConvention.INSTANCE)
    val right: RelNode = RelOptRule.convert(calc.getRight(), DataStreamConvention.INSTANCE)

    new DataStreamJoin(
      calc,
      rel.getCluster,
      traitSet,
      left,
      right,
      rel.getRowType,
      description + calc.getId())
  }
}

object DataStreamJoinRule {
  val INSTANCE: RelOptRule = new DataStreamJoinRule
}
