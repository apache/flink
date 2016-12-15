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

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalCorrelate, LogicalTableFunctionScan}
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.plan.nodes.dataset.{DataSetConvention, DataSetCorrelate}

/**
  * Rule to convert a LogicalCorrelate into a DataSetCorrelate.
  */
class DataSetCorrelateRule
  extends ConverterRule(
      classOf[LogicalCorrelate],
      Convention.NONE,
      DataSetConvention.INSTANCE,
      "DataSetCorrelateRule") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val join: LogicalCorrelate = call.rel(0).asInstanceOf[LogicalCorrelate]
      val right = join.getRight.asInstanceOf[RelSubset].getOriginal


      right match {
        // right node is a table function
        case scan: LogicalTableFunctionScan => true
        // a filter is pushed above the table function
        case filter: LogicalFilter =>
          filter
            .getInput.asInstanceOf[RelSubset]
            .getOriginal
            .isInstanceOf[LogicalTableFunctionScan]
        case _ => false
      }
    }

    override def convert(rel: RelNode): RelNode = {
      val join: LogicalCorrelate = rel.asInstanceOf[LogicalCorrelate]
      val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
      val convInput: RelNode = RelOptRule.convert(join.getInput(0), DataSetConvention.INSTANCE)
      val right: RelNode = join.getInput(1)

      def convertToCorrelate(relNode: RelNode, condition: Option[RexNode]): DataSetCorrelate = {
        relNode match {
          case rel: RelSubset =>
            convertToCorrelate(rel.getRelList.get(0), condition)

          case filter: LogicalFilter =>
            convertToCorrelate(
              filter.getInput.asInstanceOf[RelSubset].getOriginal,
              Some(filter.getCondition))

          case scan: LogicalTableFunctionScan =>
            new DataSetCorrelate(
              rel.getCluster,
              traitSet,
              convInput,
              scan,
              condition,
              rel.getRowType,
              join.getRowType,
              join.getJoinType,
              description)
        }
      }
      convertToCorrelate(right, None)
    }
  }

object DataSetCorrelateRule {
  val INSTANCE: RelOptRule = new DataSetCorrelateRule
}
