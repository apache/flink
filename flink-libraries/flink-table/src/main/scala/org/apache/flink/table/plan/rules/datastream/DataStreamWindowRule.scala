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
import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.logical.LogicalWindow
import org.apache.calcite.rex.{RexInputRef, RexLiteral}
import org.apache.flink.table.api.{SlidingWindow, TableException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.nodes.datastream.{DataStreamAggregate, DataStreamConvention}
import org.apache.flink.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.table.expressions.ExpressionParser

import scala.collection.JavaConversions._

/**
  * Rule to convert a LogicalWindow into a DataStreamAggregate.
  */
class DataStreamWindowRule
  extends ConverterRule(
    classOf[LogicalWindow],
    Convention.NONE,
    DataStreamConvention.INSTANCE,
    "DataStreamWindowRule")
{

  override def convert(rel: RelNode): RelNode = {
    val agg: LogicalWindow = rel.asInstanceOf[LogicalWindow]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, DataStreamConvention.INSTANCE)

    val inputRowType = convInput.asInstanceOf[RelSubset].getOriginal.getRowType

    if (agg.groups.size > 1) {
      for (i <- 0 until agg.groups.size - 1)
        if (agg.groups(i).toString != agg.groups(i + 1).toString) {
          throw new UnsupportedOperationException(
            "Unsupport different window in the same projection")
        }
    }

    val win = agg.groups(0)
    val namedAgg =
      for (i <- 0 until agg.groups.size; aggCalls = agg.groups(i).getAggregateCalls(agg);
           j <- 0 until aggCalls.size)
        yield new CalcitePair[AggregateCall, String](aggCalls.get(j), "w" + i + "$o" + j)

    if (win.isRows) {
      val rowIdx: RexInputRef =
        if (win.lowerBound.getOffset == null) win.upperBound.getOffset.asInstanceOf[RexInputRef]
        else win.lowerBound.getOffset.asInstanceOf[RexInputRef]
      val rowsExpr = RexLiteral.intValue(
        agg.constants.get(rowIdx.getIndex - inputRowType.getFieldCount)) + ".rows"

      new DataStreamAggregate(
        new SlidingWindow(ExpressionParser.parseExpression(rowsExpr),
          ExpressionParser.parseExpression("1.rows")).toLogicalWindow,
        Seq[NamedWindowProperty](),
        rel.getCluster,
        traitSet,
        convInput,
        namedAgg,
        rel.getRowType,
        inputRowType,
        win.keys.toArray)
    } else null
  }
}

object DataStreamWindowRule {
  val INSTANCE: RelOptRule = new DataStreamWindowRule
}

