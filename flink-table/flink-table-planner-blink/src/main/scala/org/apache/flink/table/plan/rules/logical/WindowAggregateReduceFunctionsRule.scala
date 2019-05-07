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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.plan.nodes.calcite.LogicalWindowAggregate

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall, RelFactories}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import java.util

import scala.collection.JavaConversions._

/**
  * Rule to convert complex aggregation functions into simpler ones.
  * Have a look at [[AggregateReduceFunctionsRule]] for details.
  */
class WindowAggregateReduceFunctionsRule
  extends AggregateReduceFunctionsRule(
    operand(classOf[LogicalWindowAggregate], any()),
    RelFactories.LOGICAL_BUILDER) {

  override def newAggregateRel(
      relBuilder: RelBuilder,
      oldAgg: Aggregate,
      newCalls: util.List[AggregateCall]): Unit = {

    // create a LogicalAggregate with simpler aggregation functions
    super.newAggregateRel(relBuilder, oldAgg, newCalls)
    // pop LogicalAggregate from RelBuilder
    val newAgg = relBuilder.build().asInstanceOf[LogicalAggregate]

    // create a new LogicalWindowAggregate (based on the new LogicalAggregate) and push it on the
    // RelBuilder
    val oldWindowAgg = oldAgg.asInstanceOf[LogicalWindowAggregate]
    val newWindowAgg = LogicalWindowAggregate.create(
      oldWindowAgg.getWindow,
      oldWindowAgg.getNamedProperties,
      newAgg)
    relBuilder.push(newWindowAgg)
  }

  override def newCalcRel(
      relBuilder: RelBuilder,
      rowType: RelDataType,
      exprs: util.List[RexNode]): Unit = {
    val numExprs = exprs.size()
    // add all named properties of the window to the selection
    rowType
      .getFieldList
      .subList(numExprs, rowType.getFieldCount).toList
      .foreach(f => exprs.add(relBuilder.field(f.getName)))
    // create a LogicalCalc that computes the complex aggregates and forwards the window properties
    relBuilder.project(exprs, rowType.getFieldNames)
  }

}

object WindowAggregateReduceFunctionsRule {
  val INSTANCE = new WindowAggregateReduceFunctionsRule
}
