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

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

class DecomposeGroupingSetRule
  extends RelOptRule(
    operand(classOf[LogicalAggregate], any),
  "DecomposeGroupingSetRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalAggregate = call.rel(0).asInstanceOf[LogicalAggregate]

    !agg.getGroupSets.isEmpty &&
      DecomposeGroupingSetRule.getGroupIdExprIndexes(agg.getAggCallList).nonEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: LogicalAggregate = call.rel(0).asInstanceOf[LogicalAggregate]
    val groupIdExprs = DecomposeGroupingSetRule.getGroupIdExprIndexes(agg.getAggCallList).toSet

    val subAggs = agg.groupSets.map(set =>
      DecomposeGroupingSetRule.decompose(call.builder(), agg, groupIdExprs, set))

    val union = subAggs.reduce((l, r) => new LogicalUnion(
      agg.getCluster,
      agg.getTraitSet,
      Seq(l, r),
      true
    ))
    call.transformTo(union)
  }
}

object DecomposeGroupingSetRule {
  val INSTANCE = new DecomposeGroupingSetRule

  private def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
        call.getAggregation.getKind match {
          case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID =>
            true
          case _ =>
            false
        }
    }.map { case (_, idx) => idx}
  }

  private def decompose(
     relBuilder: RelBuilder,
     agg: LogicalAggregate,
     groupExprIndexes : Set[Int],
     groupSet: ImmutableBitSet) = {

    val aggsWithIndexes = agg.getAggCallList.zipWithIndex

    // construct aggregate without groupExpressions
    val subAgg = new LogicalAggregate(
      agg.getCluster,
      agg.getTraitSet,
      agg.getInput,
      false,
      groupSet,
      Seq(),
      aggsWithIndexes.collect{ case (call, idx) if !groupExprIndexes.contains(idx) => call }
    )
    relBuilder.push(subAgg)

    val rexBuilder = relBuilder.getRexBuilder
    // get names of grouping fields
    val groupingFieldsName = Seq.range(0, agg.getGroupCount)
      .map(x => agg.getRowType.getFieldNames.get(x))

    // create null literals for all grouping fields
    val groupingFields: Array[RexNode] = Seq.range(0, agg.getGroupCount)
      .map(x => rexBuilder.makeNullLiteral(agg.getRowType.getFieldList.get(x).getType)).toArray
    // override null literals with field access for grouping fields of current aggregation
    groupSet.toList.zipWithIndex.foreach { case (group, idx) =>
      groupingFields(group) = rexBuilder.makeInputRef(relBuilder.peek(), idx)
    }

    var aggCnt = 0
    val aggFields = aggsWithIndexes.map {
      case (call, idx) if groupExprIndexes.contains(idx) =>
        // create literal for group expression
        lowerGroupExpr(rexBuilder, call, groupSet)
      case _ =>
        // create access to aggregation result
        val aggResult = rexBuilder.makeInputRef(subAgg, subAgg.getGroupCount + aggCnt)
        aggCnt += 1
        aggResult
    }

    // add a projection to establish the result schema and set the values of the group expressions.
    relBuilder.project(
      groupingFields.toSeq ++ aggFields,
      groupingFieldsName ++ agg.getAggCallList.map(_.name))
    // return aggregation + projection
    relBuilder.build()
  }

  /** Returns a literal for a given group expression. */
  private def lowerGroupExpr(
      builder: RexBuilder,
      call: AggregateCall,
      groupSet: ImmutableBitSet) : RexNode = {

    val groups = groupSet.asSet()

    call.getAggregation.getKind match {
      case SqlKind.GROUP_ID =>
        val id = groupSet.asList().map(x => 1 << x).sum
        builder.makeLiteral(id, call.getType, false)
      case SqlKind.GROUPING | SqlKind.GROUPING_ID =>
        val res = call.getArgList.foldLeft(0)((res, arg) =>
          (res << 1) + (if (groups.contains(arg)) 1 else 0)
        )
        builder.makeLiteral(res, call.getType, false)
      case _ => builder.constantNull()
    }
  }
}
