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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.core.{Aggregate, AggregateCall, RelFactories}
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.tools.RelBuilderFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Planner rule that reduces unless grouping columns.
 *
 * Find (minimum) unique group for the grouping columns, and use it as new grouping columns.
 */
class AggregateReduceGroupingRule(relBuilderFactory: RelBuilderFactory)
  extends RelOptRule(
    operand(classOf[Aggregate], any),
    relBuilderFactory,
    "AggregateReduceGroupingRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: Aggregate = call.rel(0)
    agg.getGroupCount > 1 && agg.getGroupType == Group.SIMPLE
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: Aggregate = call.rel(0)
    val aggRowType = agg.getRowType
    val input = agg.getInput
    val inputRowType = input.getRowType
    val originalGrouping = agg.getGroupSet
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
    val newGrouping = fmq.getUniqueGroups(input, originalGrouping)
    val uselessGrouping = originalGrouping.except(newGrouping)
    if (uselessGrouping.isEmpty) {
      return
    }

    // new agg: new grouping + aggCalls for dropped grouping + original aggCalls
    val indexOldToNewMap = new mutable.HashMap[Int, Int]()
    val newGroupingList = newGrouping.toList
    var idxOfNewGrouping = 0
    var idxOfAggCallsForDroppedGrouping = newGroupingList.size()
    originalGrouping.zipWithIndex.foreach {
      case (column, oldIdx) =>
        val newIdx = if (newGroupingList.contains(column)) {
          val p = idxOfNewGrouping
          idxOfNewGrouping += 1
          p
        } else {
          val p = idxOfAggCallsForDroppedGrouping
          idxOfAggCallsForDroppedGrouping += 1
          p
        }
        indexOldToNewMap += (oldIdx -> newIdx)
    }
    require(indexOldToNewMap.size == originalGrouping.cardinality())

    // the indices of aggCalls (or NamedProperties for WindowAggregate) do not change
    (originalGrouping.cardinality() until aggRowType.getFieldCount).foreach {
      index => indexOldToNewMap += (index -> index)
    }

    val aggCallsForDroppedGrouping = uselessGrouping.map {
      column =>
        val fieldType = inputRowType.getFieldList.get(column).getType
        val fieldName = inputRowType.getFieldNames.get(column)
        AggregateCall.create(
          FlinkSqlOperatorTable.AUXILIARY_GROUP,
          false,
          false,
          false,
          ImmutableList.of(column),
          -1,
          null,
          RelCollations.EMPTY,
          fieldType,
          fieldName)
    }.toList

    val newAggCalls = aggCallsForDroppedGrouping ++ agg.getAggCallList
    val newAgg = agg.copy(
      agg.getTraitSet,
      input,
      newGrouping,
      ImmutableList.of(newGrouping),
      newAggCalls
    )
    val builder = call.builder()
    builder.push(newAgg)
    val projects = (0 until aggRowType.getFieldCount).map {
      index =>
        val refIndex = indexOldToNewMap.getOrElse(
          index,
          throw new IllegalArgumentException(s"Illegal index: $index"))
        builder.field(refIndex)
    }
    builder.project(projects, aggRowType.getFieldNames)
    call.transformTo(builder.build())
  }
}

object AggregateReduceGroupingRule {
  val INSTANCE = new AggregateReduceGroupingRule(RelFactories.LOGICAL_BUILDER)
}
