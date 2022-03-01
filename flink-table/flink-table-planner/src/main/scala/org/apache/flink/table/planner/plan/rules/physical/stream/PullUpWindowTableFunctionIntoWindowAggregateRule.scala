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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalExchange, StreamPhysicalWindowAggregate, StreamPhysicalWindowTableFunction}
import org.apache.flink.table.planner.plan.utils.WindowUtil
import org.apache.flink.table.planner.plan.utils.WindowUtil.buildNewProgramWithoutWindowColumns

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

/**
 * Planner rule that tries to pull up [[StreamPhysicalWindowTableFunction]] into a
 * [[StreamPhysicalWindowAggregate]].
 */
class PullUpWindowTableFunctionIntoWindowAggregateRule
  extends RelOptRule(
    operand(classOf[StreamPhysicalWindowAggregate],
      operand(classOf[StreamPhysicalExchange],
        operand(classOf[StreamPhysicalCalc],
          operand(classOf[StreamPhysicalWindowTableFunction], any())))),
    "PullUpWindowTableFunctionIntoWindowAggregateRule"){

  override def matches(call: RelOptRuleCall): Boolean = {
    val windowAgg: StreamPhysicalWindowAggregate = call.rel(0)
    val calc: StreamPhysicalCalc = call.rel(2)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(windowAgg.getCluster.getMetadataQuery)

    // condition and projection of Calc shouldn't contain calls on window columns,
    // otherwise, we can't transpose WindowTVF and Calc
    if (WindowUtil.calcContainsCallsOnWindowColumns(calc, fmq)) {
      return false
    }

    val aggInputWindowProps = fmq.getRelWindowProperties(calc).getWindowColumns
    // aggregate call shouldn't be on window columns
    // TODO: this can be supported in the future by referencing them as a RexFieldVariable
    windowAgg.aggCalls.forall { call =>
      aggInputWindowProps.intersect(ImmutableBitSet.of(call.getArgList)).isEmpty
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val windowAgg: StreamPhysicalWindowAggregate = call.rel(0)
    val calc: StreamPhysicalCalc = call.rel(2)
    val windowTVF: StreamPhysicalWindowTableFunction = call.rel(3)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(windowAgg.getCluster.getMetadataQuery)
    val cluster = windowAgg.getCluster
    val input = windowTVF.getInput
    val inputRowType = input.getRowType

    val requiredInputTraitSet = input.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(input, requiredInputTraitSet)

    // -------------------------------------------------------------------------
    //  1. transpose Calc and WindowTVF, build the new Calc node
    // -------------------------------------------------------------------------
    val windowColumns = fmq.getRelWindowProperties(windowTVF).getWindowColumns
    val (newProgram, aggInputFieldsShift, timeAttributeIndex, _) =
      buildNewProgramWithoutWindowColumns(
        cluster.getRexBuilder,
        calc.getProgram,
        inputRowType,
        windowTVF.windowing.getTimeAttributeIndex,
        windowColumns.toArray)
    val newCalc = new StreamPhysicalCalc(
      cluster,
      calc.getTraitSet,
      newInput,
      newProgram,
      newProgram.getOutputRowType)

    // -------------------------------------------------------------------------
    //  2. Adjust grouping index and convert Calc with new distribution
    // -------------------------------------------------------------------------
    val newGrouping = windowAgg.grouping
      .map(aggInputFieldsShift(_))
    val requiredDistribution = if (newGrouping.length != 0) {
      FlinkRelDistribution.hash(newGrouping, requireStrict = true)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = newCalc.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val convertedCalc = RelOptRule.convert(newCalc, requiredTraitSet)

    // -----------------------------------------------------------------------------
    //  3. Adjust aggregate arguments index and construct new window aggregate node
    // -----------------------------------------------------------------------------
    val newWindowing = new TimeAttributeWindowingStrategy(
      windowTVF.windowing.getWindow,
      windowTVF.windowing.getTimeAttributeType,
      timeAttributeIndex)
    val providedTraitSet = windowAgg.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newAggCalls = windowAgg.aggCalls.map { call =>
      val newArgList = call.getArgList.map(arg => Int.box(aggInputFieldsShift(arg)))
      val newFilterArg = if (call.hasFilter) {
        aggInputFieldsShift(call.filterArg)
      } else {
        call.filterArg
      }
      val newFiledCollations = call.getCollation.getFieldCollations.map { field =>
        field.withFieldIndex(aggInputFieldsShift(field.getFieldIndex))
      }
      val newCollation = RelCollations.of(newFiledCollations)
      call.copy(newArgList, newFilterArg, newCollation)
    }

    val newWindowAgg = new StreamPhysicalWindowAggregate(
      cluster,
      providedTraitSet,
      convertedCalc,
      newGrouping,
      newAggCalls,
      newWindowing,
      windowAgg.namedWindowProperties)

    call.transformTo(newWindowAgg)
  }
}

object PullUpWindowTableFunctionIntoWindowAggregateRule {
  val INSTANCE = new PullUpWindowTableFunctionIntoWindowAggregateRule
}
