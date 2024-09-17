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
import org.apache.flink.table.planner.JList
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.{isRowtimeIndicatorType, isTimeIndicatorType}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalAggregate, FlinkLogicalCalc, FlinkLogicalJoin, FlinkLogicalRank}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalIntervalJoin, StreamPhysicalRank, StreamPhysicalTemporalSort}
import org.apache.flink.table.planner.plan.utils.{RankProcessStrategy, RankUtil, WindowJoinUtil}
import org.apache.flink.table.planner.plan.utils.WindowUtil.groupingContainsWindowStartEnd
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}
import org.apache.flink.table.types.logical.IntType

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField, RelDataTypeFieldImpl, RelDataTypeSystem}
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.{RexInputRef, RexNode, RexProgram}
import org.apache.calcite.util.ImmutableBitSet

import java.util
import java.util.Collections

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

/**
 * Rule that matches [[FlinkLogicalAggregate]], and converts it to [[FlinkLogicalRank]] in the case
 * of SELECT DISTINCT queries.
 *
 * e.g. {SELECT DISTINCT a, b, c;} will be converted to [[FlinkLogicalRank]] instead of
 * [[FlinkLogicalAggregate]] in rowtime.
 */
class StreamLogicalOptimizeSelectDistinctRule
  extends RelOptRule(operand(classOf[FlinkLogicalAggregate], any)) {
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val typeSystem = RelDataTypeSystem.DEFAULT
  private val typeFactory = new FlinkTypeFactory(classLoader, typeSystem)
  private val intType: RelDataType =
    typeFactory.createFieldTypeFromLogicalType(new IntType(false))

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel: FlinkLogicalAggregate = call.rel(0)
    // check if it's a SELECT DISTINCT query
    val ret =
      rel.getGroupSet.cardinality() == rel.getRowType.getFieldCount && rel.getAggCallList.isEmpty

    val mq = call.getMetadataQuery
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val windowProperties = fmq.getRelWindowProperties(rel.getInput)
    val grouping = rel.getGroupSet
    if (groupingContainsWindowStartEnd(grouping, windowProperties)) {
      return false // do not match if the grouping set contains window start and end
    }

    if (ret) {
      rel.getGroupSet.toList.foreach(
        i => {
          val field: RelDataTypeField = rel.getInput.getRowType.getFieldList.get(i)
          if (isTimeIndicatorType(field.getType)) {
            return false
          }
        })
    }
    ret
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: FlinkLogicalAggregate = call.rel(0)

    val input = agg.getInput
    val traitSet = agg.getTraitSet
    val cluster = agg.getCluster

    // Create a list of all group keys
    val groupKeys = agg.getGroupSet

    // Create a projection to select only the fields in the DISTINCT clause
    val projectList: JList[RexNode] = {
      val list = new util.ArrayList[RexNode](groupKeys.cardinality())
      groupKeys.toList.foreach(i => list.add(RexInputRef.of(i, input.getRowType)))
      list
    }

    // Create the projected row type
    val projectedFields: JList[RelDataTypeField] = {
      val fields = new util.ArrayList[RelDataTypeField](projectList.size())
      projectList.forEach {
        rexNode =>
          val rexInputRef = rexNode.asInstanceOf[RexInputRef]
          fields.add(input.getRowType.getFieldList.get(rexInputRef.getIndex))
      }
      fields
    }

    // Create the projected row type
    val projectedRowType: RelDataType = typeFactory.createStructType(projectedFields)

    // Create a RelCollation based on all group keys
    val fieldCollations: JList[RelFieldCollation] = {
      val collations = new util.ArrayList[RelFieldCollation](groupKeys.cardinality())
      groupKeys.foreach {
        key => collations.add(new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING))
      }
      collations
    }
    val collation: RelCollation = RelCollations.of(fieldCollations)

    // Create a projection to select only the fields in the DISTINCT clause
    val projection: RelNode = FlinkLogicalCalc.create(
      input,
      RexProgram.create(
        input.getRowType,
        projectList,
        null,
        projectedRowType,
        cluster.getRexBuilder
      ))

    val rank = new FlinkLogicalRank(
      cluster,
      traitSet,
      projection,
      groupKeys,
      collation,
      RankType.ROW_NUMBER,
      new ConstantRankRange(1, 1), // We only want the first row for each group
      new RelDataTypeFieldImpl("rk", projectedRowType.getFieldCount - 1, intType),
      false
    )
    try RankUtil.canConvertToDeduplicate(rank)
    catch {
      case _: Exception =>
        return
    }
    call.transformTo(rank)
  }
}

object StreamLogicalOptimizeSelectDistinctRule {
  val INSTANCE = new StreamLogicalOptimizeSelectDistinctRule()
}
