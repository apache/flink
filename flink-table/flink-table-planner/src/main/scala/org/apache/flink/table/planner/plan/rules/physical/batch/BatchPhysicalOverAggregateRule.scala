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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.python.PythonFunctionKind
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalOverAggregate
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalOverAggregate, BatchPhysicalOverAggregateBase, BatchPhysicalPythonOverAggregate}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, OverAggregateUtil, SortUtil}
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.planner.utils.ShortcutUtils

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rex.{RexInputRef, RexNode, RexShuttle}
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.tools.ValidationException

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Rule that converts [[FlinkLogicalOverAggregate]] to one or more [[BatchPhysicalOverAggregate]]s.
 * If there are more than one [[Group]], this rule will combine adjacent [[Group]]s with the same
 * partition keys and order keys into one BatchExecOverAggregate.
 */
class BatchPhysicalOverAggregateRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalOverAggregate], operand(classOf[RelNode], any)),
    "BatchPhysicalOverAggregateRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val logicWindow: FlinkLogicalOverAggregate = call.rel(0)
    var input: RelNode = call.rel(1)
    var inputRowType = logicWindow.getInput.getRowType
    val originInputSize = inputRowType.getFieldCount
    val typeFactory = logicWindow.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val constants = logicWindow.constants.asScala

    var overWindowAgg: BatchPhysicalOverAggregateBase = null

    var lastGroup: Window.Group = null
    val groupBuffer = ArrayBuffer[Window.Group]()

    def generatorOverAggregate(): Unit = {
      val groupSet: Array[Int] = lastGroup.keys.toArray
      val sortSpec = SortUtil.getSortSpec(lastGroup.orderKeys.getFieldCollations)

      val requiredDistribution = if (groupSet.nonEmpty) {
        FlinkRelDistribution.hash(groupSet.map(Integer.valueOf).toList, requireStrict = false)
      } else {
        FlinkRelDistribution.SINGLETON
      }
      var requiredTrait = logicWindow.getTraitSet
        .replace(FlinkConventions.BATCH_PHYSICAL)
        .replace(requiredDistribution)
        .replace(RelCollations.EMPTY)
      if (OverAggregateUtil.needCollationTrait(logicWindow, lastGroup)) {
        val collation = OverAggregateUtil.createCollation(lastGroup)
        if (!collation.equals(RelCollations.EMPTY)) {
          requiredTrait = requiredTrait.replace(collation)
        }
      }

      val newInput = RelOptRule.convert(input, requiredTrait)

      val groupToAggCallToAggFunction = groupBuffer.zipWithIndex.map {
        case (_, idx) =>
          // we may need to adjust the arg index of AggregateCall in the group
          // for the input's size may change
          adjustGroup(groupBuffer, idx, originInputSize, newInput.getRowType.getFieldCount)
          val group = groupBuffer.get(idx)
          val aggregateCalls = group.getAggregateCalls(logicWindow)
          val (_, _, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
            ShortcutUtils.unwrapTypeFactory(input),
            FlinkTypeFactory.toLogicalRowType(generateInputTypeWithConstants()),
            aggregateCalls,
            sortSpec.getFieldIndices)
          val aggCallToAggFunction = aggregateCalls.zip(aggregates)
          (group, aggCallToAggFunction)
      }

      val outputRowType = inferOutputRowType(
        logicWindow.getCluster,
        inputRowType,
        groupToAggCallToAggFunction.flatMap(_._2).map(_._1))

      val providedTraitSet = call.getPlanner.emptyTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      // TODO: split pandas udaf, general python udaf, java/scala udaf into different node
      val existGeneralPythonFunction = groupToAggCallToAggFunction
        .map(_._2)
        .exists(_.map(_._1).exists(isPythonAggregate(_, PythonFunctionKind.GENERAL)))
      val existPandasFunction = groupToAggCallToAggFunction
        .map(_._2)
        .exists(_.map(_._1).exists(isPythonAggregate(_, PythonFunctionKind.PANDAS)))
      val existJavaFunction = groupToAggCallToAggFunction
        .map(_._2)
        .exists(_.map(_._1).exists(!isPythonAggregate(_)))
      if (existPandasFunction || existGeneralPythonFunction) {
        if (existGeneralPythonFunction) {
          throw new TableException("non-Pandas UDAFs are not supported in batch mode currently.")
        }
        if (existJavaFunction) {
          throw new TableException("Python UDAF and Java/Scala UDAF cannot be used together.")
        }
      }
      overWindowAgg = if (existJavaFunction) {
        new BatchPhysicalOverAggregate(
          logicWindow.getCluster,
          providedTraitSet,
          newInput,
          outputRowType,
          newInput.getRowType,
          groupBuffer.clone(),
          logicWindow)
      } else {
        new BatchPhysicalPythonOverAggregate(
          logicWindow.getCluster,
          providedTraitSet,
          newInput,
          outputRowType,
          newInput.getRowType,
          groupBuffer.clone(),
          logicWindow)
      }

      input = overWindowAgg
      inputRowType = outputRowType
    }

    def generateInputTypeWithConstants(): RelDataType = {
      val constantTypes = constants.map(c => FlinkTypeFactory.toLogicalType(c.getType))
      val inputNamesWithConstants = inputRowType.getFieldNames ++
        constants.indices.map(i => s"TMP$i")
      val inputTypesWithConstants = inputRowType.getFieldList
        .map(i => FlinkTypeFactory.toLogicalType(i.getType)) ++ constantTypes
      typeFactory.buildRelNodeRowType(inputNamesWithConstants, inputTypesWithConstants)
    }

    logicWindow.groups.foreach {
      group =>
        validate(group)
        if (lastGroup != null && !satisfies(lastGroup, group, logicWindow)) {
          generatorOverAggregate()
          groupBuffer.clear()
        }
        groupBuffer.add(group)
        lastGroup = group
    }
    if (groupBuffer.nonEmpty) {
      generatorOverAggregate()
    }
    call.transformTo(overWindowAgg)
  }

  /** Returns true if group1 satisfies group2 on keys and orderKeys, else false. */
  def satisfies(group1: Group, group2: Group, logicWindow: FlinkLogicalOverAggregate): Boolean = {
    var isSatisfied = false
    val keyComp = group1.keys.compareTo(group2.keys)
    if (keyComp == 0) {
      val needCollation1 = OverAggregateUtil.needCollationTrait(logicWindow, group1)
      val needCollation2 = OverAggregateUtil.needCollationTrait(logicWindow, group2)
      if (needCollation1 || needCollation2) {
        val collation1 = OverAggregateUtil.createCollation(group1)
        val collation2 = OverAggregateUtil.createCollation(group2)
        isSatisfied = collation1.equals(collation2)
      } else {
        isSatisfied = true
      }
    }
    isSatisfied
  }

  private def inferOutputRowType(
      cluster: RelOptCluster,
      inputType: RelDataType,
      aggCalls: Seq[AggregateCall]): RelDataType = {

    val inputNameList = inputType.getFieldNames
    val inputTypeList = inputType.getFieldList.asScala.map(field => field.getType)

    // we should avoid duplicated names with input column names
    val aggNames = RowTypeUtils.getUniqueName(aggCalls.map(_.getName), inputNameList)
    val aggTypes = aggCalls.map(_.getType)

    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    typeFactory.createStructType(inputTypeList ++ aggTypes, inputNameList ++ aggNames)
  }

  private def adjustGroup(
      groupBuffer: ArrayBuffer[Window.Group],
      groupIdx: Int,
      originInputSize: Int,
      newInputSize: Int): Unit = {
    val inputSizeDiff = newInputSize - originInputSize
    if (inputSizeDiff > 0) {
      // the input's size of this group has increased, adjust the arg index of agg call
      // in the group to make sure the arg index still refers to the origin value
      var hasAdjust = false
      val indexAdjustment = new RexShuttle() {
        override def visitInputRef(inputRef: RexInputRef): RexNode = {
          if (inputRef.getIndex >= originInputSize) {
            hasAdjust = true
            new RexInputRef(inputRef.getIndex + inputSizeDiff, inputRef.getType)
          } else {
            inputRef
          }
        }
      }
      val group = groupBuffer.get(groupIdx)
      val newAggCalls = new util.ArrayList[Window.RexWinAggCall]()
      group.aggCalls.forEach(
        aggCall => {
          val newOperands = indexAdjustment.visitList(aggCall.operands);
          newAggCalls.add(
            new Window.RexWinAggCall(
              aggCall.getOperator.asInstanceOf[SqlAggFunction],
              aggCall.getType,
              newOperands,
              aggCall.ordinal,
              aggCall.distinct,
              aggCall.ignoreNulls))
        })
      if (hasAdjust) {
        groupBuffer.set(
          groupIdx,
          new Group(
            group.keys,
            group.isRows,
            group.lowerBound,
            group.upperBound,
            group.orderKeys,
            newAggCalls))
      }
    }
  }

  // SPARK/PostgreSQL don't support distinct on over(), and Hive only support distinct without
  // window frame. Because it is complicated for Distinct on over().
  private def validate(group: Group): Unit = {
    if (group.aggCalls.exists(_.distinct)) {
      throw new ValidationException("Distinct not supported in Windowing function!")
    }
  }
}

object BatchPhysicalOverAggregateRule {
  val INSTANCE: RelOptRule = new BatchPhysicalOverAggregateRule
}
