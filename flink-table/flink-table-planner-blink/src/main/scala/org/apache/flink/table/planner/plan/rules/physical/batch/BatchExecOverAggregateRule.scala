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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalOverAggregate
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecOverAggregate, BatchExecOverAggregateBase, BatchExecPythonOverAggregate}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, OverAggregateUtil, SortUtil}
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.tools.ValidationException
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.python.PythonFunctionKind

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Rule that converts [[FlinkLogicalOverAggregate]] to one or more [[BatchExecOverAggregate]]s.
  * If there are more than one [[Group]], this rule will combine adjacent [[Group]]s with the
  * same partition keys and order keys into one BatchExecOverAggregate.
  */
class BatchExecOverAggregateRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalOverAggregate],
      operand(classOf[RelNode], any)),
    "BatchExecOverAggregateRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val logicWindow: FlinkLogicalOverAggregate = call.rel(0)
    var input: RelNode = call.rel(1)
    var inputRowType = logicWindow.getInput.getRowType
    val typeFactory = logicWindow.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val constants = logicWindow.constants.asScala
    val constantTypes = constants.map(c => FlinkTypeFactory.toLogicalType(c.getType))
    val inputNamesWithConstants = inputRowType.getFieldNames ++ constants.indices.map(i => s"TMP$i")
    val inputTypesWithConstants = inputRowType.getFieldList
      .map(i => FlinkTypeFactory.toLogicalType(i.getType)) ++ constantTypes
    val inputTypeWithConstants = typeFactory.buildRelNodeRowType(
      inputNamesWithConstants, inputTypesWithConstants)

    var overWindowAgg: BatchExecOverAggregateBase = null

    var lastGroup: Window.Group = null
    val groupBuffer = ArrayBuffer[Window.Group]()

    def generatorOverAggregate(): Unit = {
      val groupSet: Array[Int] = lastGroup.keys.toArray
      val (orderKeyIndexes, orders, nullIsLasts) = SortUtil.getKeysAndOrders(
        lastGroup.orderKeys.getFieldCollations)

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

      val groupToAggCallToAggFunction = groupBuffer.map { group =>
        val aggregateCalls = group.getAggregateCalls(logicWindow)
        val (_, _, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
          FlinkTypeFactory.toLogicalRowType(inputTypeWithConstants),
          aggregateCalls,
          orderKeyIndexes)
        val aggCallToAggFunction = aggregateCalls.zip(aggregates)
        (group, aggCallToAggFunction)
      }

      val outputRowType = inferOutputRowType(logicWindow.getCluster, inputRowType,
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
        if (existJavaFunction) {
          throw new TableException("Python UDAF and Java/Scala UDAF cannot be used together.")
        }
        if (existPandasFunction && existGeneralPythonFunction) {
          throw new TableException("Pandas UDAF and non-Pandas UDAF cannot be used together.")
        }
      }
      overWindowAgg = if (existJavaFunction) {
        new BatchExecOverAggregate(
          logicWindow.getCluster,
          call.builder(),
          providedTraitSet,
          newInput,
          outputRowType,
          newInput.getRowType,
          groupSet,
          orderKeyIndexes,
          orders,
          nullIsLasts,
          groupToAggCallToAggFunction,
          logicWindow)
      } else {
        new BatchExecPythonOverAggregate(
          logicWindow.getCluster,
          call.builder(),
          providedTraitSet,
          newInput,
          outputRowType,
          newInput.getRowType,
          groupSet,
          orderKeyIndexes,
          orders,
          nullIsLasts,
          groupToAggCallToAggFunction,
          logicWindow)
      }

      input = overWindowAgg
      inputRowType = outputRowType
    }

    logicWindow.groups.foreach { group =>
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

  /**
    * Returns true if group1 satisfies group2 on keys and orderKeys, else false.
    */
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

    val aggNames = aggCalls.map(_.getName)
    val aggTypes = aggCalls.map(_.getType)

    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    typeFactory.createStructType(inputTypeList ++ aggTypes, inputNameList ++ aggNames)
  }

  // SPARK/PostgreSQL don't support distinct on over(), and Hive only support distinct without
  // window frame. Because it is complicated for Distinct on over().
  private def validate(group: Group): Unit = {
    if (group.aggCalls.exists(_.distinct)) {
      throw new ValidationException("Distinct not supported in Windowing function!")
    }
  }
}

object BatchExecOverAggregateRule {
  val INSTANCE: RelOptRule = new BatchExecOverAggregateRule
}
