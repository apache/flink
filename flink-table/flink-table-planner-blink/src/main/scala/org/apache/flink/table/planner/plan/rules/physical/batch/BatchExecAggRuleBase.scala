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

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.planner.JArrayList
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecGroupAggregateBase, BatchExecLocalHashAggregate, BatchExecLocalSortAggregate}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, FlinkRelOptUtil}
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy
import org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation, RelNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

trait BatchExecAggRuleBase {

  protected def inferLocalAggType(
      inputRowType: RelDataType,
      agg: Aggregate,
      groupSet: Array[Int],
      auxGroupSet: Array[Int],
      aggFunctions: Array[UserDefinedFunction],
      aggBufferTypes: Array[Array[LogicalType]]): RelDataType = {

    val typeFactory = agg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val aggCallNames = Util.skip(
      agg.getRowType.getFieldNames, groupSet.length + auxGroupSet.length).toList.toArray[String]
    inferLocalAggType(
      inputRowType, typeFactory, aggCallNames, groupSet, auxGroupSet, aggFunctions, aggBufferTypes)
  }

  protected def inferLocalAggType(
      inputRowType: RelDataType,
      typeFactory: FlinkTypeFactory,
      aggCallNames: Array[String],
      groupSet: Array[Int],
      auxGroupSet: Array[Int],
      aggFunctions: Array[UserDefinedFunction],
      aggBufferTypes: Array[Array[LogicalType]]): RelDataType = {

    val aggBufferFieldNames = new Array[Array[String]](aggFunctions.length)
    var index = -1
    aggFunctions.zipWithIndex.foreach {
      case (udf, aggIndex) =>
        aggBufferFieldNames(aggIndex) = udf match {
          case _: AggregateFunction[_, _] =>
            Array(aggCallNames(aggIndex))
          case agf: DeclarativeAggregateFunction =>
            agf.aggBufferAttributes.map { attr =>
              index += 1
              s"${attr.getName}$$$index"
            }
          case _: UserDefinedFunction =>
            throw new TableException(s"Don't get localAgg merge name")
        }
    }

    // local agg output order: groupSet + auxGroupSet + aggCalls
    val aggBufferSqlTypes = aggBufferTypes.flatten.map { t =>
      val nullable = !FlinkTypeFactory.isTimeIndicatorType(t)
      typeFactory.createFieldTypeFromLogicalType(t)
    }

    val localAggFieldTypes = (
      groupSet.map(inputRowType.getFieldList.get(_).getType) ++ // groupSet
        auxGroupSet.map(inputRowType.getFieldList.get(_).getType) ++ // auxGroupSet
        aggBufferSqlTypes // aggCalls
      ).toList

    val localAggFieldNames = (
      groupSet.map(inputRowType.getFieldList.get(_).getName) ++ // groupSet
        auxGroupSet.map(inputRowType.getFieldList.get(_).getName) ++ // auxGroupSet
        aggBufferFieldNames.flatten.toArray[String] // aggCalls
      ).toList

    typeFactory.createStructType(localAggFieldTypes, localAggFieldNames)
  }

  protected def isTwoPhaseAggWorkable(
      aggFunctions: Array[UserDefinedFunction],
      tableConfig: TableConfig): Boolean = {
    getAggPhaseStrategy(tableConfig) match {
      case AggregatePhaseStrategy.ONE_PHASE => false
      case _ => doAllSupportMerge(aggFunctions)
    }
  }

  protected def isOnePhaseAggWorkable(
      agg: Aggregate,
      aggFunctions: Array[UserDefinedFunction],
      tableConfig: TableConfig): Boolean = {
    getAggPhaseStrategy(tableConfig) match {
      case AggregatePhaseStrategy.ONE_PHASE => true
      case AggregatePhaseStrategy.TWO_PHASE => !doAllSupportMerge(aggFunctions)
      case AggregatePhaseStrategy.AUTO =>
        if (!doAllSupportMerge(aggFunctions)) {
          true
        } else {
          // if ndv of group key in aggregate is Unknown and all aggFunctions are splittable,
          // use two-phase agg.
          // else whether choose one-phase agg or two-phase agg depends on CBO.
          val mq = agg.getCluster.getMetadataQuery
          mq.getDistinctRowCount(agg.getInput, agg.getGroupSet, null) != null
        }
    }
  }

  protected def doAllSupportMerge(aggFunctions: Array[UserDefinedFunction]): Boolean = {
    val supportLocalAgg = aggFunctions.forall {
      case _: DeclarativeAggregateFunction => true
      case a => ifMethodExistInFunction("merge", a)
    }
    //it means grouping without aggregate functions
    aggFunctions.isEmpty || supportLocalAgg
  }

  protected def isEnforceOnePhaseAgg(tableConfig: TableConfig): Boolean = {
    getAggPhaseStrategy(tableConfig) == AggregatePhaseStrategy.ONE_PHASE
  }

  protected def isEnforceTwoPhaseAgg(tableConfig: TableConfig): Boolean = {
    getAggPhaseStrategy(tableConfig) == AggregatePhaseStrategy.TWO_PHASE
  }

  protected def isAggBufferFixedLength(agg: Aggregate): Boolean = {
    val (_, aggCallsWithoutAuxGroupCalls) = AggregateUtil.checkAndSplitAggCalls(agg)
    val (_, aggBufferTypes, _) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, agg.getInput.getRowType)

    isAggBufferFixedLength(aggBufferTypes.map(_.map(fromDataTypeToLogicalType)))
  }

  protected def isAggBufferFixedLength(aggBufferTypes: Array[Array[LogicalType]]): Boolean = {
    val aggBuffAttributesTypes = aggBufferTypes.flatten
    val isAggBufferFixedLength = aggBuffAttributesTypes.forall(
      t => BinaryRow.isMutable(t))
    // it means grouping without aggregate functions
    aggBuffAttributesTypes.isEmpty || isAggBufferFixedLength
  }

  protected def createRelCollation(groupSet: Array[Int]): RelCollation = {
    val fields = new JArrayList[RelFieldCollation]()
    for (field <- groupSet) {
      fields.add(FlinkRelOptUtil.ofRelFieldCollation(field))
    }
    RelCollations.of(fields)
  }

  protected def getGlobalAggGroupSetPair(
      localAggGroupSet: Array[Int], localAggAuxGroupSet: Array[Int]): (Array[Int], Array[Int]) = {
    val globalGroupSet = localAggGroupSet.indices.toArray
    val globalAuxGroupSet = (localAggGroupSet.length until localAggGroupSet.length +
      localAggAuxGroupSet.length).toArray
    (globalGroupSet, globalAuxGroupSet)
  }

  protected def createLocalAgg(
      cluster: RelOptCluster,
      relBuilder: RelBuilder,
      traitSet: RelTraitSet,
      input: RelNode,
      originalAggRowType: RelDataType,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggBufferTypes: Array[Array[DataType]],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      isLocalHashAgg: Boolean): BatchExecGroupAggregateBase = {
    val inputRowType = input.getRowType
    val aggFunctions = aggCallToAggFunction.map(_._2).toArray

    val typeFactory = input.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val aggCallNames = Util.skip(
      originalAggRowType.getFieldNames, grouping.length + auxGrouping.length).toList.toArray

    val localAggRowType = inferLocalAggType(
      inputRowType,
      typeFactory,
      aggCallNames,
      grouping,
      auxGrouping,
      aggFunctions,
      aggBufferTypes.map(_.map(fromDataTypeToLogicalType)))

    if (isLocalHashAgg) {
      new BatchExecLocalHashAggregate(
        cluster,
        relBuilder,
        traitSet,
        input,
        localAggRowType,
        inputRowType,
        grouping,
        auxGrouping,
        aggCallToAggFunction)
    } else {
      new BatchExecLocalSortAggregate(
        cluster,
        relBuilder,
        traitSet,
        input,
        localAggRowType,
        inputRowType,
        grouping,
        auxGrouping,
        aggCallToAggFunction)
    }
  }

}
