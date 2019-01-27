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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.api.{AggPhaseEnforcer, TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.api.functions.{AggregateFunction, DeclarativeAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.util.{AggregateUtil, FlinkRelOptUtil}
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.runtime.aggregate.RelFieldCollations

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.{RelCollations, RelFieldCollation}
import org.apache.calcite.util.Util

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait BaseBatchExecAggRule {

  protected def inferLocalAggType(
      inputType: RelDataType,
      agg: Aggregate,
      groupSet: Array[Int],
      auxGroupSet: Array[Int],
      aggregates: Array[UserDefinedFunction],
      aggBufferTypes: Array[Array[InternalType]]): RelDataType = {

    val typeFactory = agg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val aggCallNames = Util.skip(
      agg.getRowType.getFieldNames, groupSet.length + auxGroupSet.length).toList.toArray[String]
    inferLocalAggType(inputType, typeFactory, aggCallNames, groupSet, auxGroupSet, aggregates,
                      aggBufferTypes)
  }

  protected def inferLocalAggType(
    inputType: RelDataType,
    typeFactory: FlinkTypeFactory,
    aggCallNames: Array[String],
    groupSet: Array[Int],
    auxGroupSet: Array[Int],
    aggregates: Array[UserDefinedFunction],
    aggBufferTypes: Array[Array[InternalType]]): RelDataType = {

    val aggBufferFieldNames = new Array[Array[String]](aggregates.length)
    var index = -1
    aggregates.zipWithIndex.foreach{ case (udf, aggIndex) =>
      aggBufferFieldNames(aggIndex) = udf match {
        case _: AggregateFunction[_, _] =>
          Array(aggCallNames(aggIndex))
        case agf: DeclarativeAggregateFunction =>
          agf.aggBufferAttributes.map { attr =>
            index += 1
            s"${attr.name}$$$index"}.toArray
        case _: UserDefinedFunction =>
          throw new TableException(s"Don't get localAgg merge name")
      }
    }

    //localAggType
    // local agg output order: groupSet + auxGroupSet + aggCalls
    val aggBufferSqlTypes = aggBufferTypes.flatten.map { t =>
      val nullable = !FlinkTypeFactory.isTimeIndicatorType(t)
      typeFactory.createTypeFromInternalType(t, nullable)
    }

    val localAggFieldTypes = (
      groupSet.map(inputType.getFieldList.get(_).getType) ++ // groupSet
        auxGroupSet.map(inputType.getFieldList.get(_).getType) ++ // auxGroupSet
        aggBufferSqlTypes // aggCalls
      ).toList

    val localAggFieldNames = (
      groupSet.map(inputType.getFieldList.get(_).getName) ++ // groupSet
        auxGroupSet.map(inputType.getFieldList.get(_).getName) ++ // auxGroupSet
        aggBufferFieldNames.flatten.toArray[String] // aggCalls
      ).toList

    typeFactory.createStructType(localAggFieldTypes.asJava, localAggFieldNames.asJava)
  }

  protected def inferLocalWindowAggType(
      enableAssignPane: Boolean,
      inputType: RelDataType,
      agg: Aggregate,
      groupSet: Array[Int],
      auxGroupSet: Array[Int],
      windowType: InternalType,
      aggregates: Array[UserDefinedFunction],
      aggBufferTypes: Array[Array[InternalType]]): RelDataType = {
    val aggNames = agg.getNamedAggCalls.map(_.right)

    val aggBufferFieldNames = new Array[Array[String]](aggregates.length)
    var index = -1
    aggregates.zipWithIndex.foreach{ case (udf, aggIndex) =>
      aggBufferFieldNames(aggIndex) = udf match {
        case _: AggregateFunction[_, _] =>
          Array(aggNames(aggIndex))
        case agf: DeclarativeAggregateFunction =>
          agf.aggBufferAttributes.map { attr =>
            index += 1
            s"${attr.name}$$$index"}.toArray
        case _: UserDefinedFunction =>
          throw new TableException(s"Don't get localAgg merge name")
      }
    }

    // local win-agg output order: groupSet + assignTs + auxGroupSet + aggCalls
    val typeFactory = agg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val aggBufferSqlTypes = aggBufferTypes.flatten.map { t =>
      val nullable = !FlinkTypeFactory.isTimeIndicatorType(t)
      typeFactory.createTypeFromInternalType(t, nullable)
    }

    val localAggFieldTypes = (
      groupSet.map(inputType.getFieldList.get(_).getType) ++ // groupSet
        Array(typeFactory.createTypeFromInternalType(windowType, isNullable = false)) ++ // assignTs
        auxGroupSet.map(inputType.getFieldList.get(_).getType) ++ // auxGroupSet
        aggBufferSqlTypes // aggCalls
      ).toList

    val assignTsFieldName = if (enableAssignPane) "assignedPane$" else "assignedWindow$"
    val localAggFieldNames = (
      groupSet.map(inputType.getFieldList.get(_).getName) ++ // groupSet
        Array(assignTsFieldName) ++ // assignTs
        auxGroupSet.map(inputType.getFieldList.get(_).getName) ++ // auxGroupSet
        aggBufferFieldNames.flatten.toArray[String] // aggCalls
      ).toList

    typeFactory.createStructType(localAggFieldTypes.asJava, localAggFieldNames.asJava)
  }

  protected def isTwoPhaseAggWorkable(
    aggregateList: Seq[UserDefinedFunction],
    call: RelOptRuleCall): Boolean = {
    getAggEnforceStrategy(call) match {
      case AggPhaseEnforcer.ONE_PHASE => false
      case _ => doAllSupportMerge(aggregateList)
    }
  }

  protected def isOnePhaseAggWorkable(
    agg: Aggregate,
    aggregateList: Array[UserDefinedFunction],
    call: RelOptRuleCall): Boolean = {
    getAggEnforceStrategy(call) match {
      case AggPhaseEnforcer.ONE_PHASE => true
      case AggPhaseEnforcer.TWO_PHASE => !doAllSupportMerge(aggregateList)
      case AggPhaseEnforcer.NONE =>
        if (!doAllSupportMerge(aggregateList)) {
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

  protected def doAllSupportMerge(
      aggregateList: Seq[UserDefinedFunction]): Boolean = {
    val supportLocalAgg = aggregateList.forall {
      case _: DeclarativeAggregateFunction => true
      case a => ifMethodExistInFunction("merge", a)
    }
    //it means grouping without aggregate functions
    aggregateList.isEmpty || supportLocalAgg
  }

  protected def isEnforceOnePhaseAgg(call: RelOptRuleCall): Boolean =
    getAggEnforceStrategy(call) == AggPhaseEnforcer.ONE_PHASE

  protected def isEnforceTwoPhaseAgg(call: RelOptRuleCall): Boolean =
    getAggEnforceStrategy(call) == AggPhaseEnforcer.TWO_PHASE

  protected def getAggEnforceStrategy(call: RelOptRuleCall): AggPhaseEnforcer.Value = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[TableConfig])
    val aggPrefConfig = tableConfig.getConf.getString(
      TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER)
    AggPhaseEnforcer.values.find(_.toString.equalsIgnoreCase(aggPrefConfig)).getOrElse(
      throw new IllegalArgumentException(
        "Agg phase enforcer can only set to be: NONE, ONE_PHASE, TWO_PHASE!"))
  }

  protected def isAggBufferFixedLength(call: RelOptRuleCall): Boolean = {
    val agg = call.rels(0).asInstanceOf[Aggregate]
    val input = call.rels(1)

    val (_, aggCallsWithoutAuxGroupCalls) = FlinkRelOptUtil.checkAndSplitAggCalls(agg)
    val aggBufferTypes = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, input.getRowType)._2

    isAggBufferFixedLength(aggBufferTypes.map(_.map(_.toInternalType)))
  }

  protected def isAggBufferFixedLength(
      aggBufferTypes: Array[Array[InternalType]]): Boolean = {
    val aggBuffAttributesTypes = aggBufferTypes.flatten
    val isAggBufferFixedLength = aggBuffAttributesTypes.forall(BinaryRow.isMutable)
    //it means grouping without aggregate functions
    aggBuffAttributesTypes.isEmpty || isAggBufferFixedLength
  }

  protected def createRelCollation(groupSet: Array[Int]) = {
    val fields = new JArrayList[RelFieldCollation]()
    for (field <- groupSet) {
      fields.add(RelFieldCollations.of(field))
    }
    RelCollations.of(fields)
  }
}
