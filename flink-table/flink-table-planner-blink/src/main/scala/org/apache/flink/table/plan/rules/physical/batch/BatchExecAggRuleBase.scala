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

import org.apache.flink.table.JArrayList
import org.apache.flink.table.`type`.{InternalType, TypeConverters}
import org.apache.flink.table.api.{AggPhaseEnforcer, PlannerConfigOptions, TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.plan.util.{AggregateUtil, FlinkRelOptUtil}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation}
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

trait BatchExecAggRuleBase {

  protected def inferLocalAggType(
      inputRowType: RelDataType,
      agg: Aggregate,
      groupSet: Array[Int],
      auxGroupSet: Array[Int],
      aggFunctions: Array[UserDefinedFunction],
      aggBufferTypes: Array[Array[InternalType]]): RelDataType = {

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
      aggBufferTypes: Array[Array[InternalType]]): RelDataType = {

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
      typeFactory.createTypeFromInternalType(t, nullable)
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
    getAggEnforceStrategy(tableConfig) match {
      case AggPhaseEnforcer.ONE_PHASE => false
      case _ => doAllSupportMerge(aggFunctions)
    }
  }

  protected def isOnePhaseAggWorkable(
      agg: Aggregate,
      aggFunctions: Array[UserDefinedFunction],
      tableConfig: TableConfig): Boolean = {
    getAggEnforceStrategy(tableConfig) match {
      case AggPhaseEnforcer.ONE_PHASE => true
      case AggPhaseEnforcer.TWO_PHASE => !doAllSupportMerge(aggFunctions)
      case AggPhaseEnforcer.NONE =>
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
    getAggEnforceStrategy(tableConfig) == AggPhaseEnforcer.ONE_PHASE
  }

  protected def isEnforceTwoPhaseAgg(tableConfig: TableConfig): Boolean = {
    getAggEnforceStrategy(tableConfig) == AggPhaseEnforcer.TWO_PHASE
  }

  protected def getAggEnforceStrategy(tableConfig: TableConfig): AggPhaseEnforcer.Value = {
    val aggPrefConfig = tableConfig.getConf.getString(
      PlannerConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER)
    AggPhaseEnforcer.values.find(_.toString.equalsIgnoreCase(aggPrefConfig))
      .getOrElse(throw new IllegalArgumentException(
        "Agg phase enforcer can only set to be: NONE, ONE_PHASE, TWO_PHASE!"))
  }

  protected def isAggBufferFixedLength(agg: Aggregate): Boolean = {
    val (_, aggCallsWithoutAuxGroupCalls) = AggregateUtil.checkAndSplitAggCalls(agg)
    val (_, aggBufferTypes, _) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, agg.getInput.getRowType)

    isAggBufferFixedLength(aggBufferTypes.map(_.map(TypeConverters.createInternalTypeFromTypeInfo)))
  }

  protected def isAggBufferFixedLength(aggBufferTypes: Array[Array[InternalType]]): Boolean = {
    val aggBuffAttributesTypes = aggBufferTypes.flatten
    val isAggBufferFixedLength = aggBuffAttributesTypes.forall(BinaryRow.isMutable)
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
}
