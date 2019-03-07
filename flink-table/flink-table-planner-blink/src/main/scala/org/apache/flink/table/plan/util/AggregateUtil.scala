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
package org.apache.flink.table.plan.util

import org.apache.flink.table.`type`.InternalType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.dataview.DataViewSpec
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlKind

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Utility methods for aggregate operator.
  */
object AggregateUtil {

  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
      call.getAggregation.getKind match {
        case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
        case _ => false
      }
    }.map { case (_, idx) => idx }
  }

  /**
    * Returns whether any of the aggregates are accurate DISTINCT.
    *
    * @return Whether any of the aggregates are accurate DISTINCT
    */
  def containsAccurateDistinctCall(aggCalls: Seq[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && !call.isApproximate)
  }

  def groupingToString(inputType: RelDataType, grouping: Array[Int]): String = {
    val inFields = inputType.getFieldNames
    grouping.map(inFields(_)).mkString(", ")
  }

  def aggregationToString(
      inputType: RelDataType,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      rowType: RelDataType,
      aggCalls: Seq[AggregateCall],
      aggFunctions: Seq[UserDefinedFunction],
      isMerge: Boolean,
      isGlobal: Boolean,
      distinctInfos: Seq[DistinctInfo] = Seq()): String = {
    val prefix = if (isMerge) {
      "Final_"
    } else if (!isGlobal) {
      "Partial_"
    } else {
      ""
    }

    val inFields = inputType.getFieldNames
    val outFields = rowType.getFieldNames
    val fullGrouping = grouping ++ auxGrouping

    val distinctFieldNames = distinctInfos.indices.map(index => s"distinct$$$index")
    val distinctStrings = if (isMerge) {
      // not output distinct fields in global merge
      Seq()
    } else {
      distinctInfos.map { distinct =>
        val argListNames = distinct.argIndexes.map(inFields).mkString(",")
        // TODO Refactor local&global aggregate name
        val filterNames = distinct.filterArgs.filter(_ > 0).map(inFields).mkString(", ")
        if (filterNames.nonEmpty) {
          s"DISTINCT($argListNames) FILTER ($filterNames)"
        } else {
          s"DISTINCT($argListNames)"
        }
      }
    }

    val aggToDistinctMapping = mutable.HashMap.empty[Int, String]
    distinctInfos.zipWithIndex.foreach {
      case (distinct, index) =>
        distinct.aggIndexes.foreach {
          aggIndex =>
            aggToDistinctMapping += (aggIndex -> distinctFieldNames(index))
        }
    }

    // agg
    var offset = fullGrouping.length
    val aggStrings = aggCalls.zip(aggFunctions).zipWithIndex.map {
      case ((aggCall, udf), index) =>
        val distinct = if (aggCall.isDistinct) {
          if (aggCall.getArgList.size() == 0) {
            "DISTINCT"
          } else {
            "DISTINCT "
          }
        } else {
          if (isMerge && aggToDistinctMapping.contains(index)) {
            "DISTINCT "
          } else {
            ""
          }
        }
        var newArgList = aggCall.getArgList.map(_.toInt).toList
        if (isMerge) {
          newArgList = udf match {
            case _: AggregateFunction[_, _] =>
              val argList = List(offset)
              offset = offset + 1
              argList
            case _ =>
              // TODO supports DeclarativeAggregateFunction
              throw new TableException("Unsupported now")
          }
        }
        val argListNames = if (aggToDistinctMapping.contains(index)) {
          aggToDistinctMapping(index)
        } else if (newArgList.nonEmpty) {
          newArgList.map(inFields(_)).mkString(", ")
        } else {
          "*"
        }

        if (aggCall.filterArg >= 0 && aggCall.filterArg < inFields.size) {
          s"${aggCall.getAggregation}($distinct$argListNames) FILTER ${inFields(aggCall.filterArg)}"
        } else {
          s"${aggCall.getAggregation}($distinct$argListNames)"
        }
    }

    //output for agg
    offset = fullGrouping.length
    val outFieldNames = aggFunctions.map { udf =>
      val outFieldName = if (isGlobal) {
        val name = outFields(offset)
        offset = offset + 1
        name
      } else {
        udf match {
          case _: AggregateFunction[_, _] =>
            val name = outFields(offset)
            offset = offset + 1
            name
          case _ =>
            // TODO supports DeclarativeAggregateFunction
            throw new TableException("Unsupported now")
        }
      }
      outFieldName
    }

    (fullGrouping.map(inFields(_)) ++ aggStrings ++ distinctStrings).zip(
      fullGrouping.indices.map(outFields(_)) ++ outFieldNames ++ distinctFieldNames).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$prefix$f AS $o"
      }
    }.mkString(", ")
  }

}

/**
  * The information about aggregate function call
  *
  * @param agg  calcite agg call
  * @param function AggregateFunction or DeclarativeAggregateFunction
  * @param aggIndex the index of the aggregate call in the aggregation list
  * @param argIndexes the aggregate arguments indexes in the input
  * @param externalAccTypes  accumulator types
  * @param viewSpecs  data view specs
  * @param externalResultType the result type of aggregate
  * @param consumeRetraction whether the aggregate consumes retractions
  */
case class AggregateInfo(
    agg: AggregateCall,
    function: UserDefinedFunction,
    aggIndex: Int,
    argIndexes: Array[Int],
    externalAccTypes: Array[InternalType],
    viewSpecs: Array[DataViewSpec],
    externalResultType: InternalType,
    consumeRetraction: Boolean)

/**
  * The information about shared distinct of the aggregates. It indicates which aggregates are
  * distinct aggregates.
  *
  * @param argIndexes the distinct aggregate arguments indexes in the input
  * @param keyType the distinct key type
  * @param accType the accumulator type of the shared distinct
  * @param excludeAcc whether the distinct acc should excluded from the aggregate accumulator.
  *                    e.g. when this works in incremental mode, returns true, otherwise false.
  * @param dataViewSpec data view spec about this distinct agg used to generate state access,
  * None when dataview is not worked in state mode
  * @param consumeRetraction whether the distinct agg consumes retractions
  * @param filterArgs the ordinal of filter argument for each aggregate, -1 means without filter
  * @param aggIndexes the distinct aggregate index in the aggregation list
  */
case class DistinctInfo(
    argIndexes: Array[Int],
    keyType: InternalType,
    accType: InternalType,
    excludeAcc: Boolean,
    dataViewSpec: Option[DataViewSpec],
    consumeRetraction: Boolean,
    filterArgs: ArrayBuffer[Int],
    aggIndexes: ArrayBuffer[Int])