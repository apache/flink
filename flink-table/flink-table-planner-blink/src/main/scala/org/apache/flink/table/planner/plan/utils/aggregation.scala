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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.dataview.DataViewSpec
import org.apache.flink.table.types.DataType

import org.apache.calcite.rel.core.AggregateCall

import scala.collection.mutable.ArrayBuffer

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
    externalAccTypes: Array[DataType],
    viewSpecs: Array[DataViewSpec],
    externalResultType: DataType,
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
    keyType: DataType,
    accType: DataType,
    excludeAcc: Boolean,
    dataViewSpec: Option[DataViewSpec],
    consumeRetraction: Boolean,
    filterArgs: ArrayBuffer[Int],
    aggIndexes: ArrayBuffer[Int])


/**
  * The information contains all aggregate infos, and including input count information.
  *
  * @param aggInfos the information about every aggregates
  * @param indexOfCountStar  None if input count is not needed, otherwise is needed and the index
  *                        represents the count(*) index
  * @param countStarInserted  true when the count(*) is inserted into agg list,
  *                           false when the count(*) is already existent in agg list.
  * @param distinctInfos the distinct information, empty if all the aggregates are not distinct
  */
case class AggregateInfoList(
    aggInfos: Array[AggregateInfo],
    indexOfCountStar: Option[Int],
    countStarInserted: Boolean,
    distinctInfos: Array[DistinctInfo]) {

  def getAggNames: Array[String] = aggInfos.map(_.agg.getName)

  def getAccTypes: Array[DataType] = {
    aggInfos.flatMap(_.externalAccTypes) ++ distinctInfos.filter(!_.excludeAcc).map(_.accType)
  }

  def getActualAggregateCalls: Array[AggregateCall] = {
    getActualAggregateInfos.map(_.agg)
  }

  def getActualFunctions: Array[UserDefinedFunction] = {
    getActualAggregateInfos.map(_.function)
  }

  def getActualValueTypes: Array[DataType] = {
    getActualAggregateInfos.map(_.externalResultType)
  }

  def getIndexOfCountStar: Int = {
    if (indexOfCountStar.nonEmpty) {
      var accOffset = 0
      aggInfos.indices.foreach { i =>
        if (i < indexOfCountStar.get) {
          accOffset += aggInfos(i).externalAccTypes.length
        }
      }
      accOffset
    } else {
      -1
    }
  }

  def getActualAggregateInfos: Array[AggregateInfo] = {
    if (indexOfCountStar.nonEmpty && countStarInserted) {
      // need input count agg and the count1 is inserted,
      // which means the count1 shouldn't be calculated in value
      aggInfos.zipWithIndex
        .filter { case (_, index) => index != indexOfCountStar.get }
        .map { case (aggInfo, _) => aggInfo }
    } else {
      aggInfos
    }
  }
}
