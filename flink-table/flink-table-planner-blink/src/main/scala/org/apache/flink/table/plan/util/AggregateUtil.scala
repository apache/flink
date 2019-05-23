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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation, Types}
import org.apache.flink.table.JLong
import org.apache.flink.table.`type`.InternalTypes._
import org.apache.flink.table.`type`.{DecimalType, InternalType, InternalTypes, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataview.DataViewUtils.useNullSerializerForStateViewFieldsFromAccType
import org.apache.flink.table.dataview.{DataViewSpec, MapViewSpec}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.functions.sql.{FlinkSqlOperatorTable, SqlConcatAggFunction, SqlFirstLastValueAggFunction}
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.runtime.bundle.trigger.CountBundleTrigger
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, BinaryStringTypeInfo, DecimalTypeInfo, MapViewTypeInfo, TimeIndicatorTypeInfo, TimeIntervalTypeInfo}

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.{SqlKind, SqlRankFunction}
import org.apache.calcite.tools.RelBuilder

import java.time.Duration
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AggregateUtil extends Enumeration {

  /**
    * Returns whether any of the aggregates are accurate DISTINCT.
    *
    * @return Whether any of the aggregates are accurate DISTINCT
    */
  def containsAccurateDistinctCall(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && !call.isApproximate)
  }

  /**
    * Returns whether any of the aggregates are approximate DISTINCT.
    *
    * @return Whether any of the aggregates are approximate DISTINCT
    */
  def containsApproximateDistinctCall(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && call.isApproximate)
  }

  /**
    * Returns indices of group functions.
    */
  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
      call.getAggregation.getKind match {
        case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
        case _ => false
      }
    }.map { case (_, idx) => idx }
  }

  /**
    * Check whether AUXILIARY_GROUP aggCalls is in the front of the given agg's aggCallList,
    * and whether aggCallList contain AUXILIARY_GROUP when the given agg's groupSet is empty
    * or the indicator is true.
    * Returns AUXILIARY_GROUP aggCalls' args and other aggCalls.
    *
    * @param agg aggregate
    * @return returns AUXILIARY_GROUP aggCalls' args and other aggCalls
    */
  def checkAndSplitAggCalls(agg: Aggregate): (Array[Int], Seq[AggregateCall]) = {
    var nonAuxGroupCallsStartIdx = -1

    val aggCalls = agg.getAggCallList
    aggCalls.zipWithIndex.foreach {
      case (call, idx) =>
        if (call.getAggregation == FlinkSqlOperatorTable.AUXILIARY_GROUP) {
          require(call.getArgList.size == 1)
        }
        if (nonAuxGroupCallsStartIdx >= 0) {
          // the left aggCalls should not be AUXILIARY_GROUP
          require(call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP,
            "AUXILIARY_GROUP should be in the front of aggCall list")
        }
        if (nonAuxGroupCallsStartIdx < 0 &&
          call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP) {
          nonAuxGroupCallsStartIdx = idx
        }
    }

    if (nonAuxGroupCallsStartIdx < 0) {
      nonAuxGroupCallsStartIdx = aggCalls.length
    }

    val (auxGroupCalls, otherAggCalls) = aggCalls.splitAt(nonAuxGroupCallsStartIdx)
    if (agg.getGroupCount == 0) {
      require(auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when groupSet is empty")
    }
    if (agg.indicator) {
      require(auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when indicator is true")
    }

    val auxGrouping = auxGroupCalls.map(_.getArgList.head.toInt).toArray
    require(auxGrouping.length + otherAggCalls.length == aggCalls.length)
    (auxGrouping, otherAggCalls)
  }

  def checkAndGetFullGroupSet(agg: Aggregate): Array[Int] = {
    val (auxGroupSet, _) = checkAndSplitAggCalls(agg)
    agg.getGroupSet.toArray ++ auxGroupSet
  }

  def getOutputIndexToAggCallIndexMap(
      aggregateCalls: Seq[AggregateCall],
      inputType: RelDataType,
      orderKeyIdx: Array[Int] = null): util.Map[Integer, Integer] = {
    val aggInfos = transformToAggregateInfoList(
      aggregateCalls,
      inputType,
      orderKeyIdx,
      Array.fill(aggregateCalls.size)(false),
      needInputCount = false,
      isStateBackedDataViews = false,
      needDistinctInfo = false).aggInfos

    val map = new util.HashMap[Integer, Integer]()
    var outputIndex = 0
    aggregateCalls.indices.foreach {
      aggCallIndex =>
        val aggInfo = aggInfos(aggCallIndex)
        val aggBuffers = aggInfo.externalAccTypes
        aggBuffers.indices.foreach { bufferIndex =>
          map.put(outputIndex + bufferIndex, aggCallIndex)
        }
        outputIndex += aggBuffers.length
    }
    map
  }

  def transformToBatchAggregateFunctions(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      orderKeyIdx: Array[Int] = null)
  : (Array[Array[Int]], Array[Array[TypeInformation[_]]], Array[UserDefinedFunction]) = {

    val aggInfos = transformToAggregateInfoList(
      aggregateCalls,
      inputRowType,
      orderKeyIdx,
      Array.fill(aggregateCalls.size)(false),
      needInputCount = false,
      isStateBackedDataViews = false,
      needDistinctInfo = false).aggInfos

    val aggFields = aggInfos.map(_.argIndexes)
    val bufferTypes = aggInfos.map(_.externalAccTypes)
    val functions = aggInfos.map(_.function)

    (aggFields, bufferTypes, functions)
  }

  def transformToBatchAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      orderKeyIdx: Array[Int] = null,
      needRetractions: Array[Boolean] = null): AggregateInfoList = {

    val needRetractionArray = if (needRetractions == null) {
      Array.fill(aggregateCalls.size)(false)
    } else {
      needRetractions
    }

    transformToAggregateInfoList(
      aggregateCalls,
      inputRowType,
      orderKeyIdx,
      needRetractionArray,
      needInputCount = false,
      isStateBackedDataViews = false,
      needDistinctInfo = false)
  }

  def transformToStreamAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      needRetraction: Array[Boolean],
      needInputCount: Boolean,
      isStateBackendDataViews: Boolean,
      needDistinctInfo: Boolean = true): AggregateInfoList = {
    transformToAggregateInfoList(
      aggregateCalls,
      inputRowType,
      orderKeyIdx = null,
      needRetraction ++ Array(needInputCount), // for additional count(*)
      needInputCount,
      isStateBackendDataViews,
      needDistinctInfo)
  }

  /**
    * Transforms calcite aggregate calls to AggregateInfos.
    *
    * @param aggregateCalls   the calcite aggregate calls
    * @param inputRowType     the input rel data type
    * @param orderKeyIdx      the index of order by field in the input, null if not over agg
    * @param needRetraction   whether the aggregate function need retract method
    * @param needInputCount   whether need to calculate the input counts, which is used in
    *                         aggregation with retraction input.If needed,
    *                         insert a count(1) aggregate into the agg list.
    * @param isStateBackedDataViews   whether the dataview in accumulator use state or heap
    * @param needDistinctInfo  whether need to extract distinct information
    */
  private def transformToAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      orderKeyIdx: Array[Int],
      needRetraction: Array[Boolean],
      needInputCount: Boolean,
      isStateBackedDataViews: Boolean,
      needDistinctInfo: Boolean): AggregateInfoList = {

    // Step-1:
    // if need inputCount, find count1 in the existed aggregate calls first,
    // if not exist, insert a new count1 and remember the index
    val (indexOfCountStar, countStarInserted, aggCalls) = insertCountStarAggCall(
      needInputCount,
      aggregateCalls)

    // Step-2:
    // extract distinct information from aggregate calls
    val (distinctInfos, newAggCalls) = extractDistinctInformation(
      needDistinctInfo,
      aggCalls,
      inputRowType,
      isStateBackedDataViews,
      needInputCount) // needInputCount means whether the aggregate consume retractions

    // Step-3:
    // create aggregate information
    val factory = new AggFunctionFactory(inputRowType, orderKeyIdx, needRetraction)
    val aggInfos = newAggCalls.zipWithIndex.map { case (call, index) =>
      val argIndexes = call.getAggregation match {
        case _: SqlRankFunction => orderKeyIdx
        case _ => call.getArgList.map(_.intValue()).toArray
      }

      val function = factory.createAggFunction(call, index)
      val (externalAccTypes, viewSpecs, externalResultType) = function match {
        case a: DeclarativeAggregateFunction =>
          val bufferTypes: Array[InternalType] = a.getAggBufferTypes
          val bufferTypeInfos = bufferTypes.map(
            TypeConverters.createExternalTypeInfoFromInternalType)
          (bufferTypeInfos, Array.empty[DataViewSpec], a.getResultType)
        case a: AggregateFunction[_, _] =>
          val (implicitAccType, implicitResultType) = call.getAggregation match {
            case aggSqlFun: AggSqlFunction =>
              (aggSqlFun.externalAccType, aggSqlFun.externalResultType)
            case _ => (null, null)
          }
          val externalAccType = getAccumulatorTypeOfAggregateFunction(a, implicitAccType)
          val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccType(
            index,
            a,
            externalAccType,
            isStateBackedDataViews)
          (Array(newExternalAccType), specs,
            getResultTypeOfAggregateFunction(a, implicitResultType))
        case _ => throw new TableException(s"Unsupported function: $function")
      }

      AggregateInfo(
        call,
        function,
        index,
        argIndexes,
        externalAccTypes.asInstanceOf[Array[TypeInformation[_]]],
        viewSpecs,
        externalResultType,
        needRetraction(index))

    }.toArray

    AggregateInfoList(aggInfos, indexOfCountStar, countStarInserted, distinctInfos)
  }


  /**
    * Inserts an COUNT(*) aggregate call if needed. The COUNT(*) aggregate call is used
    * to count the number of added and retracted input records.
    * @param needInputCount whether to insert an InputCount aggregate
    * @param aggregateCalls original aggregate calls
    * @return (indexOfCountStar, countStarInserted, newAggCalls)
    */
  private def insertCountStarAggCall(
      needInputCount: Boolean,
      aggregateCalls: Seq[AggregateCall]): (Option[Int], Boolean, Seq[AggregateCall]) = {

    var indexOfCountStar: Option[Int] = None
    var countStarInserted: Boolean = false
    if (!needInputCount) {
      return (indexOfCountStar, countStarInserted, aggregateCalls)
    }

    // if need inputCount, find count(*) in the existed aggregate calls first,
    // if not exist, insert a new count(*) and remember the index
    var newAggCalls = aggregateCalls
    aggregateCalls.zipWithIndex.foreach { case (call, index) =>
      if (call.getAggregation.isInstanceOf[SqlCountAggFunction] &&
        call.filterArg < 0 &&
        call.getArgList.isEmpty &&
        !call.isApproximate &&
        !call.isDistinct) {
        indexOfCountStar = Some(index)
      }
    }

    // count(*) not exist in aggregateCalls, insert a count(*) in it.
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    if (indexOfCountStar.isEmpty) {

      val count1 = AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        false,
        new util.ArrayList[Integer](),
        -1,
        typeFactory.createTypeFromInternalType(InternalTypes.LONG, isNullable = false),
        "_$count1$_")

      indexOfCountStar = Some(aggregateCalls.length)
      countStarInserted = true
      newAggCalls = aggregateCalls ++ Seq(count1)
    }

    (indexOfCountStar, countStarInserted, newAggCalls)
  }

  /**
    * Extracts DistinctInfo array from the aggregate calls,
    * and change the distinct aggregate to non-distinct aggregate.
    *
    * @param needDistinctInfo whether to extract distinct information
    * @param aggCalls   the original aggregate calls
    * @param inputType  the input rel data type
    * @param isStateBackedDataViews whether the dataview in accumulator use state or heap
    * @param consumeRetraction  whether the distinct aggregate consumes retraction messages
    * @return (distinctInfoArray, newAggCalls)
    */
  private def extractDistinctInformation(
      needDistinctInfo: Boolean,
      aggCalls: Seq[AggregateCall],
      inputType: RelDataType,
      isStateBackedDataViews: Boolean,
      consumeRetraction: Boolean): (Array[DistinctInfo], Seq[AggregateCall]) = {

    if (!needDistinctInfo) {
      return (Array(), aggCalls)
    }

    val distinctMap = mutable.LinkedHashMap.empty[String, DistinctInfo]
    val newAggCalls = aggCalls.zipWithIndex.map { case (call, index) =>
      val argIndexes = call.getArgList.map(_.intValue()).toArray

      // extract distinct information and replace a new call
      if (call.isDistinct && !call.isApproximate && argIndexes.length > 0) {
        val argTypes: Array[InternalType] = call
          .getArgList
          .map(inputType.getFieldList.get(_).getType) // RelDataType
          .map(FlinkTypeFactory.toInternalType) // InternalType
          .toArray

        val keyType = createDistinctKeyType(argTypes)
        val distinctInfo = distinctMap.getOrElseUpdate(
          argIndexes.mkString(","),
          DistinctInfo(
            argIndexes,
            keyType,
            null, // later fill in
            excludeAcc = false,
            null, // later fill in
            consumeRetraction,
            ArrayBuffer.empty[Int],
            ArrayBuffer.empty[Int]))
        // add current agg to the distinct agg list
        distinctInfo.filterArgs += call.filterArg
        distinctInfo.aggIndexes += index

        AggregateCall.create(
          call.getAggregation,
          false,
          false,
          call.getArgList,
          -1, // remove filterArg
          call.getType,
          call.getName)
      } else {
        call
      }
    }

    // fill in the acc type and dataview spec
    val distinctInfos = distinctMap.values.zipWithIndex.map { case (d, index) =>
      val valueType = if (consumeRetraction) {
        if (d.filterArgs.length <= 1) {
          Types.LONG
        } else {
          Types.PRIMITIVE_ARRAY(Types.LONG)
        }
      } else {
        if (d.filterArgs.length <= 64) {
          Types.LONG
        } else {
          Types.PRIMITIVE_ARRAY(Types.LONG)
        }
      }

      val accTypeInfo = new MapViewTypeInfo(
        // distinct is internal code gen, use internal type serializer.
        d.keyType,
        valueType,
        isStateBackedDataViews,
        // the mapview serializer should handle null keys
        true)

      val distinctMapViewSpec = if (isStateBackedDataViews) {
        Some(MapViewSpec(
          s"distinctAcc_$index",
          -1, // the field index will not be used
          accTypeInfo))
      } else {
        None
      }

      DistinctInfo(
        d.argIndexes,
        d.keyType,
        accTypeInfo,
        excludeAcc = false,
        distinctMapViewSpec,
        consumeRetraction,
        d.filterArgs,
        d.aggIndexes)
    }

    (distinctInfos.toArray, newAggCalls)
  }

  def createDistinctKeyType(argTypes: Array[InternalType]): TypeInformation[_] = {
    if (argTypes.length == 1) {
      argTypes(0) match {
        case BYTE => Types.BYTE
        case SHORT => Types.SHORT
        case INT => Types.INT
        case LONG => Types.LONG
        case FLOAT => Types.FLOAT
        case DOUBLE => Types.DOUBLE
        case BOOLEAN => Types.BOOLEAN
        case DATE | TIME => Types.INT
        case TIMESTAMP => Types.LONG
        case STRING => BinaryStringTypeInfo.INSTANCE
        case d: DecimalType => DecimalTypeInfo.of(d.precision(), d.scale())
        case t =>
          throw new TableException(s"Distinct aggregate function does not support type: $t.\n" +
            s"Please re-check the data type.")
      }
    } else {
      new BaseRowTypeInfo(argTypes: _*)
    }
  }

  /**
    * Return true if all aggregates can be partially merged. False otherwise.
    */
  def doAllSupportPartialMerge(aggInfos: Array[AggregateInfo]): Boolean = {
    val supportMerge = aggInfos.map(_.function).forall {
      case _: DeclarativeAggregateFunction => true
      case a => ifMethodExistInFunction("merge", a)
    }

    //it means grouping without aggregate functions
    aggInfos.isEmpty || supportMerge
  }

  /**
    * Return true if all aggregates can be split. False otherwise.
    */
  def doAllAggSupportSplit(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.forall { aggCall =>
      aggCall.getAggregation match {
        case _: SqlCountAggFunction |
             _: SqlAvgAggFunction |
             _: SqlMinMaxAggFunction |
             _: SqlSumAggFunction |
             _: SqlSumEmptyIsZeroAggFunction |
             _: SqlSingleValueAggFunction |
             _: SqlConcatAggFunction => true
        case _: SqlFirstLastValueAggFunction => aggCall.getArgList.size() == 1
        case _ => false
      }
    }
  }

  /**
    * Derives output row type from stream local aggregate
    */
  def inferStreamLocalAggRowType(
      aggInfoList: AggregateInfoList,
      inputType: RelDataType,
      groupSet: Array[Int],
      typeFactory: FlinkTypeFactory): RelDataType = {
    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = groupSet
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toInternalType)
    val groupingNames = groupSet.map(inputType.getFieldNames.get(_))
    val accFieldNames = inferStreamAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelDataType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(TypeConverters.createInternalTypeFromTypeInfo))
  }

  /**
    * Derives accumulators names from stream aggregate
    */
  def inferStreamAggAccumulatorNames(aggInfoList: AggregateInfoList): Array[String] = {
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap { i =>
      aggInfoList.aggInfos(i).function match {
        case _: AggregateFunction[_, _] =>
          val name = aggInfoList.aggInfos(i).agg.getAggregation.getName.toLowerCase
          index += 1
          Array(s"$name$$$index")
        case daf: DeclarativeAggregateFunction =>
          daf.aggBufferAttributes.map { a =>
            index += 1
            s"${a.getName}$$$index"
          }
      }
    }
    val distinctBufferNames = aggInfoList.distinctInfos.indices.map { i =>
      s"distinct$$$i"
    }
    (aggBufferNames ++ distinctBufferNames).toArray
  }

  /**
    * Optimize max or min with retraction agg. MaxWithRetract can be optimized to Max if input is
    * update increasing.
    */
  def getNeedRetractions(
      groupCount: Int,
      needRetraction: Boolean,
      monotonicity: RelModifiedMonotonicity,
      aggCalls: Seq[AggregateCall]): Array[Boolean] = {
    val needRetractionArray = Array.fill(aggCalls.size)(needRetraction)
    if (monotonicity != null && needRetraction) {
      aggCalls.zipWithIndex.foreach { case (aggCall, idx) =>
        aggCall.getAggregation match {
          // if monotonicity is decreasing and aggCall is min with retract,
          // set needRetraction to false
          case a: SqlMinMaxAggFunction
            if a.getKind == SqlKind.MIN &&
              monotonicity.fieldMonotonicities(groupCount + idx) == SqlMonotonicity.DECREASING =>
            needRetractionArray(idx) = false
          // if monotonicity is increasing and aggCall is max with retract,
          // set needRetraction to false
          case a: SqlMinMaxAggFunction
            if a.getKind == SqlKind.MAX &&
              monotonicity.fieldMonotonicities(groupCount + idx) == SqlMonotonicity.INCREASING =>
            needRetractionArray(idx) = false
          case _ => // do nothing
        }
      }
    }

    needRetractionArray
  }

  /**
    * Derives output row type from local aggregate
    */
  def inferLocalAggRowType(
      aggInfoList: AggregateInfoList,
      inputRowType: RelDataType,
      groupSet: Array[Int],
      typeFactory: FlinkTypeFactory): RelDataType = {
    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = groupSet
      .map(inputRowType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toInternalType)
    val groupingNames = groupSet.map(inputRowType.getFieldNames.get(_))
    val accFieldNames = inferAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelDataType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(TypeConverters.createInternalTypeFromTypeInfo))
  }

  /**
    * Derives accumulators names from aggregate
    */
  def inferAggAccumulatorNames(aggInfoList: AggregateInfoList): Array[String] = {
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap { i =>
      aggInfoList.aggInfos(i).function match {
        case _: AggregateFunction[_, _] =>
          val name = aggInfoList.aggInfos(i).agg.getAggregation.getName.toLowerCase
          index += 1
          Array(s"$name$$$index")
        case daf: DeclarativeAggregateFunction =>
          daf.aggBufferAttributes.map { a =>
            index += 1
            s"${a.getName}$$$index"
          }
      }
    }
    val distinctBufferNames = aggInfoList.distinctInfos.indices.map { i =>
      s"distinct$$$i"
    }
    (aggBufferNames ++ distinctBufferNames).toArray
  }

  /**
    * Creates a MiniBatch trigger depends on the config.
    */
  def createMiniBatchTrigger(tableConfig: TableConfig): CountBundleTrigger[BaseRow] = {
    new CountBundleTrigger[BaseRow](
      tableConfig.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE))
  }

  /**
    * Compute field index of given timeField expression.
    */
  def timeFieldIndex(
      inputType: RelDataType, relBuilder: RelBuilder, timeField: FieldReferenceExpression): Int = {
    timeField.accept(new RexNodeConverter(relBuilder.values(inputType)))
        .asInstanceOf[RexInputRef].getIndex
  }

  /**
    * Computes the positions of (window start, window end, row time).
    */
  private[flink] def computeWindowPropertyPos(
      properties: Seq[NamedWindowProperty]): (Option[Int], Option[Int], Option[Int]) = {
    val propPos = properties.foldRight(
      (None: Option[Int], None: Option[Int], None: Option[Int], 0)) {
      case (p, (s, e, rt, i)) => p match {
        case NamedWindowProperty(_, prop) =>
          prop match {
            case WindowStart(_) if s.isDefined =>
              throw new TableException(
                "Duplicate window start property encountered. This is a bug.")
            case WindowStart(_) =>
              (Some(i), e, rt, i - 1)
            case WindowEnd(_) if e.isDefined =>
              throw new TableException("Duplicate window end property encountered. This is a bug.")
            case WindowEnd(_) =>
              (s, Some(i), rt, i - 1)
            case RowtimeAttribute(_) if rt.isDefined =>
              throw new TableException(
                "Duplicate window rowtime property encountered. This is a bug.")
            case RowtimeAttribute(_) =>
              (s, e, Some(i), i - 1)
            case ProctimeAttribute(_) =>
              // ignore this property, it will be null at the position later
              (s, e, rt, i - 1)
          }
      }
    }
    (propPos._1, propPos._2, propPos._3)
  }

  def isRowtimeIndicatorType(fieldType: TypeInformation[_]): Boolean = fieldType match {
    case typeInfo: TimeIndicatorTypeInfo => typeInfo.isEventTime
    case _ => false
  }

  def isProctimeIndicatorType(fieldType: TypeInformation[_]): Boolean = fieldType match {
    case typeInfo: TimeIndicatorTypeInfo => !typeInfo.isEventTime
    case _ => false
  }

  def isTimeIntervalType(intervalType: TypeInformation[_]): Boolean = {
    intervalType == TimeIntervalTypeInfo.INTERVAL_MILLIS
  }

  def isRowIntervalType(intervalType: TypeInformation[_]): Boolean = {
    intervalType == BasicTypeInfo.LONG_TYPE_INFO
  }

  def toLong(literalExpr: ValueLiteralExpression): JLong = {
    if (literalExpr.getType == BasicTypeInfo.LONG_TYPE_INFO) {
      literalExpr.getValue match {
        case v: JLong => v
        case _ => throw new IllegalArgumentException()
      }
    } else {
      throw new IllegalArgumentException()
    }
  }

  def toDuration(literalExpr: ValueLiteralExpression): Duration = {
    if (literalExpr.getType == TimeIntervalTypeInfo.INTERVAL_MILLIS) {
      literalExpr.getValue match {
        case v: JLong => Duration.ofMillis(v)
        case _ => throw new IllegalArgumentException()
      }
    } else {
      throw new IllegalArgumentException()
    }
  }

  def extractTimeIntervalValue(literal: ValueLiteralExpression): JLong = {
    if (isTimeIntervalType(literal.getType)) {
      literal.getValue.asInstanceOf[JLong]
    } else {
      throw new IllegalArgumentException()
    }
  }
}
