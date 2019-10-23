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

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{DataTypes, TableConfig, TableException}
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, Decimal}
import org.apache.flink.table.dataview.MapViewTypeInfo
import org.apache.flink.table.expressions.ExpressionUtils.extractValue
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.dataview.DataViewUtils.useNullSerializerForStateViewFieldsFromAccType
import org.apache.flink.table.planner.dataview.{DataViewSpec, MapViewSpec}
import org.apache.flink.table.planner.expressions.{PlannerProctimeAttribute, PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowStart, RexNodeConverter}
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.functions.sql.{FlinkSqlOperatorTable, SqlListAggFunction, SqlFirstLastValueAggFunction}
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.{fromDataTypeToLogicalType, fromLogicalTypeToDataType}
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot
import org.apache.flink.table.types.logical.{LogicalTypeRoot, _}
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
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
  : (Array[Array[Int]], Array[Array[DataType]], Array[UserDefinedFunction]) = {

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
          val bufferTypes: Array[LogicalType] = a.getAggBufferTypes.map(_.getLogicalType)
          val bufferTypeInfos = bufferTypes.map(fromLogicalTypeToDataType)
          (bufferTypeInfos, Array.empty[DataViewSpec],
              fromLogicalTypeToDataType(a.getResultType.getLogicalType))
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
        externalAccTypes,
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
        typeFactory.createFieldTypeFromLogicalType(new BigIntType()),
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
        val argTypes: Array[LogicalType] = call
          .getArgList
          .map(inputType.getFieldList.get(_).getType)
          .map(FlinkTypeFactory.toLogicalType)
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
        fromDataTypeToTypeInfo(d.keyType),
        valueType,
        isStateBackedDataViews,
        // the mapview serializer should handle null keys
        true)

      val accDataType = fromLegacyInfoToDataType(accTypeInfo)

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
        accDataType,
        excludeAcc = false,
        distinctMapViewSpec,
        consumeRetraction,
        d.filterArgs,
        d.aggIndexes)
    }

    (distinctInfos.toArray, newAggCalls)
  }

  def createDistinctKeyType(argTypes: Array[LogicalType]): DataType = {
    if (argTypes.length == 1) {
      argTypes(0).getTypeRoot match {
      case INTEGER => DataTypes.INT
      case BIGINT => DataTypes.BIGINT
      case SMALLINT => DataTypes.SMALLINT
      case TINYINT => DataTypes.TINYINT
      case FLOAT => DataTypes.FLOAT
      case DOUBLE => DataTypes.DOUBLE
      case BOOLEAN => DataTypes.BOOLEAN

      case DATE => DataTypes.INT
      case TIME_WITHOUT_TIME_ZONE => DataTypes.INT
      case TIMESTAMP_WITHOUT_TIME_ZONE => DataTypes.BIGINT

      case INTERVAL_YEAR_MONTH => DataTypes.INT
      case INTERVAL_DAY_TIME => DataTypes.BIGINT

      case VARCHAR =>
        val dt = argTypes(0).asInstanceOf[VarCharType]
        DataTypes.VARCHAR(dt.getLength).bridgedTo(classOf[BinaryString])
      case CHAR =>
        val dt = argTypes(0).asInstanceOf[CharType]
        DataTypes.CHAR(dt.getLength).bridgedTo(classOf[BinaryString])
      case DECIMAL =>
        val dt = argTypes(0).asInstanceOf[DecimalType]
        DataTypes.DECIMAL(dt.getPrecision, dt.getScale).bridgedTo(classOf[Decimal])
      case t =>
        throw new TableException(s"Distinct aggregate function does not support type: $t.\n" +
          s"Please re-check the data type.")
      }
    } else {
      fromLogicalTypeToDataType(RowType.of(argTypes: _*)).bridgedTo(classOf[BaseRow])
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
             _: SqlListAggFunction => true
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
      .map(FlinkTypeFactory.toLogicalType)
    val groupingNames = groupSet.map(inputType.getFieldNames.get(_))
    val accFieldNames = inferStreamAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelNodeRowType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(fromDataTypeToLogicalType))
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
      .map(FlinkTypeFactory.toLogicalType)
    val groupingNames = groupSet.map(inputRowType.getFieldNames.get(_))
    val accFieldNames = inferAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelNodeRowType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(fromDataTypeToLogicalType))
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
    val size = tableConfig.getConfiguration.getLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE)
    if (size <= 0 ) {
      throw new IllegalArgumentException(
        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE + " must be > 0.")
    }
    new CountBundleTrigger[BaseRow](size)
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
      properties: Seq[PlannerNamedWindowProperty]): (Option[Int], Option[Int], Option[Int]) = {
    val propPos = properties.foldRight(
      (None: Option[Int], None: Option[Int], None: Option[Int], 0)) {
      case (p, (s, e, rt, i)) => p match {
        case PlannerNamedWindowProperty(_, prop) =>
          prop match {
            case PlannerWindowStart(_) if s.isDefined =>
              throw new TableException(
                "Duplicate window start property encountered. This is a bug.")
            case PlannerWindowStart(_) =>
              (Some(i), e, rt, i - 1)
            case PlannerWindowEnd(_) if e.isDefined =>
              throw new TableException("Duplicate window end property encountered. This is a bug.")
            case PlannerWindowEnd(_) =>
              (s, Some(i), rt, i - 1)
            case PlannerRowtimeAttribute(_) if rt.isDefined =>
              throw new TableException(
                "Duplicate window rowtime property encountered. This is a bug.")
            case PlannerRowtimeAttribute(_) =>
              (s, e, Some(i), i - 1)
            case PlannerProctimeAttribute(_) =>
              // ignore this property, it will be null at the position later
              (s, e, rt, i - 1)
          }
      }
    }
    (propPos._1, propPos._2, propPos._3)
  }

  def isRowtimeAttribute(field: FieldReferenceExpression): Boolean = {
    LogicalTypeChecks.isRowtimeAttribute(field.getOutputDataType.getLogicalType)
  }

  def isProctimeAttribute(field: FieldReferenceExpression): Boolean = {
    LogicalTypeChecks.isProctimeAttribute(field.getOutputDataType.getLogicalType)
  }

  def hasTimeIntervalType(intervalType: ValueLiteralExpression): Boolean = {
    hasRoot(intervalType.getOutputDataType.getLogicalType, LogicalTypeRoot.INTERVAL_DAY_TIME)
  }

  def hasRowIntervalType(intervalType: ValueLiteralExpression): Boolean = {
    hasRoot(intervalType.getOutputDataType.getLogicalType, LogicalTypeRoot.BIGINT)
  }

  def toLong(literalExpr: ValueLiteralExpression): JLong =
    extractValue(literalExpr, classOf[JLong]).get()

  def toDuration(literalExpr: ValueLiteralExpression): Duration =
    extractValue(literalExpr, classOf[Duration]).get()
}
