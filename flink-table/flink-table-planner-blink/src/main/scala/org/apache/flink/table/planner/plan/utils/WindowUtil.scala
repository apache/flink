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

import org.apache.flink.table.api.{DataTypes, TableConfig, TableException, ValidationException}
import org.apache.flink.table.planner.JBigDecimal
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions._
import org.apache.flink.table.planner.functions.sql.{FlinkSqlOperatorTable, SqlWindowTableFunction}
import org.apache.flink.table.planner.plan.`trait`.RelWindowProperties
import org.apache.flink.table.planner.plan.logical._
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.utils.AggregateUtil.inferAggAccumulatorNames
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy.{TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED, TABLE_EXEC_EMIT_LATE_FIRE_ENABLED}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.logical.TimestampType
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.canBeTimeAttributeType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall, Calc}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.SqlTypeFamily
import org.apache.calcite.util.ImmutableBitSet

import java.time.Duration
import java.util.Collections

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Utilities for window table-valued functions.
 */
object WindowUtil {

  /**
   * Returns true if the grouping keys contain window_start and window_end properties.
   */
  def groupingContainsWindowStartEnd(
      grouping: ImmutableBitSet,
      windowProperties: RelWindowProperties): Boolean = {
    if (windowProperties != null) {
      val windowStarts = windowProperties.getWindowStartColumns
      val windowEnds = windowProperties.getWindowEndColumns
      val hasWindowStart = !windowStarts.intersect(grouping).isEmpty
      val hasWindowEnd = !windowEnds.intersect(grouping).isEmpty
      hasWindowStart && hasWindowEnd
    } else {
      false
    }
  }

  /**
   * Returns true if the [[RexNode]] is a window table-valued function call.
   */
  def isWindowTableFunctionCall(node: RexNode): Boolean = node match {
    case call: RexCall => call.getOperator.isInstanceOf[SqlWindowTableFunction]
    case _ => false
  }

  /**
   * Returns true if expressions in [[Calc]] contain calls on window columns.
   */
  def calcContainsCallsOnWindowColumns(calc: Calc, fmq: FlinkRelMetadataQuery): Boolean = {
    val calcInput = calc.getInput
    val calcInputWindowColumns = fmq.getRelWindowProperties(calcInput).getWindowColumns
    val calcProgram = calc.getProgram
    val condition = calcProgram.getCondition
    if (condition != null) {
      val predicate = calcProgram.expandLocalRef(condition)
      // condition shouldn't contain window columns
      if (FlinkRexUtil.containsExpectedInputRef(predicate, calcInputWindowColumns)) {
        return true
      }
    }
    // the expressions shouldn't contain calls on window columns
    val callsContainProps = calcProgram.getProjectList.map(calcProgram.expandLocalRef).exists {
      case rex: RexCall => FlinkRexUtil.containsExpectedInputRef(rex, calcInputWindowColumns)
      case _ => false
    }
    callsContainProps
  }

  /**
   * Builds a new RexProgram on the input of window-tvf to exclude window columns,
   * but include time attribute.
   *
   * The return tuple consists of 4 elements:
   * (1) the new RexProgram
   * (2) the field index shifting
   * (3) the new index of time attribute on the new RexProgram
   * (4) whether the time attribute is new added
   */
  def buildNewProgramWithoutWindowColumns(
      rexBuilder: RexBuilder,
      oldProgram: RexProgram,
      inputRowType: RelDataType,
      inputTimeAttributeIndex: Int,
      windowColumns: Array[Int]): (RexProgram, Array[Int], Int, Boolean) = {
    val programBuilder = new RexProgramBuilder(inputRowType, rexBuilder)
    // mapping from original field index to new field index
    var containsTimeAttribute = false
    var newTimeAttributeIndex = -1
    val calcFieldShifting = ArrayBuffer[Int]()

    oldProgram.getNamedProjects.foreach { namedProject =>
      val expr = oldProgram.expandLocalRef(namedProject.left)
      val name = namedProject.right
      // project columns except window columns
      expr match {
        case inputRef: RexInputRef if windowColumns.contains(inputRef.getIndex) =>
          calcFieldShifting += -1

        case _ =>
          try {
            programBuilder.addProject(expr, name)
          } catch {
            case e: Throwable =>
              e.printStackTrace()
          }
          val fieldIndex = programBuilder.getProjectList.size() - 1
          calcFieldShifting += fieldIndex
          // check time attribute exists in the calc
          expr match {
            case ref: RexInputRef if ref.getIndex == inputTimeAttributeIndex =>
              containsTimeAttribute = true
              newTimeAttributeIndex = fieldIndex
            case _ => // nothing
          }
      }
    }

    // append time attribute if the calc doesn't refer it
    if (!containsTimeAttribute) {
      programBuilder.addProject(
        inputTimeAttributeIndex,
        inputRowType.getFieldNames.get(inputTimeAttributeIndex))
      newTimeAttributeIndex = programBuilder.getProjectList.size() - 1
    }

    if (oldProgram.getCondition != null) {
      val condition = oldProgram.expandLocalRef(oldProgram.getCondition)
      programBuilder.addCondition(condition)
    }

    val program = programBuilder.getProgram()
    (program, calcFieldShifting.toArray, newTimeAttributeIndex, !containsTimeAttribute)
  }

  /**
   * Converts a [[RexCall]] into [[TimeAttributeWindowingStrategy]], the [[RexCall]] must be a
   * window table-valued function call.
   */
  def convertToWindowingStrategy(
      windowCall: RexCall,
      inputRowType: RelDataType): TimeAttributeWindowingStrategy = {
    if (!isWindowTableFunctionCall(windowCall)) {
      throw new IllegalArgumentException(s"RexCall $windowCall is not a window table-valued " +
        "function, can't convert it into WindowingStrategy")
    }

    val timeIndex = getTimeAttributeIndex(windowCall.operands(1))
    val fieldType = inputRowType.getFieldList.get(timeIndex).getType
    if (!FlinkTypeFactory.isTimeIndicatorType(fieldType)) {
      throw new ValidationException("Window can only be defined on a time attribute column, " +
        "but is type of " + fieldType)
    }
    val timeAttributeType = FlinkTypeFactory.toLogicalType(fieldType)
    if (!canBeTimeAttributeType(timeAttributeType)) {
      throw new ValidationException("The supported time indicator type are TIMESTAMP" +
        " and TIMESTAMP_LTZ, but is " + FlinkTypeFactory.toLogicalType(fieldType) + "")
    }

    val windowFunction = windowCall.getOperator.asInstanceOf[SqlWindowTableFunction]
    val windowSpec = windowFunction match {
      case FlinkSqlOperatorTable.TUMBLE =>
        val interval = getOperandAsLong(windowCall.operands(2))
        new TumblingWindowSpec(Duration.ofMillis(interval))

      case FlinkSqlOperatorTable.HOP =>
        val slide = getOperandAsLong(windowCall.operands(2))
        val size = getOperandAsLong(windowCall.operands(3))
        new HoppingWindowSpec(Duration.ofMillis(size), Duration.ofMillis(slide))

      case FlinkSqlOperatorTable.CUMULATE =>
        val step = getOperandAsLong(windowCall.operands(2))
        val maxSize = getOperandAsLong(windowCall.operands(3))
        new CumulativeWindowSpec(Duration.ofMillis(maxSize), Duration.ofMillis(step))
    }

    new TimeAttributeWindowingStrategy(windowSpec, timeAttributeType, timeIndex)
  }

  /**
   * Window TVF based aggregations don't support early-fire and late-fire,
   * throws exception when the configurations are set.
   */
  def checkEmitConfiguration(tableConfig: TableConfig): Unit = {
    val conf = tableConfig.getConfiguration
    if (conf.getBoolean(TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED) ||
      conf.getBoolean(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED)) {
      throw new TableException("Currently, window table function based aggregate doesn't " +
        s"support early-fire and late-fire configuration " +
        s"'${TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED.key()}' and " +
        s"'${TABLE_EXEC_EMIT_LATE_FIRE_ENABLED.key()}'.")
    }
  }

  // ------------------------------------------------------------------------------------------
  // RelNode RowType
  // ------------------------------------------------------------------------------------------

  def deriveWindowAggregateRowType(
      grouping: Array[Int],
      aggCalls: Seq[AggregateCall],
      windowing: WindowingStrategy,
      namedWindowProperties: Seq[PlannerNamedWindowProperty],
      inputRowType: RelDataType,
      typeFactory: FlinkTypeFactory): RelDataType = {
    val groupSet = ImmutableBitSet.of(grouping: _*)
    val baseType = Aggregate.deriveRowType(
      typeFactory,
      inputRowType,
      false,
      groupSet,
      Collections.singletonList(groupSet),
      aggCalls)
    val builder = typeFactory.builder
    builder.addAll(baseType.getFieldList)
    namedWindowProperties.foreach { namedProp =>
      // use types from windowing strategy which keeps the precision and timestamp type
      // cast the type to not null type, because window properties should never be null
      val timeType = namedProp.getProperty match {
        case _: PlannerWindowStart | _: PlannerWindowEnd =>
          new TimestampType(false, 3)
        case _: PlannerRowtimeAttribute | _: PlannerProctimeAttribute =>
          windowing.getTimeAttributeType.copy(false)
      }
      builder.add(namedProp.getName, typeFactory.createFieldTypeFromLogicalType(timeType))
    }
    builder.build()
  }

  /**
   * Derives output row type from local window aggregate
   */
  def deriveLocalWindowAggregateRowType(
      aggInfoList: AggregateInfoList,
      grouping: Array[Int],
      endPropertyName: String,
      inputRowType: RelDataType,
      typeFactory: FlinkTypeFactory): RelDataType = {
    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = grouping
      .map(inputRowType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
    val sliceEndType = Array(DataTypes.BIGINT().getLogicalType)

    val groupingNames = grouping.map(inputRowType.getFieldNames.get(_))
    val accFieldNames = inferAggAccumulatorNames(aggInfoList)
    val sliceEndName = Array(s"$$$endPropertyName")

    typeFactory.buildRelNodeRowType(
      groupingNames ++ accFieldNames ++ sliceEndName,
      groupingTypes ++ accTypes.map(fromDataTypeToLogicalType) ++ sliceEndType)
  }

  // ------------------------------------------------------------------------------------------
  // Private Helpers
  // ------------------------------------------------------------------------------------------

  private def getTimeAttributeIndex(operand: RexNode): Int = {
    val timeAttributeIndex = operand match {
      case call: RexCall if call.getKind == SqlKind.DESCRIPTOR =>
        call.operands(0) match {
          case inputRef: RexInputRef => inputRef.getIndex
          case _ => -1
        }
      case _ => -1
    }
    if (timeAttributeIndex == -1) {
      throw new TableException(
        s"Failed to get time attribute index from $operand. " +
          "This is a bug, please file a JIRA issue.")
    }
    timeAttributeIndex
  }

  private def getOperandAsLong(operand: RexNode): Long = {
    operand match {
      case v: RexLiteral if v.getTypeName.getFamily == SqlTypeFamily.INTERVAL_DAY_TIME =>
        v.getValue.asInstanceOf[JBigDecimal].longValue()
      case _: RexLiteral => throw new TableException(
        "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. " +
          "MONTH and YEAR time unit are not supported yet.")
      case _ => throw new TableException("Only constant window descriptors are supported.")
    }
  }

}
