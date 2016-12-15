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

package org.apache.flink.table.plan.logical

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.{BatchTableEnvironment, StreamTableEnvironment, TableEnvironment}
import org.apache.flink.table.expressions._
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo, TypeCoercion}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

abstract class EventTimeGroupWindow(
    alias: Option[Expression],
    time: Expression)
  extends LogicalWindow(alias) {

  override def validate(tableEnv: TableEnvironment): ValidationResult = {
    val valid = super.validate(tableEnv)
    if (valid.isFailure) {
        return valid
    }

    tableEnv match {
      case _: StreamTableEnvironment =>
        time match {
          case RowtimeAttribute() =>
            ValidationSuccess
          case _ =>
            ValidationFailure("Event-time window expects a 'rowtime' time field.")
      }
      case _: BatchTableEnvironment =>
        if (!TypeCoercion.canCast(time.resultType, BasicTypeInfo.LONG_TYPE_INFO)) {
          ValidationFailure(s"Event-time window expects a time field that can be safely cast " +
            s"to Long, but is ${time.resultType}")
        } else {
          ValidationSuccess
        }
    }

  }
}

abstract class ProcessingTimeGroupWindow(alias: Option[Expression]) extends LogicalWindow(alias) {
  override def validate(tableEnv: TableEnvironment): ValidationResult = {
    val valid = super.validate(tableEnv)
    if (valid.isFailure) {
      return valid
    }

    tableEnv match {
      case b: BatchTableEnvironment => ValidationFailure(
        "Window on batch must declare a time attribute over which the query is evaluated.")
      case _ =>
        ValidationSuccess
    }
  }
}

// ------------------------------------------------------------------------------------------------
// Tumbling group windows
// ------------------------------------------------------------------------------------------------

object TumblingGroupWindow {
  def validate(tableEnv: TableEnvironment, size: Expression): ValidationResult = size match {
    case Literal(_, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
      ValidationSuccess
    case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS) =>
      ValidationSuccess
    case _ =>
      ValidationFailure("Tumbling window expects size literal of type Interval of Milliseconds " +
        "or Interval of Rows.")
  }
}

case class ProcessingTimeTumblingGroupWindow(
    override val alias: Option[Expression],
    size: Expression)
  extends ProcessingTimeGroupWindow(alias) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    ProcessingTimeTumblingGroupWindow(
      alias.map(resolve),
      resolve(size))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(TumblingGroupWindow.validate(tableEnv, size))

  override def toString: String = s"ProcessingTimeTumblingGroupWindow($alias, $size)"
}

case class EventTimeTumblingGroupWindow(
    override val alias: Option[Expression],
    timeField: Expression,
    size: Expression)
  extends EventTimeGroupWindow(
    alias,
    timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    EventTimeTumblingGroupWindow(
      alias.map(resolve),
      resolve(timeField),
      resolve(size))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv)
      .orElse(TumblingGroupWindow.validate(tableEnv, size))
      .orElse(size match {
        case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS)
          if tableEnv.isInstanceOf[StreamTableEnvironment] =>
          ValidationFailure(
            "Event-time grouping windows on row intervals in a stream environment " +
              "are currently not supported.")
        case _ =>
          ValidationSuccess
      })

  override def toString: String = s"EventTimeTumblingGroupWindow($alias, $timeField, $size)"
}

// ------------------------------------------------------------------------------------------------
// Sliding group windows
// ------------------------------------------------------------------------------------------------

object SlidingGroupWindow {
  def validate(
      tableEnv: TableEnvironment,
      size: Expression,
      slide: Expression)
    : ValidationResult = {

    val checkedSize = size match {
      case Literal(_, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
        ValidationSuccess
      case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS) =>
        ValidationSuccess
      case _ =>
        ValidationFailure("Sliding window expects size literal of type Interval of " +
          "Milliseconds or Interval of Rows.")
    }

    val checkedSlide = slide match {
      case Literal(_, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
        ValidationSuccess
      case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS) =>
        ValidationSuccess
      case _ =>
        ValidationFailure("Sliding window expects slide literal of type Interval of " +
          "Milliseconds or Interval of Rows.")
    }

    checkedSize
      .orElse(checkedSlide)
      .orElse {
        if (size.resultType != slide.resultType) {
          ValidationFailure("Sliding window expects same type of size and slide.")
        } else {
          ValidationSuccess
        }
      }
  }
}

case class ProcessingTimeSlidingGroupWindow(
    override val alias: Option[Expression],
    size: Expression,
    slide: Expression)
  extends ProcessingTimeGroupWindow(alias) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    ProcessingTimeSlidingGroupWindow(
      alias.map(resolve),
      resolve(size),
      resolve(slide))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SlidingGroupWindow.validate(tableEnv, size, slide))

  override def toString: String = s"ProcessingTimeSlidingGroupWindow($alias, $size, $slide)"
}

case class EventTimeSlidingGroupWindow(
    override val alias: Option[Expression],
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends EventTimeGroupWindow(alias, timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    EventTimeSlidingGroupWindow(
      alias.map(resolve),
      resolve(timeField),
      resolve(size),
      resolve(slide))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv)
      .orElse(SlidingGroupWindow.validate(tableEnv, size, slide))
      .orElse(size match {
        case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS)
          if tableEnv.isInstanceOf[StreamTableEnvironment] =>
          ValidationFailure(
            "Event-time grouping windows on row intervals in a stream environment " +
              "are currently not supported.")
        case _ =>
          ValidationSuccess
      })

  override def toString: String = s"EventTimeSlidingGroupWindow($alias, $timeField, $size, $slide)"
}

// ------------------------------------------------------------------------------------------------
// Session group windows
// ------------------------------------------------------------------------------------------------

object SessionGroupWindow {

  def validate(tableEnv: TableEnvironment, gap: Expression): ValidationResult = gap match {
    case Literal(timeInterval: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
      ValidationSuccess
    case _ =>
      ValidationFailure(
        "Session window expects gap literal of type Interval of Milliseconds.")
  }
}

case class ProcessingTimeSessionGroupWindow(
    override val alias: Option[Expression],
    gap: Expression)
  extends ProcessingTimeGroupWindow(alias) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    ProcessingTimeSessionGroupWindow(
      alias.map(resolve),
      resolve(gap))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SessionGroupWindow.validate(tableEnv, gap))

  override def toString: String = s"ProcessingTimeSessionGroupWindow($alias, $gap)"
}

case class EventTimeSessionGroupWindow(
    override val alias: Option[Expression],
    timeField: Expression,
    gap: Expression)
  extends EventTimeGroupWindow(
    alias,
    timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    EventTimeSessionGroupWindow(
      alias.map(resolve),
      resolve(timeField),
      resolve(gap))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SessionGroupWindow.validate(tableEnv, gap))

  override def toString: String = s"EventTimeSessionGroupWindow($alias, $timeField, $gap)"
}
