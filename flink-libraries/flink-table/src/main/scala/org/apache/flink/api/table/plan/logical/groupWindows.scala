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

package org.apache.flink.api.table.plan.logical

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.{BatchTableEnvironment, StreamTableEnvironment, TableEnvironment}
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo, TypeCoercion}
import org.apache.flink.api.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

abstract class EventTimeGroupWindow(
    name: Option[Expression],
    time: Expression)
  extends LogicalWindow(name) {

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

abstract class ProcessingTimeGroupWindow(name: Option[Expression]) extends LogicalWindow(name)

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
    name: Option[Expression],
    size: Expression)
  extends ProcessingTimeGroupWindow(name) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    ProcessingTimeTumblingGroupWindow(
      name.map(resolve),
      resolve(size))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(TumblingGroupWindow.validate(tableEnv, size))

  override def toString: String = s"ProcessingTimeTumblingGroupWindow($name, $size)"
}

case class EventTimeTumblingGroupWindow(
    name: Option[Expression],
    timeField: Expression,
    size: Expression)
  extends EventTimeGroupWindow(
    name,
    timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    EventTimeTumblingGroupWindow(
      name.map(resolve),
      resolve(timeField),
      resolve(size))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv)
      .orElse(TumblingGroupWindow.validate(tableEnv, size))
      .orElse(size match {
        case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS) =>
          ValidationFailure(
            "Event-time grouping windows on row intervals are currently not supported.")
        case _ =>
          ValidationSuccess
      })

  override def toString: String = s"EventTimeTumblingGroupWindow($name, $timeField, $size)"
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
    name: Option[Expression],
    size: Expression,
    slide: Expression)
  extends ProcessingTimeGroupWindow(name) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    ProcessingTimeSlidingGroupWindow(
      name.map(resolve),
      resolve(size),
      resolve(slide))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SlidingGroupWindow.validate(tableEnv, size, slide))

  override def toString: String = s"ProcessingTimeSlidingGroupWindow($name, $size, $slide)"
}

case class EventTimeSlidingGroupWindow(
    name: Option[Expression],
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends EventTimeGroupWindow(name, timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    EventTimeSlidingGroupWindow(
      name.map(resolve),
      resolve(timeField),
      resolve(size),
      resolve(slide))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv)
      .orElse(SlidingGroupWindow.validate(tableEnv, size, slide))
      .orElse(size match {
        case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS) =>
          ValidationFailure(
            "Event-time grouping windows on row intervals are currently not supported.")
        case _ =>
          ValidationSuccess
      })

  override def toString: String = s"EventTimeSlidingGroupWindow($name, $timeField, $size, $slide)"
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
    name: Option[Expression],
    gap: Expression)
  extends ProcessingTimeGroupWindow(name) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    ProcessingTimeSessionGroupWindow(
      name.map(resolve),
      resolve(gap))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SessionGroupWindow.validate(tableEnv, gap))

  override def toString: String = s"ProcessingTimeSessionGroupWindow($name, $gap)"
}

case class EventTimeSessionGroupWindow(
    name: Option[Expression],
    timeField: Expression,
    gap: Expression)
  extends EventTimeGroupWindow(
    name,
    timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    EventTimeSessionGroupWindow(
      name.map(resolve),
      resolve(timeField),
      resolve(gap))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SessionGroupWindow.validate(tableEnv, gap))

  override def toString: String = s"EventTimeSessionGroupWindow($name, $timeField, $gap)"
}
