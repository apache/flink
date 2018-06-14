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

import org.apache.flink.table.api.{BatchTableEnvironment, StreamTableEnvironment, TableEnvironment}
import org.apache.flink.table.expressions.ExpressionUtils.{isRowCountLiteral, isRowtimeAttribute, isTimeAttribute, isTimeIntervalLiteral}
import org.apache.flink.table.expressions._
import org.apache.flink.table.typeutils.TypeCheckUtils.{isTimePoint, isLong}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

// ------------------------------------------------------------------------------------------------
// Tumbling group windows
// ------------------------------------------------------------------------------------------------

case class TumblingGroupWindow(
    alias: Expression,
    timeField: Expression,
    size: Expression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    TumblingGroupWindow(
      resolve(alias),
      resolve(timeField),
      resolve(size))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(
      tableEnv match {

        // check size
        case _ if !isTimeIntervalLiteral(size) && !isRowCountLiteral(size) =>
          ValidationFailure(
            "Tumbling window expects size literal of type Interval of Milliseconds " +
              "or Interval of Rows.")

        // check time attribute
        case _: StreamTableEnvironment if !isTimeAttribute(timeField) =>
          ValidationFailure(
            "Tumbling window expects a time attribute for grouping in a stream environment.")
        case _: BatchTableEnvironment
          if !(isTimePoint(timeField.resultType) || isLong(timeField.resultType)) =>
          ValidationFailure(
            "Tumbling window expects a time attribute for grouping in a batch environment.")

        // check row intervals on event-time
        case _: StreamTableEnvironment
            if isRowCountLiteral(size) && isRowtimeAttribute(timeField) =>
          ValidationFailure(
            "Event-time grouping windows on row intervals in a stream environment " +
              "are currently not supported.")

        case _ =>
          ValidationSuccess
      }
    )

  override def toString: String = s"TumblingGroupWindow($alias, $timeField, $size)"
}

// ------------------------------------------------------------------------------------------------
// Sliding group windows
// ------------------------------------------------------------------------------------------------

case class SlidingGroupWindow(
    alias: Expression,
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    SlidingGroupWindow(
      resolve(alias),
      resolve(timeField),
      resolve(size),
      resolve(slide))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(
      tableEnv match {

        // check size
        case _ if !isTimeIntervalLiteral(size) && !isRowCountLiteral(size) =>
          ValidationFailure(
            "Sliding window expects size literal of type Interval of Milliseconds " +
              "or Interval of Rows.")

        // check slide
        case _ if !isTimeIntervalLiteral(slide) && !isRowCountLiteral(slide) =>
          ValidationFailure(
            "Sliding window expects slide literal of type Interval of Milliseconds " +
              "or Interval of Rows.")

        // check same type of intervals
        case _ if isTimeIntervalLiteral(size) != isTimeIntervalLiteral(slide) =>
          ValidationFailure("Sliding window expects same type of size and slide.")

        // check time attribute
        case _: StreamTableEnvironment if !isTimeAttribute(timeField) =>
          ValidationFailure(
            "Sliding window expects a time attribute for grouping in a stream environment.")
        case _: BatchTableEnvironment
          if !(isTimePoint(timeField.resultType) || isLong(timeField.resultType)) =>
          ValidationFailure(
            "Sliding window expects a time attribute for grouping in a stream environment.")

        // check row intervals on event-time
        case _: StreamTableEnvironment
            if isRowCountLiteral(size) && isRowtimeAttribute(timeField) =>
          ValidationFailure(
            "Event-time grouping windows on row intervals in a stream environment " +
              "are currently not supported.")

        case _ =>
          ValidationSuccess
      }
    )

  override def toString: String = s"SlidingGroupWindow($alias, $timeField, $size, $slide)"
}

// ------------------------------------------------------------------------------------------------
// Session group windows
// ------------------------------------------------------------------------------------------------

case class SessionGroupWindow(
    alias: Expression,
    timeField: Expression,
    gap: Expression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def resolveExpressions(resolve: (Expression) => Expression): LogicalWindow =
    SessionGroupWindow(
      resolve(alias),
      resolve(timeField),
      resolve(gap))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(
      tableEnv match {

        // check size
        case _ if !isTimeIntervalLiteral(gap) =>
          ValidationFailure(
            "Session window expects size literal of type Interval of Milliseconds.")

        // check time attribute
        case _: StreamTableEnvironment if !isTimeAttribute(timeField) =>
          ValidationFailure(
            "Session window expects a time attribute for grouping in a stream environment.")
        case _: BatchTableEnvironment
          if !(isTimePoint(timeField.resultType) || isLong(timeField.resultType)) =>
          ValidationFailure(
            "Session window expects a time attribute for grouping in a stream environment.")

        case _ =>
          ValidationSuccess
      }
    )

  override def toString: String = s"SessionGroupWindow($alias, $timeField, $gap)"
}
