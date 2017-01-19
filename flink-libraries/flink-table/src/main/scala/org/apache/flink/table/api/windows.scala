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

package org.apache.flink.table.api

import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.plan.logical._

/**
  * A window specification.
  *
  * Window groups rows based on time or row-count intervals. It is a general way to group the
  * elements, which is very helpful for both groupby-aggregations and over-aggregations to
  * compute aggregates on groups of elements.
  *
  * Infinite streaming tables can only be grouped into time or row intervals. Hence window grouping
  * is required to apply aggregations on streaming tables.
  *
  * For finite batch tables, window provides shortcuts for time-based groupBy.
  *
  */
abstract class Window {

  // The expression of alias for this Window
  private[flink] var alias: Option[Expression] = None
  /**
    * Converts an API class to a logical window for planning.
    */
  private[flink] def toLogicalWindow: LogicalWindow

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): Window = {
    this.alias = Some(alias)
    this
  }

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): Window = as(ExpressionParser.parseExpression(alias))
}

/**
  * A window operating on event-time.
  *
  * @param timeField defines the time mode for streaming tables. For batch table it defines the
  *                  time attribute on which is grouped.
  */
abstract class EventTimeWindow(val timeField: Expression) extends Window

// ------------------------------------------------------------------------------------------------
// Tumbling windows
// ------------------------------------------------------------------------------------------------

/**
  * Tumbling window.
  *
  * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows are
  * grouped by processing-time.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class TumblingWindow(size: Expression) extends Window {

  /**
    * Tumbling window.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * @param size the size of the window either as time or row-count interval.
    */
  def this(size: String) = this(ExpressionParser.parseExpression(size))

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: Expression): TumblingEventTimeWindow =
    new TumblingEventTimeWindow(timeField, size)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: String): TumblingEventTimeWindow =
    on(ExpressionParser.parseExpression(timeField))

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeTumblingGroupWindow(alias, size)
}

/**
  * Tumbling window on event-time.
  */
class TumblingEventTimeWindow(
    time: Expression,
    size: Expression)
  extends EventTimeWindow(time) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeTumblingGroupWindow(alias, time, size)
}

// ------------------------------------------------------------------------------------------------
// Sliding windows
// ------------------------------------------------------------------------------------------------

/**
  * Partially specified sliding window.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class SlideWithSize(size: Expression) {

  /**
    * Partially specified sliding window.
    *
    * @param size the size of the window either as time or row-count interval.
    */
  def this(size: String) = this(ExpressionParser.parseExpression(size))

  /**
    * Specifies the window's slide as time or row-count interval.
    *
    * The slide determines the interval in which windows are started. Hence, sliding windows can
    * overlap if the slide is smaller than the size of the window.
    *
    * For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
    * 15 minutes worth of elements are grouped every 3 minutes and each row contributes to 5
    * windows.
    *
    * @param slide the slide of the window either as time or row-count interval.
    * @return a sliding window
    */
  def every(slide: Expression): SlidingWindow = new SlidingWindow(size, slide)

  /**
    * Specifies the window's slide as time or row-count interval.
    *
    * The slide determines the interval in which windows are started. Hence, sliding windows can
    * overlap if the slide is smaller than the size of the window.
    *
    * For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
    * 15 minutes worth of elements are grouped every 3 minutes and each row contributes to 5
    * windows.
    *
    * @param slide the slide of the window either as time or row-count interval.
    * @return a sliding window
    */
  def every(slide: String): SlidingWindow = every(ExpressionParser.parseExpression(slide))
}

/**
  * Sliding window.
  *
  * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows are
  * grouped by processing-time.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class SlidingWindow(
    size: Expression,
    slide: Expression)
  extends Window {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a sliding window on event-time
    */
  def on(timeField: Expression): SlidingEventTimeWindow =
    new SlidingEventTimeWindow(timeField, size, slide)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a sliding window on event-time
    */
  def on(timeField: String): SlidingEventTimeWindow =
    on(ExpressionParser.parseExpression(timeField))

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeSlidingGroupWindow(alias, size, slide)
}

/**
  * Sliding window on event-time.
  */
class SlidingEventTimeWindow(
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends EventTimeWindow(timeField) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeSlidingGroupWindow(alias, timeField, size, slide)
}

// ------------------------------------------------------------------------------------------------
// Session windows
// ------------------------------------------------------------------------------------------------

/**
  * Session window.
  *
  * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows are
  * grouped by processing-time.
  *
  * @param gap the time interval of inactivity before a window is closed.
  */
class SessionWindow(gap: Expression) extends Window {

  /**
    * Session window.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * @param gap the time interval of inactivity before a window is closed.
    */
  def this(gap: String) = this(ExpressionParser.parseExpression(gap))

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a session window on event-time
    */
  def on(timeField: Expression): SessionEventTimeWindow =
    new SessionEventTimeWindow(timeField, gap)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a session window on event-time
    */
  def on(timeField: String): SessionEventTimeWindow =
    on(ExpressionParser.parseExpression(timeField))

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeSessionGroupWindow(alias, gap)
}

/**
  * Session window on event-time.
  */
class SessionEventTimeWindow(
    timeField: Expression,
    gap: Expression)
  extends EventTimeWindow(timeField) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeSessionGroupWindow(alias, timeField, gap)
}
