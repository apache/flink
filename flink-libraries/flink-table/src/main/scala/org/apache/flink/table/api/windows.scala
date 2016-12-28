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
  * A group-window specification.
  *
  * Group-windows group rows based on time or row-count intervals and is therefore essentially a
  * special type of groupBy. Just like groupBy, group-windows allow to compute aggregates
  * on groups of elements.
  *
  * Infinite streaming tables can only be grouped into time or row intervals. Hence window grouping
  * is required to apply aggregations on streaming tables.
  *
  * For finite batch tables, group-windows provide shortcuts for time-based groupBy.
  *
  */
trait GroupWindow {

  /**
    * Converts an API class to a logical window for planning.
    */
  private[flink] def toLogicalWindow: LogicalWindow
}

/**
  * A group-window operating on event-time.
  *
  * @param timeField defines the time mode for streaming tables. For batch table it defines the
  *                  time attribute on which is grouped.
  */
abstract class EventTimeWindow(val timeField: Expression) extends GroupWindow {

  protected var name: Option[Expression] = None

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): EventTimeWindow = {
    this.name = Some(alias)
    this
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): EventTimeWindow = as(ExpressionParser.parseExpression(alias))
}

// ------------------------------------------------------------------------------------------------
// Tumbling group-windows
// ------------------------------------------------------------------------------------------------

/**
  * Tumbling group-window.
  *
  * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows are
  * grouped by processing-time.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class TumblingWindow(size: Expression) extends GroupWindow {

  /**
    * Tumbling group-window.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * @param size the size of the window either as time or row-count interval.
    */
  def this(size: String) = this(ExpressionParser.parseExpression(size))

  private var alias: Option[Expression] = None

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a tumbling group-window on event-time
    */
  def on(timeField: Expression): TumblingEventTimeWindow =
    new TumblingEventTimeWindow(alias, timeField, size)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a tumbling group-window on event-time
    */
  def on(timeField: String): TumblingEventTimeWindow =
    on(ExpressionParser.parseExpression(timeField))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): TumblingWindow = {
    this.alias = Some(alias)
    this
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): TumblingWindow = as(ExpressionParser.parseExpression(alias))

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeTumblingGroupWindow(alias, size)
}

/**
  * Tumbling group-window on event-time.
  */
class TumblingEventTimeWindow(
    alias: Option[Expression],
    time: Expression,
    size: Expression)
  extends EventTimeWindow(time) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeTumblingGroupWindow(name.orElse(alias), time, size)
}

// ------------------------------------------------------------------------------------------------
// Sliding group windows
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
    * @return a sliding group-window
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
    * @return a sliding group-window
    */
  def every(slide: String): SlidingWindow = every(ExpressionParser.parseExpression(slide))
}

/**
  * Sliding group-window.
  *
  * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows are
  * grouped by processing-time.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class SlidingWindow(
    size: Expression,
    slide: Expression)
  extends GroupWindow {

  private var alias: Option[Expression] = None

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a sliding group-window on event-time
    */
  def on(timeField: Expression): SlidingEventTimeWindow =
    new SlidingEventTimeWindow(alias, timeField, size, slide)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a sliding group-window on event-time
    */
  def on(timeField: String): SlidingEventTimeWindow =
    on(ExpressionParser.parseExpression(timeField))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): SlidingWindow = {
    this.alias = Some(alias)
    this
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): SlidingWindow = as(ExpressionParser.parseExpression(alias))

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeSlidingGroupWindow(alias, size, slide)
}

/**
  * Sliding group-window on event-time.
  */
class SlidingEventTimeWindow(
    alias: Option[Expression],
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends EventTimeWindow(timeField) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeSlidingGroupWindow(name.orElse(alias), timeField, size, slide)
}

// ------------------------------------------------------------------------------------------------
// Session group windows
// ------------------------------------------------------------------------------------------------

/**
  * Session group-window.
  *
  * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows are
  * grouped by processing-time.
  *
  * @param gap the time interval of inactivity before a window is closed.
  */
class SessionWindow(gap: Expression) extends GroupWindow {

  /**
    * Session group-window.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * @param gap the time interval of inactivity before a window is closed.
    */
  def this(gap: String) = this(ExpressionParser.parseExpression(gap))

  private var alias: Option[Expression] = None

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a session group-window on event-time
    */
  def on(timeField: Expression): SessionEventTimeWindow =
    new SessionEventTimeWindow(alias, timeField, gap)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a session group-window on event-time
    */
  def on(timeField: String): SessionEventTimeWindow =
    on(ExpressionParser.parseExpression(timeField))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): SessionWindow = {
    this.alias = Some(alias)
    this
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to in order
    * to access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): SessionWindow = as(ExpressionParser.parseExpression(alias))

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeSessionGroupWindow(alias, gap)
}

/**
  * Session group-window on event-time.
  */
class SessionEventTimeWindow(
    alias: Option[Expression],
    timeField: Expression,
    gap: Expression)
  extends EventTimeWindow(timeField) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeSessionGroupWindow(name.orElse(alias), timeField, gap)
}
