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

import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}

trait UnresolvedOverWindow

abstract class Window

/**
  * Over window is similar to the traditional OVER SQL.
  */
case class OverWindow(
    private[flink] val alias: PlannerExpression,
    private[flink] val partitionBy: Seq[PlannerExpression],
    private[flink] val orderBy: PlannerExpression,
    private[flink] val preceding: PlannerExpression,
    private[flink] val following: PlannerExpression) extends UnresolvedOverWindow

case class CurrentRow() extends PlannerExpression {
  override private[flink] def resultType = RowIntervalTypeInfo.INTERVAL_ROWS

  override private[flink] def children = Seq()

  override def toString = "CURRENT ROW"
}

case class CurrentRange() extends PlannerExpression {
  override private[flink] def resultType = TimeIntervalTypeInfo.INTERVAL_MILLIS

  override private[flink] def children = Seq()

  override def toString = "CURRENT RANGE"
}

case class UnboundedRow() extends PlannerExpression {
  override private[flink] def resultType = RowIntervalTypeInfo.INTERVAL_ROWS

  override private[flink] def children = Seq()

  override def toString = "UNBOUNDED ROW"
}

case class UnboundedRange() extends PlannerExpression {
  override private[flink] def resultType = TimeIntervalTypeInfo.INTERVAL_MILLIS

  override private[flink] def children = Seq()

  override def toString = "UNBOUNDED RANGE"
}

case class PartitionedOver(partitionBy: Array[PlannerExpression]) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[orderBy 'rowtime or orderBy 'proctime]] to specify time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    */
  def orderBy(orderBy: PlannerExpression): OverWindowWithOrderBy = {
    OverWindowWithOrderBy(partitionBy, orderBy)
  }
}

case class OverWindowWithOrderBy(partitionBy: Seq[PlannerExpression], orderBy: PlannerExpression) {

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return this over window
    */
  def preceding(preceding: PlannerExpression): OverWindowWithPreceding = {
    new OverWindowWithPreceding(partitionBy, orderBy, preceding)
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: String): OverWindow = as(ExpressionParser.parseExpression(alias))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: PlannerExpression): OverWindow = {
    OverWindow(alias, partitionBy, orderBy, UnboundedRange(), CurrentRange())
  }
}

/**
  * A partially defined over window.
  */
class OverWindowWithPreceding(
    private val partitionBy: Seq[PlannerExpression],
    private val orderBy: PlannerExpression,
    private val preceding: PlannerExpression) {

  private[flink] var following: PlannerExpression = _

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: String): OverWindow = as(ExpressionParser.parseExpression(alias))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: PlannerExpression): OverWindow = {

    // set following to CURRENT_ROW / CURRENT_RANGE if not defined
    if (null == following) {
      if (preceding.resultType.isInstanceOf[RowIntervalTypeInfo]) {
        following = CurrentRow()
      } else {
        following = CurrentRange()
      }
    }
    OverWindow(alias, partitionBy, orderBy, preceding, following)
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return this over window
    */
  def following(following: String): OverWindowWithPreceding = {
    this.following(ExpressionParser.parseExpression(following))
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return this over window
    */
  def following(following: PlannerExpression): OverWindowWithPreceding = {
    this.following = following
    this
  }
}

/**
  * A window specification.
  *
  * Window groups rows based on time or row-count intervals. It is a general way to group the
  * elements, which is very helpful for both groupBy-aggregations and over-aggregations to
  * compute aggregates on groups of elements.
  *
  * Infinite streaming tables can only be grouped into time or row intervals. Hence window grouping
  * is required to apply aggregations on streaming tables.
  *
  * For finite batch tables, window provides shortcuts for time-based groupBy.
  *
  */
abstract class PlannerWindow(val alias: PlannerExpression, val timeField: PlannerExpression) {

  /**
    * Converts an API class to a logical window for planning.
    */
  private[flink] def toLogicalWindow: LogicalWindow

}

// ------------------------------------------------------------------------------------------------
// Tumbling windows
// ------------------------------------------------------------------------------------------------

/**
  * Tumbling window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class TumbleWithSize(size: PlannerExpression) {

  /**
    * Tumbling window.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param size the size of the window either as time or row-count interval.
    */
  def this(size: String) = this(ExpressionParser.parseExpression(size))

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: PlannerExpression): TumbleWithSizeOnTime =
    new TumbleWithSizeOnTime(timeField, size)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: String): TumbleWithSizeOnTime =
    on(ExpressionParser.parseExpression(timeField))
}

/**
  * Tumbling window on time.
  */
class TumbleWithSizeOnTime(time: PlannerExpression, size: PlannerExpression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: PlannerExpression): TumbleWithSizeOnTimeWithAlias = {
    new TumbleWithSizeOnTimeWithAlias(alias, time, size)
  }

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): TumbleWithSizeOnTimeWithAlias = {
    as(ExpressionParser.parseExpression(alias))
  }
}

/**
  * Tumbling window on time with alias. Fully specifies a window.
  */
class TumbleWithSizeOnTimeWithAlias(
    alias: PlannerExpression,
    timeField: PlannerExpression,
    size: PlannerExpression)
  extends PlannerWindow(
    alias,
    timeField) {

  /**
    * Converts an API class to a logical window for planning.
    */
  override private[flink] def toLogicalWindow: LogicalWindow = {
    TumblingGroupWindow(alias, timeField, size)
  }
}

// ------------------------------------------------------------------------------------------------
// Sliding windows
// ------------------------------------------------------------------------------------------------

/**
  * Partially specified sliding window.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class SlideWithSize(size: PlannerExpression) {

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
  def every(slide: PlannerExpression): SlideWithSizeAndSlide =
    new SlideWithSizeAndSlide(size, slide)

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
  def every(slide: String): SlideWithSizeAndSlide = every(ExpressionParser.parseExpression(slide))
}

/**
  * Sliding window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class SlideWithSizeAndSlide(size: PlannerExpression, slide: PlannerExpression) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: PlannerExpression): SlideWithSizeAndSlideOnTime =
    new SlideWithSizeAndSlideOnTime(timeField, size, slide)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: String): SlideWithSizeAndSlideOnTime =
    on(ExpressionParser.parseExpression(timeField))
}

/**
  * Sliding window on time.
  */
class SlideWithSizeAndSlideOnTime(
    timeField: PlannerExpression,
    size: PlannerExpression,
    slide: PlannerExpression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: PlannerExpression): SlideWithSizeAndSlideOnTimeWithAlias = {
    new SlideWithSizeAndSlideOnTimeWithAlias(alias, timeField, size, slide)
  }

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): SlideWithSizeAndSlideOnTimeWithAlias = {
    as(ExpressionParser.parseExpression(alias))
  }
}

/**
  * Sliding window on time with alias. Fully specifies a window.
  */
class SlideWithSizeAndSlideOnTimeWithAlias(
    alias: PlannerExpression,
    timeField: PlannerExpression,
    size: PlannerExpression,
    slide: PlannerExpression)
  extends PlannerWindow(
    alias,
    timeField) {

  /**
    * Converts an API class to a logical window for planning.
    */
  override private[flink] def toLogicalWindow: LogicalWindow = {
    SlidingGroupWindow(alias, timeField, size, slide)
  }
}

// ------------------------------------------------------------------------------------------------
// Session windows
// ------------------------------------------------------------------------------------------------

/**
  * Session window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param gap the time interval of inactivity before a window is closed.
  */
class SessionWithGap(gap: PlannerExpression) {

  /**
    * Session window.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param gap the time interval of inactivity before a window is closed.
    */
  def this(gap: String) = this(ExpressionParser.parseExpression(gap))

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: PlannerExpression): SessionWithGapOnTime =
    new SessionWithGapOnTime(timeField, gap)

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: String): SessionWithGapOnTime =
    on(ExpressionParser.parseExpression(timeField))
}

/**
  * Session window on time.
  */
class SessionWithGapOnTime(timeField: PlannerExpression, gap: PlannerExpression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: PlannerExpression): SessionWithGapOnTimeWithAlias = {
    new SessionWithGapOnTimeWithAlias(alias, timeField, gap)
  }

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): SessionWithGapOnTimeWithAlias = {
    as(ExpressionParser.parseExpression(alias))
  }
}

/**
  * Session window on time with alias. Fully specifies a window.
  */
class SessionWithGapOnTimeWithAlias(
    alias: PlannerExpression,
    timeField: PlannerExpression,
    gap: PlannerExpression)
  extends PlannerWindow(
    alias,
    timeField) {

  /**
    * Converts an API class to a logical window for planning.
    */
  override private[flink] def toLogicalWindow: LogicalWindow = {
    SessionGroupWindow(alias, timeField, gap)
  }
}
