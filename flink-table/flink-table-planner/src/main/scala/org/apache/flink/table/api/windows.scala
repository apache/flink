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

import org.apache.flink.table.api.java.{PartitionedOver => JPartitionedOver, OverWindowWithOrderBy => JOverWindowWithOrderBy}
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.table.api.scala.{CURRENT_RANGE, CURRENT_ROW, OverWindowWithOrderBy, PartitionedOver}

/**
  * Over window is similar to the traditional OVER SQL.
  */
case class OverWindow(
    private[flink] val alias: Expression,
    private[flink] val partitionBy: Seq[Expression],
    private[flink] val orderBy: Expression,
    private[flink] val preceding: Expression,
    private[flink] val following: Expression)

case class CurrentRow() extends Expression {
  override private[flink] def resultType = RowIntervalTypeInfo.INTERVAL_ROWS

  override private[flink] def children = Seq()

  override def toString = "CURRENT ROW"
}

case class CurrentRange() extends Expression {
  override private[flink] def resultType = TimeIntervalTypeInfo.INTERVAL_MILLIS

  override private[flink] def children = Seq()

  override def toString = "CURRENT RANGE"
}

case class UnboundedRow() extends Expression {
  override private[flink] def resultType = RowIntervalTypeInfo.INTERVAL_ROWS

  override private[flink] def children = Seq()

  override def toString = "UNBOUNDED ROW"
}

case class UnboundedRange() extends Expression {
  override private[flink] def resultType = TimeIntervalTypeInfo.INTERVAL_MILLIS

  override private[flink] def children = Seq()

  override def toString = "UNBOUNDED RANGE"
}

/**
  * A partially defined over window.
  */
class OverWindowWithPreceding(
    private val partitionBy: Seq[Expression],
    private val orderBy: Expression,
    private val preceding: Expression) {

  private[flink] var following: Expression = _

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
  def as(alias: Expression): OverWindow = {

    // set following to CURRENT_ROW / CURRENT_RANGE if not defined
    if (null == following) {
      if (preceding.resultType.isInstanceOf[RowIntervalTypeInfo]) {
        following = CURRENT_ROW
      } else {
        following = CURRENT_RANGE
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
  def following(following: Expression): OverWindowWithPreceding = {
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
  * For finite batch tables, group windows provide shortcuts for time-based groupBy.
  *
  * @deprecated Will be replaced by [[GroupWindow]]
  */
@Deprecated
@deprecated(
  "This class will be replaced by GroupWindow.", "1.8")
abstract class Window(val alias: Expression, val timeField: Expression) {

  /**
    * Converts an API class to a logical window for planning.
    */
  private[flink] def toLogicalWindow: LogicalWindow

}

/**
  * A group window specification.
  *
  * Group windows group rows based on time or row-count intervals and is therefore essentially a
  * special type of groupBy. Just like groupBy, group windows allow to compute aggregates
  * on groups of elements.
  *
  * Infinite streaming tables can only be grouped into time or row intervals. Hence window grouping
  * is required to apply aggregations on streaming tables.
  *
  * For finite batch tables, group windows provide shortcuts for time-based groupBy.
  */
abstract class GroupWindow(alias: Expression, timeField: Expression)
  extends Window(alias, timeField)

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
class TumbleWithSize(size: Expression) {

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
  def on(timeField: Expression): TumbleWithSizeOnTime =
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
class TumbleWithSizeOnTime(time: Expression, size: Expression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): TumbleWithSizeOnTimeWithAlias = {
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
    alias: Expression,
    timeField: Expression,
    size: Expression)
  extends GroupWindow(
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
  def every(slide: Expression): SlideWithSizeAndSlide = new SlideWithSizeAndSlide(size, slide)

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
class SlideWithSizeAndSlide(size: Expression, slide: Expression) {

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
  def on(timeField: Expression): SlideWithSizeAndSlideOnTime =
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
class SlideWithSizeAndSlideOnTime(timeField: Expression, size: Expression, slide: Expression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): SlideWithSizeAndSlideOnTimeWithAlias = {
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
    alias: Expression,
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends GroupWindow(
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
class SessionWithGap(gap: Expression) {

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
  def on(timeField: Expression): SessionWithGapOnTime =
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
class SessionWithGapOnTime(timeField: Expression, gap: Expression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): SessionWithGapOnTimeWithAlias = {
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
    alias: Expression,
    timeField: Expression,
    gap: Expression)
  extends GroupWindow(
    alias,
    timeField) {

  /**
    * Converts an API class to a logical window for planning.
    */
  override private[flink] def toLogicalWindow: LogicalWindow = {
    SessionGroupWindow(alias, timeField, gap)
  }
}

/**
  * Base class for Tumble Window Helper classes. This class contains help methods to create Tumble
  * Windows.
  */
class TumbleBase {

  /**
    * Creates a tumbling window. Tumbling windows are consecutive, non-overlapping
    * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
    * elements in 5 minutes intervals.
    *
    * @param size the size of the window as time or row-count interval.
    * @return a partially defined tumbling window
    */
  def over(size: String): TumbleWithSize = new TumbleWithSize(size)

  /**
    * Creates a tumbling window. Tumbling windows are fixed-size, consecutive, non-overlapping
    * windows. For example, a tumbling window of 5 minutes size groups
    * elements in 5 minutes intervals.
    *
    * @param size the size of the window as time or row-count interval.
    * @return a partially defined tumbling window
    */
  def over(size: Expression): TumbleWithSize = new TumbleWithSize(size)
}

/**
  * Base class for Slide Window Helper classes. This class contains help methods to create Slide
  * Windows.
  */
class SlideBase {

  /**
    * Creates a sliding window. Sliding windows have a fixed size and slide by
    * a specified slide interval. If the slide interval is smaller than the window size, sliding
    * windows are overlapping. Thus, an element can be assigned to multiple windows.
    *
    * For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups
    * elements of 15 minutes and evaluates every five minutes. Each element is contained in three
    * consecutive window evaluations.
    *
    * @param size the size of the window as time or row-count interval
    * @return a partially specified sliding window
    */
  def over(size: String): SlideWithSize = new SlideWithSize(size)

  /**
    * Creates a sliding window. Sliding windows have a fixed size and slide by
    * a specified slide interval. If the slide interval is smaller than the window size, sliding
    * windows are overlapping. Thus, an element can be assigned to multiple windows.
    *
    * For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups
    * elements of 15 minutes and evaluates every five minutes. Each element is contained in three
    * consecutive
    *
    * @param size the size of the window as time or row-count interval
    * @return a partially specified sliding window
    */
  def over(size: Expression): SlideWithSize = new SlideWithSize(size)
}

/**
  * Base class for Session Window Helper classes. This class contains help methods to create Session
  * Windows.
  */
class SessionBase {

  /**
    * Creates a session window. The boundary of session windows are defined by
    * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
    * gap period.
    *
    * @param gap specifies how long (as interval of milliseconds) to wait for new data before
    *            closing the session window.
    * @return a partially defined session window
    */
  def withGap(gap: String): SessionWithGap = new SessionWithGap(gap)

  /**
    * Creates a session window. The boundary of session windows are defined by
    * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
    * gap period.
    *
    * @param gap specifies how long (as interval of milliseconds) to wait for new data before
    *            closing the session window.
    * @return a partially defined session window
    */
  def withGap(gap: Expression): SessionWithGap = new SessionWithGap(gap)
}

/**
  * Base class for Over Window Helper classes. This class contains help methods to create Over
  * Windows.
  */
class OverBase {

  /**
    * Specifies the time attribute on which rows are ordered.
    *
    * For streaming tables, reference a rowtime or proctime time attribute here
    * to specify the time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param orderBy field reference
    * @return an over window with defined order
    */
  def orderBy(orderBy: String): JOverWindowWithOrderBy = {
    val orderByExpr = ExpressionParser.parseExpression(orderBy)
    new JOverWindowWithOrderBy(Array[Expression](), orderByExpr)
  }

  /**
    * Specifies the time attribute on which rows are ordered.
    *
    * For streaming tables, reference a rowtime or proctime time attribute here
    * to specify the time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param orderBy field reference
    * @return an over window with defined order
    */
  def orderBy(orderBy: Expression): OverWindowWithOrderBy = {
    OverWindowWithOrderBy(Seq[Expression](), orderBy)
  }

  /**
    * Partitions the elements on some partition keys.
    *
    * Each partition is individually sorted and aggregate functions are applied to each
    * partition separately.
    *
    * @param partitionBy list of field references
    * @return an over window with defined partitioning
    */
  def partitionBy(partitionBy: String): JPartitionedOver = {
    val partitionByExpr = ExpressionParser.parseExpressionList(partitionBy).toArray
    new JPartitionedOver(partitionByExpr)
  }

  /**
    * Partitions the elements on some partition keys.
    *
    * Each partition is individually sorted and aggregate functions are applied to each
    * partition separately.
    *
    * @param partitionBy list of field references
    * @return an over window with defined partitioning
    */
  def partitionBy(partitionBy: Expression*): PartitionedOver = {
    PartitionedOver(partitionBy.toArray)
  }
}

/**
  * Helper class for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
  * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
  * elements in 5 minutes intervals.
  */
object Tumble extends TumbleBase

/**
  * Helper class for creating a sliding window. Sliding windows have a fixed size and slide by
  * a specified slide interval. If the slide interval is smaller than the window size, sliding
  * windows are overlapping. Thus, an element can be assigned to multiple windows.
  *
  * For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups elements
  * of 15 minutes and evaluates every five minutes. Each element is contained in three consecutive
  * window evaluations.
  */
object Slide extends SlideBase

/**
  * Helper class for creating a session window. The boundary of session windows are defined by
  * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
  * gap period.
  */
object Session extends SessionBase

/**
  * Helper class for creating an over window. Similar to SQL, over window aggregates compute an
  * aggregate for each input row over a range of its neighboring rows.
  */
object Over extends OverBase
