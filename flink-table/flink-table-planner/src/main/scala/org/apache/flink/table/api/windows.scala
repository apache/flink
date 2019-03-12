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

/**
  * An over window specification.
  *
  * Similar to SQL, over window aggregates compute an aggregate for each input row over a range
  * of its neighboring rows.
  */
class OverWindow(
    alias: Expression,
    partitionBy: Seq[Expression],
    orderBy: Expression,
    preceding: Expression,
    following: Option[Expression]) {

  def getAlias: Expression = alias

  def getPartitioning: Seq[Expression] = partitionBy

  def getOrder: Expression = orderBy

  def getPreceding: Expression = preceding

  def getFollowing: Option[Expression] = following
}

// ------------------------------------------------------------------------------------------------
// Over windows
// ------------------------------------------------------------------------------------------------

/**
  * Partially defined over window with partitioning.
  */
class OverWindowPartitioned(partitionBy: Seq[Expression]) {

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
  def orderBy(orderBy: String): OverWindowPartitionedOrdered = {
    this.orderBy(ExpressionParser.parseExpression(orderBy))
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
  def orderBy(orderBy: Expression): OverWindowPartitionedOrdered = {
    new OverWindowPartitionedOrdered(partitionBy, orderBy)
  }
}

/**
  * Partially defined over window with (optional) partitioning and order.
  */
class OverWindowPartitionedOrdered(partitionBy: Seq[Expression], orderBy: Expression) {

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return an over window with defined preceding
    */
  def preceding(preceding: String): OverWindowPartitionedOrderedPreceding = {
    this.preceding(ExpressionParser.parseExpression(preceding))
  }

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return an over window with defined preceding
    */
  def preceding(preceding: Expression): OverWindowPartitionedOrderedPreceding = {
    new OverWindowPartitionedOrderedPreceding(partitionBy, orderBy, preceding)
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return the fully defined over window
    */
  def as(alias: String): OverWindow = as(ExpressionParser.parseExpression(alias))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return the fully defined over window
    */
  def as(alias: Expression): OverWindow = {
    new OverWindow(alias, partitionBy, orderBy, UnboundedRange(), None)
  }
}

/**
  * Partially defined over window with (optional) partitioning, order, and preceding.
  */
class OverWindowPartitionedOrderedPreceding(
    private val partitionBy: Seq[Expression],
    private val orderBy: Expression,
    private val preceding: Expression) {

  private var optionalFollowing: Option[Expression] = None

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return the fully defined over window
    */
  def as(alias: String): OverWindow = as(ExpressionParser.parseExpression(alias))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return the fully defined over window
    */
  def as(alias: Expression): OverWindow = {
    new OverWindow(alias, partitionBy, orderBy, preceding, optionalFollowing)
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return an over window with defined following
    */
  def following(following: String): OverWindowPartitionedOrderedPreceding = {
    this.following(ExpressionParser.parseExpression(following))
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return an over window with defined following
    */
  def following(following: Expression): OverWindowPartitionedOrderedPreceding = {
    optionalFollowing = Some(following)
    this
  }
}

// ------------------------------------------------------------------------------------------------
// Group windows
// ------------------------------------------------------------------------------------------------

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
abstract class Window(alias: Expression, timeField: Expression) {

  def getAlias: Expression = {
    alias
  }

  def getTimeField: Expression = {
    timeField
  }
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
  extends Window(
    alias,
    timeField) {

  def getSize: Expression = {
    size
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
  extends Window(
    alias,
    timeField) {

  def getSize: Expression = {
    size
  }

  def getSlide: Expression = {
    slide
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
  extends Window(
    alias,
    timeField) {

  def getGap: Expression = {
    gap
  }
}
