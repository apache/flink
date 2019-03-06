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

package org.apache.flink.table.api.java

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.ExpressionParser

/**
  * Helper class for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
  * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
  * elements in 5 minutes intervals.
  */
object Tumble {

  /**
    * Creates a tumbling window. Tumbling windows are consecutive, non-overlapping
    * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
    * elements in 5 minutes intervals.
    *
    * @param size the size of the window as time or row-count interval.
    * @return a partially defined tumbling window
    */
  def over(size: String): TumbleWithSize = new TumbleWithSize(size)
}

/**
  * Helper class for creating a sliding window. Sliding windows have a fixed size and slide by
  * a specified slide interval. If the slide interval is smaller than the window size, sliding
  * windows are overlapping. Thus, an element can be assigned to multiple windows.
  *
  * For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups elements
  * of 15 minutes and evaluates every five minutes. Each element is contained in three consecutive
  * window evaluations.
  */
object Slide {

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
}

/**
  * Helper class for creating a session window. The boundary of session windows are defined by
  * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
  * gap period.
  */
object Session {

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
}

/**
  * Helper class for creating an over window. Similar to SQL, over window aggregates compute an
  * aggregate for each input row over a range of its neighboring rows.
  */
object Over {

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
    new OverWindowPartitionedOrdered(Seq(), ExpressionParser.parseExpression(orderBy))
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
  def partitionBy(partitionBy: String): OverWindowPartitioned = {
    new OverWindowPartitioned(ExpressionParser.parseExpressionList(partitionBy))
  }
}
