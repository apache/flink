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

import org.apache.flink.table.api.scala.{CURRENT_RANGE, UNBOUNDED_RANGE}
import org.apache.flink.table.api.{OverWindow, OverWindowWithPreceding}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.api._

/**
  * Helper class for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
  * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
  * elements in 5 minutes intervals.
  *
  * @deprecated This class will be replaced by [[org.apache.flink.table.api.Tumble]].
  */
@Deprecated
object Tumble extends TumbleBase

/**
  * Helper class for creating a sliding window. Sliding windows have a fixed size and slide by
  * a specified slide interval. If the slide interval is smaller than the window size, sliding
  * windows are overlapping. Thus, an element can be assigned to multiple windows.
  *
  * For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups elements
  * of 15 minutes and evaluates every five minutes. Each element is contained in three consecutive
  * window evaluations.
  *
  * @deprecated This class will be replaced by [[org.apache.flink.table.api.Slide]].
  */
@Deprecated
object Slide extends SlideBase

/**
  * Helper class for creating a session window. The boundary of session windows are defined by
  * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
  * gap period.
  *
  * @deprecated This class will be replaced by [[org.apache.flink.table.api.Session]].
  */
@Deprecated
object Session extends SessionBase

/**
  * Helper class for creating an over window. Similar to SQL, over window aggregates compute an
  * aggregate for each input row over a range of its neighboring rows.
  *
  * @deprecated This class will be replaced by [[org.apache.flink.table.api.Over]].
  */
@Deprecated
object Over extends OverBase

class PartitionedOver(private val partitionByExpr: Array[Expression]) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[orderBy 'rowtime or orderBy 'proctime]] to specify time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    */
  def orderBy(orderBy: String): OverWindowWithOrderBy = {
    val orderByExpr = ExpressionParser.parseExpression(orderBy)
    new OverWindowWithOrderBy(partitionByExpr, orderByExpr)
  }
}


class OverWindowWithOrderBy(
  private val partitionByExpr: Array[Expression],
  private val orderByExpr: Expression) {

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return this over window
    */
  def preceding(preceding: String): OverWindowWithPreceding = {
    val precedingExpr = ExpressionParser.parseExpression(preceding)
    new OverWindowWithPreceding(partitionByExpr, orderByExpr, precedingExpr)
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
  def as(alias: Expression): OverWindow = {
    OverWindow(alias, partitionByExpr, orderByExpr, UNBOUNDED_RANGE, CURRENT_RANGE)
  }
}
