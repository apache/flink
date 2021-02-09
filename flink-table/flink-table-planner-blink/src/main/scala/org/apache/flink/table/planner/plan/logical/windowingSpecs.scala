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

package org.apache.flink.table.planner.plan.logical

import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.util.TimeUtils.formatWithHighestUnit

import java.time.Duration
import java.util.Objects

/**
 * Logical representation of a windowing strategy.
 */
sealed trait WindowingStrategy {
  val window: WindowSpec
  val timeAttributeType: LogicalType
  val isRowtime: Boolean = LogicalTypeChecks.isRowtimeAttribute(timeAttributeType)
  def toSummaryString(inputFieldNames: Array[String]): String
}

/**
 * A windowing strategy that gets windows by calculating on time attribute column.
 */
case class TimeAttributeWindowingStrategy(
    timeAttribute: Int,
    timeAttributeType: LogicalType,
    window: WindowSpec)
    extends WindowingStrategy {
  override def toSummaryString(inputFieldNames: Array[String]): String = {
    val windowing = s"time_col=[${inputFieldNames(timeAttribute)}]"
    window.toSummaryString(windowing)
  }
}

/**
 * A windowing strategy that gets windows from input columns as windows have been assigned and
 * attached to the physical columns.
 */
case class WindowAttachedWindowingStrategy(
    windowStart: Int,
    windowEnd: Int,
    timeAttributeType: LogicalType,
    window: WindowSpec)
  extends WindowingStrategy {
  override def toSummaryString(inputFieldNames: Array[String]): String = {
    val windowing = s"win_start=[${inputFieldNames(windowStart)}], " +
      s"win_end=[${inputFieldNames(windowEnd)}]"
    window.toSummaryString(windowing)
  }
}

// ------------------------------------------------------------------------------------------------
// Window specifications
// ------------------------------------------------------------------------------------------------

/**
 * Logical representation of a window specification.
 */
sealed trait WindowSpec {

  def toSummaryString(windowing: String): String

  def hashCode(): Int

  def equals(obj: Any): Boolean
}

case class TumblingWindowSpec(size: Duration) extends WindowSpec {
  override def toSummaryString(windowing: String): String = {
    s"TUMBLE($windowing, size=[${formatWithHighestUnit(size)}])"
  }

  override def hashCode(): Int = Objects.hash(classOf[TumblingWindowSpec], size)

  override def equals(obj: Any): Boolean = obj match {
    case TumblingWindowSpec(size) => size.equals(this.size)
    case _ => false
  }
}

case class HoppingWindowSpec(size: Duration, slide: Duration) extends WindowSpec {
  override def toSummaryString(windowing: String): String = {
    s"HOP($windowing, size=[${formatWithHighestUnit(size)}], " +
      s"slide=[${formatWithHighestUnit(slide)}])"
  }

  override def hashCode(): Int = Objects.hash(classOf[HoppingWindowSpec], size, slide)

  override def equals(obj: Any): Boolean = obj match {
    case HoppingWindowSpec(size, slide) => size.equals(this.size) && slide.equals(this.slide)
    case _ => false
  }
}

case class CumulativeWindowSpec(maxSize: Duration, step: Duration) extends WindowSpec {
  override def toSummaryString(windowing: String): String = {
    s"CUMULATE($windowing, max_size=[${formatWithHighestUnit(maxSize)}], " +
      s"step=[${formatWithHighestUnit(step)}])"
  }

  override def hashCode(): Int = Objects.hash(classOf[CumulativeWindowSpec], maxSize, step)

  override def equals(obj: Any): Boolean = obj match {
    case CumulativeWindowSpec(maxSize, step) =>
      maxSize.equals(this.maxSize) && step.equals(this.step)
    case _ => false
  }
}
