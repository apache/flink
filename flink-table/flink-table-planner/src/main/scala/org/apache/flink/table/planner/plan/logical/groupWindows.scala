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

import org.apache.flink.table.expressions._
import org.apache.flink.table.planner.expressions.PlannerWindowReference
import org.apache.flink.table.planner.plan.utils.AggregateUtil.{hasTimeIntervalType, toLong}

import java.time.Duration
import java.util.Objects

/**
 * Logical super class for group windows.
 *
 * @param aliasAttribute window alias
 * @param timeAttribute time field indicating event-time or processing-time
 */
abstract class LogicalWindow(
    val aliasAttribute: PlannerWindowReference,
    val timeAttribute: FieldReferenceExpression) {

  override def equals(o: Any): Boolean = {
    if (o == null || (getClass ne o.getClass)) {
      return false
    }
    val that = o.asInstanceOf[LogicalWindow]
    Objects.equals(aliasAttribute, that.aliasAttribute) &&
      Objects.equals(timeAttribute, that.timeAttribute)
  }

  protected def isValueLiteralExpressionEqual(
      l1: ValueLiteralExpression, l2: ValueLiteralExpression): Boolean = {
    if (l1 == null && l2 == null) {
      true
    } else if (l1 == null || l2 == null) {
      false
    } else {
      if (hasTimeIntervalType(l1)) {
        if (hasTimeIntervalType(l2)) {
          val v1 = l1.getValueAs(classOf[Duration])
          val v2 = l2.getValueAs(classOf[Duration])
          v1.equals(v2) && l1.getOutputDataType.equals(l2.getOutputDataType)
        } else {
          false
        }
      } else {
        val v1 = toLong(l1)
        val v2 = toLong(l2)
        v1 == v2 && l1.getOutputDataType.equals(l2.getOutputDataType)
      }
    }
  }

  override def toString: String = getClass.getSimpleName
}

// ------------------------------------------------------------------------------------------------
// Tumbling group windows
// ------------------------------------------------------------------------------------------------

case class TumblingGroupWindow(
    alias: PlannerWindowReference,
    timeField: FieldReferenceExpression,
    size: ValueLiteralExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def equals(o: Any): Boolean = {
    if (super.equals(o)) {
      isValueLiteralExpressionEqual(size, o.asInstanceOf[TumblingGroupWindow].size)
    } else {
      false
    }
  }

  override def toString: String = s"TumblingGroupWindow($alias, $timeField, $size)"
}

// ------------------------------------------------------------------------------------------------
// Sliding group windows
// ------------------------------------------------------------------------------------------------

case class SlidingGroupWindow(
    alias: PlannerWindowReference,
    timeField: FieldReferenceExpression,
    size: ValueLiteralExpression,
    slide: ValueLiteralExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def equals(o: Any): Boolean = {
    if (super.equals(o)) {
      isValueLiteralExpressionEqual(size, o.asInstanceOf[SlidingGroupWindow].size) &&
        isValueLiteralExpressionEqual(slide, o.asInstanceOf[SlidingGroupWindow].slide)
    } else {
      false
    }
  }

  override def toString: String = s"SlidingGroupWindow($alias, $timeField, $size, $slide)"
}

// ------------------------------------------------------------------------------------------------
// Session group windows
// ------------------------------------------------------------------------------------------------

case class SessionGroupWindow(
    alias: PlannerWindowReference,
    timeField: FieldReferenceExpression,
    gap: ValueLiteralExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def equals(o: Any): Boolean = {
    if (super.equals(o)) {
      isValueLiteralExpressionEqual(gap, o.asInstanceOf[SessionGroupWindow].gap)
    } else {
      false
    }
  }

  override def toString: String = s"SessionGroupWindow($alias, $timeField, $gap)"
}
