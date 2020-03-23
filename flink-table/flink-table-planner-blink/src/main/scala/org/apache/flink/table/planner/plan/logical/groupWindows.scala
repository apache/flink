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

/**
  * Logical super class for group windows.
  *
  * @param aliasAttribute window alias
  * @param timeAttribute time field indicating event-time or processing-time
  */
abstract class LogicalWindow(
    val aliasAttribute: PlannerWindowReference,
    val timeAttribute: FieldReferenceExpression) {

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

  override def toString: String = s"SessionGroupWindow($alias, $timeField, $gap)"
}
