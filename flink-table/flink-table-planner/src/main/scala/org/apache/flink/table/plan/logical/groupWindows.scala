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

package org.apache.flink.table.plan.logical

import org.apache.flink.table.expressions.PlannerExpression

// ------------------------------------------------------------------------------------------------
// Group windows
// ------------------------------------------------------------------------------------------------

sealed trait LogicalWindow {
  def timeAttribute: PlannerExpression
  def aliasAttribute: PlannerExpression
}

case class TumblingGroupWindow(
    aliasAttribute: PlannerExpression,
    timeAttribute: PlannerExpression,
    size: PlannerExpression)
  extends LogicalWindow {

  override def toString: String = s"TumblingGroupWindow($aliasAttribute, $timeAttribute, $size)"
}

case class SlidingGroupWindow(
    aliasAttribute: PlannerExpression,
    timeAttribute: PlannerExpression,
    size: PlannerExpression,
    slide: PlannerExpression)
  extends LogicalWindow {

  override def toString: String = s"SlidingGroupWindow($aliasAttribute, $timeAttribute, $size, " +
    s"$slide)"
}

case class SessionGroupWindow(
    aliasAttribute: PlannerExpression,
    timeAttribute: PlannerExpression,
    gap: PlannerExpression)
  extends LogicalWindow {

  override def toString: String = s"SessionGroupWindow($aliasAttribute, $timeAttribute, $gap)"
}
