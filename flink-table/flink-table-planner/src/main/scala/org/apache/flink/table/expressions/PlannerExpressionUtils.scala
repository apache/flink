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

package org.apache.flink.table.expressions

import org.apache.flink.streaming.api.windowing.time.{Time => FlinkTime}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}

object PlannerExpressionUtils {

  private[flink] def isTimeIntervalLiteral(expr: PlannerExpression): Boolean = expr match {
    case Literal(_, TimeIntervalTypeInfo.INTERVAL_MILLIS) => true
    case _ => false
  }

  private[flink] def isRowCountLiteral(expr: PlannerExpression): Boolean = expr match {
    case Literal(_, RowIntervalTypeInfo.INTERVAL_ROWS) => true
    case _ => false
  }

  private[flink] def isTimeAttribute(expr: PlannerExpression): Boolean = expr match {
    case r: ResolvedFieldReference if FlinkTypeFactory.isTimeIndicatorType(r.resultType) =>
      true
    case _ => false
  }

  private[flink] def isRowtimeAttribute(expr: PlannerExpression): Boolean = expr match {
    case r: ResolvedFieldReference
      if FlinkTypeFactory.isRowtimeIndicatorType(r.resultType) =>
      true
    case _ => false
  }

  private[flink] def isProctimeAttribute(expr: PlannerExpression): Boolean = expr match {
    case r: ResolvedFieldReference
      if FlinkTypeFactory.isProctimeIndicatorType(r.resultType) =>
      true
    case _ => false
  }

  private[flink] def toTime(expr: PlannerExpression): FlinkTime = expr match {
    case Literal(value: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) =>
      FlinkTime.milliseconds(value)
    case _ => throw new IllegalArgumentException()
  }

  private[flink] def toLong(expr: PlannerExpression): Long = expr match {
    case Literal(value: Long, RowIntervalTypeInfo.INTERVAL_ROWS) => value
    case _ => throw new IllegalArgumentException()
  }
}
