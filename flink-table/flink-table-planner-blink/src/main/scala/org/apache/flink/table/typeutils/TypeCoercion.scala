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

package org.apache.flink.table.typeutils

import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, FloatType, IntType, LogicalType, SmallIntType, TinyIntType}
import org.apache.flink.table.typeutils.TypeCheckUtils._

/**
  * Utilities for type conversions.
  */
object TypeCoercion {

  val numericWideningPrecedence: IndexedSeq[LogicalType] =
    IndexedSeq(
      new TinyIntType(),
      new SmallIntType(),
      new IntType(),
      new BigIntType(),
      new FloatType(),
      new DoubleType())

  def widerTypeOf(tp1: LogicalType, tp2: LogicalType): Option[LogicalType] = {
    (tp1.getTypeRoot, tp2.getTypeRoot) match {
      case (_, _) if tp1 == tp2 => Some(tp1)

      case (_, VARCHAR) => Some(tp2)
      case (VARCHAR, _) => Some(tp1)

      case (_, DECIMAL) => Some(tp2)
      case (DECIMAL, _) => Some(tp1)

      case (_, _) if isTimePoint(tp1) && isTimeInterval(tp2) => Some(tp1)
      case (_, _) if isTimeInterval(tp1) && isTimePoint(tp2) => Some(tp2)

      case (_, _) if numericWideningPrecedence.contains(tp1) &&
          numericWideningPrecedence.contains(tp2) =>
        val higherIndex = numericWideningPrecedence.lastIndexWhere(t => t == tp1 || t == tp2)
        Some(numericWideningPrecedence(higherIndex))

      case _ => None
    }
  }

  /**
    * Test if we can do cast safely without lose of type.
    */
  def canSafelyCast(
      from: LogicalType, to: LogicalType): Boolean = (from.getTypeRoot, to.getTypeRoot) match {
    case (_, VARCHAR) => true

    case (_, DECIMAL) if isNumeric(from) => true

    case (_, _) if numericWideningPrecedence.contains(from) &&
        numericWideningPrecedence.contains(to) =>
      if (numericWideningPrecedence.indexOf(from) < numericWideningPrecedence.indexOf(to)) {
        true
      } else {
        false
      }

    case _ => false
  }

  /**
    * All the supported cast types in flink-table.
    *
    * Note: No distinction between explicit and implicit conversions
    * Note: This is a subset of SqlTypeAssignmentRule
    * Note: This may lose type during the cast.
    */
  def canCast(
      from: LogicalType, to: LogicalType): Boolean = (from.getTypeRoot, to.getTypeRoot) match {
    case (_, _) if from == to => true

    case (_, VARCHAR) => true

    case (VARCHAR, _) if isNumeric(to) => true
    case (VARCHAR, BOOLEAN) => true
    case (VARCHAR, DECIMAL) => true
    case (VARCHAR, DATE) => true
    case (VARCHAR, TIME_WITHOUT_TIME_ZONE) => true
    case (VARCHAR, TIMESTAMP_WITHOUT_TIME_ZONE) => true

    case (BOOLEAN, _) if isNumeric(to) => true
    case (BOOLEAN, DECIMAL) => true
    case (_, BOOLEAN) if isNumeric(from) => true
    case (DECIMAL, BOOLEAN) => true

    case (_, _) if isNumeric(from) && isNumeric(to) => true
    case (_, DECIMAL) if isNumeric(from) => true
    case (DECIMAL, _) if isNumeric(to) => true
    case (DECIMAL, DECIMAL) => true
    case (INTEGER, DATE) => true
    case (INTEGER, TIME_WITHOUT_TIME_ZONE) => true
    case (TINYINT, TIMESTAMP_WITHOUT_TIME_ZONE) => true
    case (SMALLINT, TIMESTAMP_WITHOUT_TIME_ZONE) => true
    case (INTEGER, TIMESTAMP_WITHOUT_TIME_ZONE) => true
    case (BIGINT, TIMESTAMP_WITHOUT_TIME_ZONE) => true
    case (DOUBLE, TIMESTAMP_WITHOUT_TIME_ZONE) => true
    case (FLOAT, TIMESTAMP_WITHOUT_TIME_ZONE) => true
    case (INTEGER, INTERVAL_YEAR_MONTH) => true
    case (BIGINT, INTERVAL_DAY_TIME) => true

    case (DATE, TIME_WITHOUT_TIME_ZONE) => false
    case (TIME_WITHOUT_TIME_ZONE, DATE) => false
    case (_, _) if isTimePoint(from) && isTimePoint(to) => true
    case (DATE, INTEGER) => true
    case (TIME_WITHOUT_TIME_ZONE, INTEGER) => true
    case (TIMESTAMP_WITHOUT_TIME_ZONE, TINYINT) => true
    case (TIMESTAMP_WITHOUT_TIME_ZONE, INTEGER) => true
    case (TIMESTAMP_WITHOUT_TIME_ZONE, SMALLINT) => true
    case (TIMESTAMP_WITHOUT_TIME_ZONE, BIGINT) => true
    case (TIMESTAMP_WITHOUT_TIME_ZONE, DOUBLE) => true
    case (TIMESTAMP_WITHOUT_TIME_ZONE, FLOAT) => true

    case (INTERVAL_YEAR_MONTH, INTEGER) => true
    case (INTERVAL_DAY_TIME, BIGINT) => true

    case _ => false
  }

  /**
    * All the supported reinterpret types in flink-table.
    */
  def canReinterpret(
      from: LogicalType, to: LogicalType): Boolean = (from.getTypeRoot, to.getTypeRoot) match {
    case (_, _) if from == to => true

    case (DATE, INTEGER) => true
    case (TIME_WITHOUT_TIME_ZONE, INTEGER) => true
    case (TIMESTAMP_WITHOUT_TIME_ZONE, BIGINT) => true
    case (INTEGER, DATE) => true
    case (INTEGER, TIME_WITHOUT_TIME_ZONE) => true
    case (BIGINT, TIMESTAMP_WITHOUT_TIME_ZONE) => true
    case (INTEGER, INTERVAL_YEAR_MONTH) => true
    case (BIGINT, INTERVAL_DAY_TIME) => true
    case (INTERVAL_YEAR_MONTH, INTEGER) => true
    case (INTERVAL_DAY_TIME, BIGINT) => true

    case (DATE, BIGINT) => true
    case (TIME_WITHOUT_TIME_ZONE, BIGINT) => true
    case (INTERVAL_YEAR_MONTH, BIGINT) => true

    case _ => false
  }
}
