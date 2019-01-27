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

import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType, TimestampType}
import org.apache.flink.table.typeutils.TypeCheckUtils._

/**
  * Utilities for type conversions.
  */
object TypeCoercion {

  val numericWideningPrecedence: IndexedSeq[InternalType] =
    IndexedSeq(
      DataTypes.BYTE,
      DataTypes.SHORT,
      DataTypes.INT,
      DataTypes.LONG,
      DataTypes.FLOAT,
      DataTypes.DOUBLE)

  def widerTypeOf(tp1: InternalType, tp2: InternalType): Option[InternalType] = {
    (tp1, tp2) match {
      case (ti1, ti2) if ti1 == ti2 => Some(ti1)

      case (_, DataTypes.STRING) => Some(DataTypes.STRING)
      case (DataTypes.STRING, _) => Some(DataTypes.STRING)

      case (_, dt: DecimalType) => Some(dt)
      case (dt: DecimalType, _) => Some(dt)

      case (a, b) if isTimePoint(a) && isTimeInterval(b) => Some(a)
      case (a, b) if isTimeInterval(a) && isTimePoint(b) => Some(b)

      case tuple if tuple.productIterator.forall(numericWideningPrecedence.contains) =>
        val higherIndex = numericWideningPrecedence.lastIndexWhere(t => t == tp1 || t == tp2)
        Some(numericWideningPrecedence(higherIndex))

      case _ => None
    }
  }

  /**
    * Test if we can do cast safely without lose of type.
    */
  def canSafelyCast(from: InternalType, to: InternalType): Boolean = (from, to) match {
    case (_, DataTypes.STRING) => true

    case (a, _: DecimalType) if isNumeric(a) => true

    case tuple if tuple.productIterator.forall(numericWideningPrecedence.contains) =>
      if (numericWideningPrecedence.indexOf(from) < numericWideningPrecedence.indexOf(to)) {
        true
      } else {
        false
      }

    case _ => false
  }

  /**
    * All the supported cast types in flink-table.
    * Note: No distinction between explicit and implicit conversions
    * Note: This is a subset of SqlTypeAssignmentRule
    * Note: This may lose type during the cast.
    *
    */
  def canCast(from: InternalType, to: InternalType): Boolean = (from, to) match {
    case (fromTp, toTp) if fromTp == toTp => true

    case (_, DataTypes.STRING) => true

    case (_, DataTypes.CHAR) => false // Character type not supported.

    case (DataTypes.STRING, b) if isNumeric(b) => true
    case (DataTypes.STRING, DataTypes.BOOLEAN) => true
    case (DataTypes.STRING, _: DecimalType) => true
    case (DataTypes.STRING, DataTypes.DATE) => true
    case (DataTypes.STRING, DataTypes.TIME) => true
    case (DataTypes.STRING, _: TimestampType) => true

    case (DataTypes.BOOLEAN, b) if isNumeric(b) => true
    case (DataTypes.BOOLEAN, _: DecimalType) => true
    case (a, DataTypes.BOOLEAN) if isNumeric(a) => true
    case (_: DecimalType, DataTypes.BOOLEAN) => true

    case (a, b) if isNumeric(a) && isNumeric(b) => true
    case (a, _: DecimalType) if isNumeric(a) => true
    case (_: DecimalType, b) if isNumeric(b) => true
    case (_: DecimalType, _: DecimalType) => true
    case (DataTypes.INT, DataTypes.DATE) => true
    case (DataTypes.INT, DataTypes.TIME) => true
    case (DataTypes.BYTE, _: TimestampType) => true
    case (DataTypes.SHORT, _: TimestampType) => true
    case (DataTypes.INT, _: TimestampType) => true
    case (DataTypes.LONG, _: TimestampType) => true
    case (DataTypes.DOUBLE, _: TimestampType) => true
    case (DataTypes.FLOAT, _: TimestampType) => true
    case (DataTypes.INT, DataTypes.INTERVAL_MONTHS) => true
    case (DataTypes.LONG, DataTypes.INTERVAL_MILLIS) => true

    case (DataTypes.DATE, DataTypes.TIME) => false
    case (DataTypes.TIME, DataTypes.DATE) => false
    case (a, b) if isTimePoint(a) && isTimePoint(b) => true
    case (DataTypes.DATE, DataTypes.INT) => true
    case (DataTypes.TIME, DataTypes.INT) => true
    case (_: TimestampType, DataTypes.BYTE) => true
    case (_: TimestampType, DataTypes.INT) => true
    case (_: TimestampType, DataTypes.SHORT) => true
    case (_: TimestampType, DataTypes.LONG) => true
    case (_: TimestampType, DataTypes.DOUBLE) => true
    case (_: TimestampType, DataTypes.FLOAT) => true

    case (DataTypes.INTERVAL_MONTHS, DataTypes.INT) => true
    case (DataTypes.INTERVAL_MILLIS, DataTypes.LONG) => true

    case _ => false
  }

  /**
    * All the supported reinterpret types in flink-table.
    *
    */
  def canReinterpret(from: InternalType, to: InternalType): Boolean = (from, to) match {
    case (fromTp, toTp) if fromTp == toTp => true

    case (DataTypes.DATE, DataTypes.INT) => true
    case (DataTypes.TIME, DataTypes.INT) => true
    case (_: TimestampType, DataTypes.LONG) => true
    case (DataTypes.INT, DataTypes.DATE) => true
    case (DataTypes.INT, DataTypes.TIME) => true
    case (DataTypes.LONG, _: TimestampType) => true
    case (DataTypes.INT, DataTypes.INTERVAL_MONTHS) => true
    case (DataTypes.LONG, DataTypes.INTERVAL_MILLIS) => true
    case (DataTypes.INTERVAL_MONTHS, DataTypes.INT) => true
    case (DataTypes.INTERVAL_MILLIS, DataTypes.LONG) => true

    case (DataTypes.DATE, DataTypes.LONG) => true
    case (DataTypes.TIME, DataTypes.LONG) => true
    case (DataTypes.INTERVAL_MONTHS, DataTypes.LONG) => true

    case _ => false
  }
}
