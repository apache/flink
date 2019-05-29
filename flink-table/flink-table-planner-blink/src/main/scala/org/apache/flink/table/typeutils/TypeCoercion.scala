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

import org.apache.flink.table.`type`.{DecimalType, InternalType, InternalTypes, TimestampType}
import org.apache.flink.table.typeutils.TypeCheckUtils._

/**
  * Utilities for type conversions.
  */
object TypeCoercion {

  val numericWideningPrecedence: IndexedSeq[InternalType] =
    IndexedSeq(
      InternalTypes.BYTE,
      InternalTypes.SHORT,
      InternalTypes.INT,
      InternalTypes.LONG,
      InternalTypes.FLOAT,
      InternalTypes.DOUBLE)

  def widerTypeOf(tp1: InternalType, tp2: InternalType): Option[InternalType] = {
    (tp1, tp2) match {
      case (ti1, ti2) if ti1 == ti2 => Some(ti1)

      case (_, InternalTypes.STRING) => Some(InternalTypes.STRING)
      case (InternalTypes.STRING, _) => Some(InternalTypes.STRING)

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
    case (_, InternalTypes.STRING) => true

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
    *
    * Note: No distinction between explicit and implicit conversions
    * Note: This is a subset of SqlTypeAssignmentRule
    * Note: This may lose type during the cast.
    */
  def canCast(from: InternalType, to: InternalType): Boolean = (from, to) match {
    case (fromTp, toTp) if fromTp == toTp => true

    case (_, InternalTypes.STRING) => true

    case (InternalTypes.STRING, b) if isNumeric(b) => true
    case (InternalTypes.STRING, InternalTypes.BOOLEAN) => true
    case (InternalTypes.STRING, _: DecimalType) => true
    case (InternalTypes.STRING, InternalTypes.DATE) => true
    case (InternalTypes.STRING, InternalTypes.TIME) => true
    case (InternalTypes.STRING, _: TimestampType) => true

    case (InternalTypes.BOOLEAN, b) if isNumeric(b) => true
    case (InternalTypes.BOOLEAN, _: DecimalType) => true
    case (a, InternalTypes.BOOLEAN) if isNumeric(a) => true
    case (_: DecimalType, InternalTypes.BOOLEAN) => true

    case (a, b) if isNumeric(a) && isNumeric(b) => true
    case (a, _: DecimalType) if isNumeric(a) => true
    case (_: DecimalType, b) if isNumeric(b) => true
    case (_: DecimalType, _: DecimalType) => true
    case (InternalTypes.INT, InternalTypes.DATE) => true
    case (InternalTypes.INT, InternalTypes.TIME) => true
    case (InternalTypes.BYTE, _: TimestampType) => true
    case (InternalTypes.SHORT, _: TimestampType) => true
    case (InternalTypes.INT, _: TimestampType) => true
    case (InternalTypes.LONG, _: TimestampType) => true
    case (InternalTypes.DOUBLE, _: TimestampType) => true
    case (InternalTypes.FLOAT, _: TimestampType) => true
    case (InternalTypes.INT, InternalTypes.INTERVAL_MONTHS) => true
    case (InternalTypes.LONG, InternalTypes.INTERVAL_MILLIS) => true

    case (InternalTypes.DATE, InternalTypes.TIME) => false
    case (InternalTypes.TIME, InternalTypes.DATE) => false
    case (a, b) if isTimePoint(a) && isTimePoint(b) => true
    case (InternalTypes.DATE, InternalTypes.INT) => true
    case (InternalTypes.TIME, InternalTypes.INT) => true
    case (_: TimestampType, InternalTypes.BYTE) => true
    case (_: TimestampType, InternalTypes.INT) => true
    case (_: TimestampType, InternalTypes.SHORT) => true
    case (_: TimestampType, InternalTypes.LONG) => true
    case (_: TimestampType, InternalTypes.DOUBLE) => true
    case (_: TimestampType, InternalTypes.FLOAT) => true

    case (InternalTypes.INTERVAL_MONTHS, InternalTypes.INT) => true
    case (InternalTypes.INTERVAL_MILLIS, InternalTypes.LONG) => true

    case _ => false
  }

  /**
    * All the supported reinterpret types in flink-table.
    */
  def canReinterpret(from: InternalType, to: InternalType): Boolean = (from, to) match {
    case (fromTp, toTp) if fromTp == toTp => true

    case (InternalTypes.DATE, InternalTypes.INT) => true
    case (InternalTypes.TIME, InternalTypes.INT) => true
    case (_: TimestampType, InternalTypes.LONG) => true
    case (InternalTypes.INT, InternalTypes.DATE) => true
    case (InternalTypes.INT, InternalTypes.TIME) => true
    case (InternalTypes.LONG, _: TimestampType) => true
    case (InternalTypes.INT, InternalTypes.INTERVAL_MONTHS) => true
    case (InternalTypes.LONG, InternalTypes.INTERVAL_MILLIS) => true
    case (InternalTypes.INTERVAL_MONTHS, InternalTypes.INT) => true
    case (InternalTypes.INTERVAL_MILLIS, InternalTypes.LONG) => true

    case (InternalTypes.DATE, InternalTypes.LONG) => true
    case (InternalTypes.TIME, InternalTypes.LONG) => true
    case (InternalTypes.INTERVAL_MONTHS, InternalTypes.LONG) => true

    case _ => false
  }
}
