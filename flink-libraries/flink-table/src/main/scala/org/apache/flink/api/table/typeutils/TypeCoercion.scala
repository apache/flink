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

package org.apache.flink.api.table.typeutils

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{NumericTypeInfo, TypeInformation}

/**
  * Utilities for type conversions.
  */
object TypeCoercion {

  val numericWideningPrecedence: IndexedSeq[TypeInformation[_]] =
    IndexedSeq(
      BYTE_TYPE_INFO,
      SHORT_TYPE_INFO,
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      FLOAT_TYPE_INFO,
      DOUBLE_TYPE_INFO)

  def widerTypeOf(tp1: TypeInformation[_], tp2: TypeInformation[_]): Option[TypeInformation[_]] = {
    (tp1, tp2) match {
      case (tp1, tp2) if tp1 == tp2 => Some(tp1)

      case (_, STRING_TYPE_INFO) => Some(STRING_TYPE_INFO)
      case (STRING_TYPE_INFO, _) => Some(STRING_TYPE_INFO)

      case tuple if tuple.productIterator.forall(numericWideningPrecedence.contains) =>
        val higherIndex = numericWideningPrecedence.lastIndexWhere(t => t == tp1 || t == tp2)
        Some(numericWideningPrecedence(higherIndex))

      case _ => None
    }
  }

  /**
    * Test if we can do cast safely without lose of information.
    */
  def canSafelyCast(from: TypeInformation[_], to: TypeInformation[_]): Boolean = (from, to) match {
    case (_, STRING_TYPE_INFO) => true

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
    * Note: This may lose information during the cast.
    */
  def canCast(from: TypeInformation[_], to: TypeInformation[_]): Boolean = (from, to) match {
    case (from, to) if from == to => true

    case (_, STRING_TYPE_INFO) => true

    case (_, DATE_TYPE_INFO) => false // Date type not supported yet.
    case (_, VOID_TYPE_INFO) => false // Void type not supported
    case (_, CHAR_TYPE_INFO) => false // Character type not supported.

    case (STRING_TYPE_INFO, _: NumericTypeInfo[_]) => true
    case (STRING_TYPE_INFO, BOOLEAN_TYPE_INFO) => true

    case (BOOLEAN_TYPE_INFO, _: NumericTypeInfo[_]) => true
    case (_: NumericTypeInfo[_], BOOLEAN_TYPE_INFO) => true

    case (_: NumericTypeInfo[_], _: NumericTypeInfo[_]) => true

    case _ => false
  }
}
