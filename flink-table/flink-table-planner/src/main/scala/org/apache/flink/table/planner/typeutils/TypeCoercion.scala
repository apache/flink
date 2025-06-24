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
package org.apache.flink.table.planner.typeutils

import org.apache.flink.table.runtime.typeutils.TypeCheckUtils._
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, FloatType, IntType, LogicalType, SmallIntType, TinyIntType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

/** Utilities for type conversions. */
object TypeCoercion {

  var numericWideningPrecedence: IndexedSeq[LogicalType] =
    IndexedSeq(
      new TinyIntType(),
      new SmallIntType(),
      new IntType(),
      new BigIntType(),
      new FloatType(),
      new DoubleType())

  numericWideningPrecedence ++= numericWideningPrecedence.map(_.copy(false))

  /** Test if we can do cast safely without lose of type. */
  def canSafelyCast(from: LogicalType, to: LogicalType): Boolean =
    (from.getTypeRoot, to.getTypeRoot) match {
      case (_, VARCHAR | CHAR) => true

      case (_, DECIMAL) if isNumeric(from) => true

      case (_, _)
          if numericWideningPrecedence.contains(from) &&
            numericWideningPrecedence.contains(to) =>
        if (numericWideningPrecedence.indexOf(from) < numericWideningPrecedence.indexOf(to)) {
          true
        } else {
          false
        }

      case _ => false
    }
}
