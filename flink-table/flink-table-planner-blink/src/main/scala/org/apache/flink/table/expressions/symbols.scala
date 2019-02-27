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

import org.apache.calcite.avatica.util.{TimeUnit, TimeUnitRange}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlTrimFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableException

import scala.language.{existentials, implicitConversions}

/**
  * General expression class to represent a symbol.
  */
case class SymbolPlannerExpression(symbol: TableSymbol) extends LeafExpression {

  override private[flink] def resultType: TypeInformation[_] =
    throw new UnsupportedOperationException("This should not happen. A symbol has no result type.")

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.getRexBuilder.makeFlag(mapToCalciteSymbol(symbol))
  }

  def mapToCalciteSymbol(symbol: TableSymbol): Enum[_] = {
    symbol match {
      // TimeIntervalUnit
      case TimeIntervalUnit.YEAR => TimeUnitRange.YEAR
      case TimeIntervalUnit.YEAR_TO_MONTH => TimeUnitRange.YEAR_TO_MONTH
      case TimeIntervalUnit.QUARTER => TimeUnitRange.QUARTER
      case TimeIntervalUnit.MONTH => TimeUnitRange.MONTH
      case TimeIntervalUnit.WEEK => TimeUnitRange.WEEK
      case TimeIntervalUnit.DAY => TimeUnitRange.DAY
      case TimeIntervalUnit.DAY_TO_HOUR => TimeUnitRange.DAY_TO_HOUR
      case TimeIntervalUnit.DAY_TO_MINUTE => TimeUnitRange.DAY_TO_MINUTE
      case TimeIntervalUnit.DAY_TO_SECOND => TimeUnitRange.DAY_TO_SECOND
      case TimeIntervalUnit.HOUR => TimeUnitRange.HOUR
      case TimeIntervalUnit.SECOND => TimeUnitRange.SECOND
      case TimeIntervalUnit.HOUR_TO_MINUTE => TimeUnitRange.HOUR_TO_MINUTE
      case TimeIntervalUnit.HOUR_TO_SECOND => TimeUnitRange.HOUR_TO_SECOND
      case TimeIntervalUnit.MINUTE => TimeUnitRange.MINUTE
      case TimeIntervalUnit.MINUTE_TO_SECOND => TimeUnitRange.MINUTE_TO_SECOND

      // TrimMode
      case TrimMode.BOTH => SqlTrimFunction.Flag.BOTH
      case TrimMode.LEADING => SqlTrimFunction.Flag.LEADING
      case TrimMode.TRAILING => SqlTrimFunction.Flag.TRAILING

      // TimePointUnit
      case TimePointUnit.YEAR => TimeUnit.YEAR
      case TimePointUnit.MONTH => TimeUnit.MONTH
      case TimePointUnit.DAY => TimeUnit.DAY
      case TimePointUnit.HOUR => TimeUnit.HOUR
      case TimePointUnit.MINUTE => TimeUnit.MINUTE
      case TimePointUnit.SECOND => TimeUnit.SECOND
      case TimePointUnit.QUARTER => TimeUnit.QUARTER
      case TimePointUnit.WEEK => TimeUnit.WEEK
      case TimePointUnit.MILLISECOND => TimeUnit.MILLISECOND
      case TimePointUnit.MICROSECOND => TimeUnit.MICROSECOND
      case _ => throw new TableException("Unsupported TableSymbol: " + symbol)
    }
  }

  override def toString: String = s"${symbol.getClass.getSimpleName}.${symbol.toString}"
}
