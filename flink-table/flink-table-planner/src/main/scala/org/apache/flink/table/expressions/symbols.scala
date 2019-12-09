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

import scala.language.{existentials, implicitConversions}

/**
  * General expression class to represent a symbol.
  */
case class SymbolPlannerExpression(symbol: PlannerSymbol) extends LeafExpression {

  override private[flink] def resultType: TypeInformation[_] =
    throw new UnsupportedOperationException("This should not happen. A symbol has no result type.")

  def toExpr: SymbolPlannerExpression = this // triggers implicit conversion

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    // dirty hack to pass Java enums to Java from Scala
    val enum = symbol.enum.asInstanceOf[Enum[T] forSome { type T <: Enum[T] }]
    relBuilder.getRexBuilder.makeFlag(enum)
  }

  override def toString: String = s"${symbol.symbols}.${symbol.name}"

}

/**
  * Symbol that wraps a Calcite symbol in form of a Java enum.
  */
trait PlannerSymbol {
  def symbols: PlannerSymbols
  def name: String
  def enum: Enum[_]
}

/**
  * Enumeration of symbols.
  */
abstract class PlannerSymbols extends Enumeration {

  class PlannerSymbolValue(e: Enum[_]) extends Val(e.name()) with PlannerSymbol {
    override def symbols: PlannerSymbols = PlannerSymbols.this

    override def enum: Enum[_] = e

    override def name: String = toString()
  }

  protected final def Value(enum: Enum[_]): PlannerSymbolValue = new PlannerSymbolValue(enum)

  implicit def symbolToExpression(symbol: PlannerSymbolValue): SymbolPlannerExpression =
    SymbolPlannerExpression(symbol)

}

/**
  * Units for working with time intervals.
  */
object PlannerTimeIntervalUnit extends PlannerSymbols {

  type PlannerTimeIntervalUnit = PlannerSymbolValue

  val YEAR = Value(TimeUnitRange.YEAR)
  val YEAR_TO_MONTH = Value(TimeUnitRange.YEAR_TO_MONTH)
  val QUARTER = Value(TimeUnitRange.QUARTER)
  val MONTH = Value(TimeUnitRange.MONTH)
  val WEEK = Value(TimeUnitRange.WEEK)
  val DAY = Value(TimeUnitRange.DAY)
  val DAY_TO_HOUR = Value(TimeUnitRange.DAY_TO_HOUR)
  val DAY_TO_MINUTE = Value(TimeUnitRange.DAY_TO_MINUTE)
  val DAY_TO_SECOND = Value(TimeUnitRange.DAY_TO_SECOND)
  val HOUR = Value(TimeUnitRange.HOUR)
  val HOUR_TO_MINUTE = Value(TimeUnitRange.HOUR_TO_MINUTE)
  val HOUR_TO_SECOND = Value(TimeUnitRange.HOUR_TO_SECOND)
  val MINUTE = Value(TimeUnitRange.MINUTE)
  val MINUTE_TO_SECOND = Value(TimeUnitRange.MINUTE_TO_SECOND)
  val SECOND = Value(TimeUnitRange.SECOND)

}

/**
  * Units for working with time points.
  */
object PlannerTimePointUnit extends PlannerSymbols {

  type PlannerTimePointUnit = PlannerSymbolValue

  val YEAR = Value(TimeUnit.YEAR)
  val MONTH = Value(TimeUnit.MONTH)
  val DAY = Value(TimeUnit.DAY)
  val HOUR = Value(TimeUnit.HOUR)
  val MINUTE = Value(TimeUnit.MINUTE)
  val SECOND = Value(TimeUnit.SECOND)
  val QUARTER = Value(TimeUnit.QUARTER)
  val WEEK = Value(TimeUnit.WEEK)
  val MILLISECOND = Value(TimeUnit.MILLISECOND)
  val MICROSECOND = Value(TimeUnit.MICROSECOND)

}

/**
  * Modes for trimming strings.
  */
object PlannerTrimMode extends PlannerSymbols {

  type PlannerTrimMode = PlannerSymbolValue

  val BOTH = Value(SqlTrimFunction.Flag.BOTH)
  val LEADING = Value(SqlTrimFunction.Flag.LEADING)
  val TRAILING = Value(SqlTrimFunction.Flag.TRAILING)

}
