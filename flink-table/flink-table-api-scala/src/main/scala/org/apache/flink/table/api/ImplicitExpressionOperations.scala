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
package org.apache.flink.table.api

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.table.api.internal.BaseExpressions
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.ApiExpressionUtils._
import org.apache.flink.table.functions.BuiltInFunctionDefinitions._

import scala.language.implicitConversions

/**
 * These are all the operations that can be used to construct an [[Expression]] AST for expression
 * operations.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(since = "1.18.0")
@PublicEvolving
trait ImplicitExpressionOperations extends BaseExpressions[Expression, Expression] {
  private[flink] def expr: Expression

  override def toExpr: Expression = expr

  override protected def toApiSpecificExpression(expression: Expression): Expression = expression

  /**
   * Specifies a name for an expression i.e. a field.
   *
   * @param name
   *   name for one field
   * @param extraNames
   *   additional names if the expression expands to multiple fields
   * @return
   *   field with an alias
   */
  def as(name: Symbol, extraNames: Symbol*): Expression = as(name.name, extraNames.map(_.name): _*)

  /** Boolean AND in three-valued logic. */
  def &&(other: Expression): Expression = and(other)

  /** Boolean OR in three-valued logic. */
  def ||(other: Expression): Expression = or(other)

  /** Greater than. */
  def >(other: Expression): Expression = isGreater(other)

  /** Greater than or equal. */
  def >=(other: Expression): Expression = isGreaterOrEqual(other)

  /** Less than. */
  def <(other: Expression): Expression = isLess(other)

  /** Less than or equal. */
  def <=(other: Expression): Expression = isLessOrEqual(other)

  /** Equals. */
  def ===(other: Expression): Expression = isEqual(other)

  /** Not equal. */
  def !==(other: Expression): Expression = isNotEqual(other)

  /** Whether boolean expression is not true; returns null if boolean is null. */
  def unary_! : Expression = unresolvedCall(NOT, expr)

  /** Returns negative numeric. */
  def unary_- : Expression = Expressions.negative(expr)

  /** Returns numeric. */
  def unary_+ : Expression = expr

  /** Returns left plus right. */
  def +(other: Expression): Expression = plus(other)

  /** Returns left minus right. */
  def -(other: Expression): Expression = minus(other)

  /** Returns left divided by right. */
  def /(other: Expression): Expression = dividedBy(other)

  /** Returns left multiplied by right. */
  def *(other: Expression): Expression = times(other)

  /**
   * Returns the remainder (modulus) of left divided by right. The result is negative only if left
   * is negative.
   */
  def %(other: Expression): Expression = mod(other)

  /**
   * Indicates the range from left to right, i.e. [left, right], which can be used in columns
   * selection.
   *
   * e.g. withColumns(1 to 3)
   */
  def to(other: Expression): Expression = unresolvedCall(RANGE_TO, expr, objectToExpression(other))

  /**
   * Ternary conditional operator that decides which of two other expressions should be based on a
   * evaluated boolean condition.
   *
   * e.g. ($"f0" > 5).?("A", "B") leads to "A"
   *
   * @param ifTrue
   *   expression to be evaluated if condition holds
   * @param ifFalse
   *   expression to be evaluated if condition does not hold
   */
  def ?(ifTrue: Expression, ifFalse: Expression): Expression =
    Expressions.ifThenElse(expr, ifTrue, ifFalse).toExpr

  // scalar functions

  /**
   * Removes leading and/or trailing characters from the given string.
   *
   * @param removeLeading
   *   if true, remove leading characters (default: true)
   * @param removeTrailing
   *   if true, remove trailing characters (default: true)
   * @param character
   *   string containing the character (default: " ")
   * @return
   *   trimmed string
   */
  def trim(
      removeLeading: Boolean = true,
      removeTrailing: Boolean = true,
      character: Expression = valueLiteral(" ")): Expression = {
    unresolvedCall(
      TRIM,
      valueLiteral(removeLeading),
      valueLiteral(removeTrailing),
      ApiExpressionUtils.objectToExpression(character),
      expr)
  }

  // Row interval type

  /**
   * Creates an interval of rows.
   *
   * @return
   *   interval of rows
   */
  def rows: Expression = toRowInterval(expr)
}
