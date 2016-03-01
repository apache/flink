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
package org.apache.flink.api.scala.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions._

import scala.language.implicitConversions

/**
 * These are all the operations that can be used to construct an [[Expression]] AST for expression
 * operations.
 *
 * These operations must be kept in sync with the parser in
 * [[org.apache.flink.api.table.parser.ExpressionParser]].
 */
trait ImplicitExpressionOperations {
  def expr: Expression

  def && (other: Expression) = And(expr, other)
  def || (other: Expression) = Or(expr, other)

  def > (other: Expression) = GreaterThan(expr, other)
  def >= (other: Expression) = GreaterThanOrEqual(expr, other)
  def < (other: Expression) = LessThan(expr, other)
  def <= (other: Expression) = LessThanOrEqual(expr, other)

  def === (other: Expression) = EqualTo(expr, other)
  def !== (other: Expression) = NotEqualTo(expr, other)

  def unary_! = Not(expr)
  def unary_- = UnaryMinus(expr)

  def isNull = IsNull(expr)
  def isNotNull = IsNotNull(expr)

  def + (other: Expression) = Plus(expr, other)
  def - (other: Expression) = Minus(expr, other)
  def / (other: Expression) = Div(expr, other)
  def * (other: Expression) = Mul(expr, other)
  def % (other: Expression) = Mod(expr, other)

  def abs = Abs(expr)

  def sum = Sum(expr)
  def min = Min(expr)
  def max = Max(expr)
  def count = Count(expr)
  def avg = Avg(expr)

  def cast(toType: TypeInformation[_]) = Cast(expr, toType)

  def as(name: Symbol) = Naming(expr, name.name)

  // scalar functions

  /**
    * Creates a substring of the given string between the given indices.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @param endIndex last character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression, endIndex: Expression) = {
    Call(BuiltInFunctionNames.SUBSTRING, expr, beginIndex, endIndex)
  }

  /**
    * Creates a substring of the given string beginning at the given index to the end.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression) = {
    Call(BuiltInFunctionNames.SUBSTRING, expr, beginIndex)
  }

  /**
    * Removes leading and/or trailing characters from the given string.
    *
    * @param removeLeading if true, remove leading characters (default: true)
    * @param removeTrailing if true, remove trailing characters (default: true)
    * @param character String containing the character (default: " ")
    * @return trimmed string
    */
  def trim(
      removeLeading: Boolean = true,
      removeTrailing: Boolean = true,
      character: Expression = BuiltInFunctionConstants.TRIM_DEFAULT_CHAR) = {
    if (removeLeading && removeTrailing) {
      Call(
        BuiltInFunctionNames.TRIM,
        BuiltInFunctionConstants.TRIM_BOTH,
        character,
        expr)
    } else if (removeLeading) {
      Call(
        BuiltInFunctionNames.TRIM,
        BuiltInFunctionConstants.TRIM_LEADING,
        character,
        expr)
    } else if (removeTrailing) {
      Call(
        BuiltInFunctionNames.TRIM,
        BuiltInFunctionConstants.TRIM_TRAILING,
        character,
        expr)
    } else {
      expr
    }
  }
}

/**
 * Implicit conversions from Scala Literals to Expression [[Literal]] and from [[Expression]]
 * to [[ImplicitExpressionOperations]].
 */
trait ImplicitExpressionConversions {
  implicit class WithOperations(e: Expression) extends ImplicitExpressionOperations {
    def expr = e
  }

  implicit class SymbolExpression(s: Symbol) extends ImplicitExpressionOperations {
    def expr = UnresolvedFieldReference(s.name)
  }

  implicit class LiteralLongExpression(l: Long) extends ImplicitExpressionOperations {
    def expr = Literal(l)
  }

  implicit class LiteralIntExpression(i: Int) extends ImplicitExpressionOperations {
    def expr = Literal(i)
  }

  implicit class LiteralFloatExpression(f: Float) extends ImplicitExpressionOperations {
    def expr = Literal(f)
  }

  implicit class LiteralDoubleExpression(d: Double) extends ImplicitExpressionOperations {
    def expr = Literal(d)
  }

  implicit class LiteralStringExpression(str: String) extends ImplicitExpressionOperations {
    def expr = Literal(str)
  }

  implicit class LiteralBooleanExpression(bool: Boolean) extends ImplicitExpressionOperations {
    def expr = Literal(bool)
  }

  implicit def symbol2FieldExpression(sym: Symbol): Expression = UnresolvedFieldReference(sym.name)
  implicit def int2Literal(i: Int): Expression = Literal(i)
  implicit def long2Literal(l: Long): Expression = Literal(l)
  implicit def double2Literal(d: Double): Expression = Literal(d)
  implicit def float2Literal(d: Float): Expression = Literal(d)
  implicit def string2Literal(str: String): Expression = Literal(str)
  implicit def boolean2Literal(bool: Boolean): Expression = Literal(bool)
}
