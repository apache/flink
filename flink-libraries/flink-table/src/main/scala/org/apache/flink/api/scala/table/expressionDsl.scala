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

import java.sql.{Timestamp, Time, Date}

import scala.language.implicitConversions

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.table.expressions._

/**
 * These are all the operations that can be used to construct an [[Expression]] AST for expression
 * operations.
 *
 * These operations must be kept in sync with the parser in
 * [[org.apache.flink.api.table.expressions.ExpressionParser]].
 */
trait ImplicitExpressionOperations {
  private[flink] def expr: Expression

  /**
    * Enables literals on left side of binary expressions.
    *
    * e.g. 12.toExpr % 'a
    *
    * @return expression
    */
  def toExpr: Expression = expr

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
  def % (other: Expression) = mod(other)

  def sum = Sum(expr)
  def min = Min(expr)
  def max = Max(expr)
  def count = Count(expr)
  def avg = Avg(expr)

  def cast(toType: TypeInformation[_]) = Cast(expr, toType)

  def as(name: Symbol) = Alias(expr, name.name)

  def asc = Asc(expr)
  def desc = Desc(expr)

  /**
    * Ternary conditional operator that decides which of two other expressions should be evaluated
    * based on a evaluated boolean condition.
    *
    * e.g. (42 > 5).?("A", "B") leads to "A"
    *
    * @param ifTrue expression to be evaluated if condition holds
    * @param ifFalse expression to be evaluated if condition does not hold
    */
  def ?(ifTrue: Expression, ifFalse: Expression) = {
    If(expr, ifTrue, ifFalse)
  }

  // scalar functions

  /**
    * Calculates the remainder of division the given number by another one.
    */
  def mod(other: Expression) = Mod(expr, other)

  /**
    * Calculates the Euler's number raised to the given power.
    */
  def exp() = Exp(expr)

  /**
    * Calculates the base 10 logarithm of given value.
    */
  def log10() = Log10(expr)

  /**
    * Calculates the natural logarithm of given value.
    */
  def ln() = Ln(expr)

  /**
    * Calculates the given number raised to the power of the other value.
    */
  def power(other: Expression) = Power(expr, other)

  /**
    * Calculates the absolute value of given one.
    */
  def abs() = Abs(expr)

  /**
    * Calculates the largest integer less than or equal to a given number.
    */
  def floor() = Floor(expr)

  /**
    * Calculates the smallest integer greater than or equal to a given number.
    */
  def ceil() = Ceil(expr)

  /**
    * Creates a substring of the given string between the given indices.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @param endIndex last character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression, endIndex: Expression) =
    SubString(expr, beginIndex, endIndex)

  /**
    * Creates a substring of the given string beginning at the given index to the end.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression) =
    new SubString(expr, beginIndex)

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
      character: Expression = TrimConstants.TRIM_DEFAULT_CHAR) = {
    if (removeLeading && removeTrailing) {
      Trim(TrimConstants.TRIM_BOTH, character, expr)
    } else if (removeLeading) {
      Trim(TrimConstants.TRIM_LEADING, character, expr)
    } else if (removeTrailing) {
      Trim(TrimConstants.TRIM_TRAILING, character, expr)
    } else {
      expr
    }
  }

  /**
    * Returns the length of a String.
    */
  def charLength() = CharLength(expr)

  /**
    * Returns all of the characters in a String in upper case using the rules of
    * the default locale.
    */
  def upperCase() = Upper(expr)

  /**
    * Returns all of the characters in a String in lower case using the rules of
    * the default locale.
    */
  def lowerCase() = Lower(expr)

  /**
    * Converts the initial letter of each word in a String to uppercase.
    * Assumes a String containing only [A-Za-z0-9], everything else is treated as whitespace.
    */
  def initCap() = InitCap(expr)

  /**
    * Returns true, if a String matches the specified LIKE pattern.
    *
    * e.g. "Jo_n%" matches all Strings that start with "Jo(arbitrary letter)n"
    */
  def like(pattern: Expression) = Like(expr, pattern)

  /**
    * Returns true, if a String matches the specified SQL regex pattern.
    *
    * e.g. "A+" matches all Strings that consist of at least one A
    */
  def similar(pattern: Expression) = Similar(expr, pattern)

  /**
    * Parses a date String in the form "yy-mm-dd" to a SQL Date.
    */
  def toDate = Cast(expr, SqlTimeTypeInfo.DATE)

  /**
    * Parses a time String in the form "hh:mm:ss" to a SQL Time.
    */
  def toTime = Cast(expr, SqlTimeTypeInfo.TIME)

    /**
    * Parses a timestamp String in the form "yy-mm-dd hh:mm:ss.fff" to a SQL Timestamp.
    */
  def toTimestamp = Cast(expr, SqlTimeTypeInfo.TIMESTAMP)
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

  implicit class LiteralByteExpression(b: Byte) extends ImplicitExpressionOperations {
    def expr = Literal(b)
  }

  implicit class LiteralShortExpression(s: Short) extends ImplicitExpressionOperations {
    def expr = Literal(s)
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

  implicit class LiteralJavaDecimalExpression(javaDecimal: java.math.BigDecimal)
      extends ImplicitExpressionOperations {
    def expr = Literal(javaDecimal)
  }

  implicit class LiteralScalaDecimalExpression(scalaDecimal: scala.math.BigDecimal)
      extends ImplicitExpressionOperations {
    def expr = Literal(scalaDecimal.bigDecimal)
  }

  implicit class LiteralSqlDateExpression(sqlDate: Date) extends ImplicitExpressionOperations {
    def expr = Literal(sqlDate)
  }

  implicit class LiteralSqlTimeExpression(sqlTime: Time) extends ImplicitExpressionOperations {
    def expr = Literal(sqlTime)
  }

  implicit class LiteralSqlTimestampExpression(sqlTimestamp: Timestamp)
      extends ImplicitExpressionOperations {
    def expr = Literal(sqlTimestamp)
  }

  implicit def symbol2FieldExpression(sym: Symbol): Expression = UnresolvedFieldReference(sym.name)
  implicit def byte2Literal(b: Byte): Expression = Literal(b)
  implicit def short2Literal(s: Short): Expression = Literal(s)
  implicit def int2Literal(i: Int): Expression = Literal(i)
  implicit def long2Literal(l: Long): Expression = Literal(l)
  implicit def double2Literal(d: Double): Expression = Literal(d)
  implicit def float2Literal(d: Float): Expression = Literal(d)
  implicit def string2Literal(str: String): Expression = Literal(str)
  implicit def boolean2Literal(bool: Boolean): Expression = Literal(bool)
  implicit def javaDec2Literal(javaDec: java.math.BigDecimal): Expression = Literal(javaDec)
  implicit def scalaDec2Literal(scalaDec: scala.math.BigDecimal): Expression =
    Literal(scalaDec.bigDecimal)
  implicit def sqlDate2Literal(sqlDate: Date): Expression = Literal(sqlDate)
  implicit def sqlTime2Literal(sqlTime: Time): Expression = Literal(sqlTime)
  implicit def sqlTimestamp2Literal(sqlTimestamp: Timestamp): Expression = Literal(sqlTimestamp)
}
