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
package org.apache.flink.table.api.scala

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}

import org.apache.calcite.avatica.util.DateTimeUtils._
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.ExpressionUtils.{convertArray, toMilliInterval, toMonthInterval, toRowInterval}
import org.apache.flink.table.expressions.TimeIntervalUnit.TimeIntervalUnit
import org.apache.flink.table.expressions._

import scala.language.implicitConversions

/**
 * These are all the operations that can be used to construct an [[Expression]] AST for expression
 * operations.
 *
 * These operations must be kept in sync with the parser in
 * [[org.apache.flink.table.expressions.ExpressionParser]].
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

  /**
    * Boolean AND in three-valued logic.
    */
  def && (other: Expression) = And(expr, other)

  /**
    * Boolean OR in three-valued logic.
    */
  def || (other: Expression) = Or(expr, other)

  /**
    * Greater than.
    */
  def > (other: Expression) = GreaterThan(expr, other)

  /**
    * Greater than or equal.
    */
  def >= (other: Expression) = GreaterThanOrEqual(expr, other)

  /**
    * Less than.
    */
  def < (other: Expression) = LessThan(expr, other)

  /**
    * Less than or equal.
    */
  def <= (other: Expression) = LessThanOrEqual(expr, other)

  /**
    * Equals.
    */
  def === (other: Expression) = EqualTo(expr, other)

  /**
    * Not equal.
    */
  def !== (other: Expression) = NotEqualTo(expr, other)

  /**
    * Whether boolean expression is not true; returns null if boolean is null.
    */
  def unary_! = Not(expr)

  /**
    * Returns negative numeric.
    */
  def unary_- = UnaryMinus(expr)

  /**
    * Returns numeric.
    */
  def unary_+ = expr

  /**
    * Returns true if the given expression is null.
    */
  def isNull = IsNull(expr)

  /**
    * Returns true if the given expression is not null.
    */
  def isNotNull = IsNotNull(expr)

  /**
    * Returns true if given boolean expression is true. False otherwise (for null and false).
    */
  def isTrue = IsTrue(expr)

  /**
    * Returns true if given boolean expression is false. False otherwise (for null and true).
    */
  def isFalse = IsFalse(expr)

  /**
    * Returns true if given boolean expression is not true (for null and false). False otherwise.
    */
  def isNotTrue = IsNotTrue(expr)

  /**
    * Returns true if given boolean expression is not false (for null and true). False otherwise.
    */
  def isNotFalse = IsNotFalse(expr)

  /**
    * Returns left plus right.
    */
  def + (other: Expression) = Plus(expr, other)

  /**
    * Returns left minus right.
    */
  def - (other: Expression) = Minus(expr, other)

  /**
    * Returns left divided by right.
    */
  def / (other: Expression) = Div(expr, other)

  /**
    * Returns left multiplied by right.
    */
  def * (other: Expression) = Mul(expr, other)

  /**
    * Returns the remainder (modulus) of left divided by right.
    * The result is negative only if left is negative.
    */
  def % (other: Expression) = mod(other)

  /**
    * Returns the sum of the numeric field across all input values.
    */
  def sum = Sum(expr)

  /**
    * Returns the minimum value of field across all input values.
    */
  def min = Min(expr)

  /**
    * Returns the maximum value of field across all input values.
    */
  def max = Max(expr)

  /**
    * Returns the number of input rows for which the field is not null.
    */
  def count = Count(expr)

  /**
    * Returns the average (arithmetic mean) of the numeric field across all input values.
    */
  def avg = Avg(expr)

  /**
    * Converts a value to a given type.
    *
    * e.g. "42".cast(Types.INT) leads to 42.
    *
    * @return casted expression
    */
  def cast(toType: TypeInformation[_]) = Cast(expr, toType)

  /**
    * Specifies a name for an expression i.e. a field.
    *
    * @param name name for one field
    * @param extraNames additional names if the expression expands to multiple fields
    * @return field with an alias
    */
  def as(name: Symbol, extraNames: Symbol*) = Alias(expr, name.name, extraNames.map(_.name))

  def asc = Asc(expr)
  def desc = Desc(expr)

  /**
    * Returns the start time of a window when applied on a window reference.
    */
  def start = WindowStart(expr)

  /**
    * Returns the end time of a window when applied on a window reference.
    */
  def end = WindowEnd(expr)

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
    * Calculates the square root of a given value.
    */
  def sqrt() = Sqrt(expr)

  /**
    * Calculates the absolute value of given value.
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

  // String operations

  /**
    * Creates a substring of the given string at given index for a given length.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @param length number of characters of the substring
    * @return substring
    */
  def substring(beginIndex: Expression, length: Expression) =
    Substring(expr, beginIndex, length)

  /**
    * Creates a substring of the given string beginning at the given index to the end.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression) =
    new Substring(expr, beginIndex)

  /**
    * Removes leading and/or trailing characters from the given string.
    *
    * @param removeLeading if true, remove leading characters (default: true)
    * @param removeTrailing if true, remove trailing characters (default: true)
    * @param character string containing the character (default: " ")
    * @return trimmed string
    */
  def trim(
      removeLeading: Boolean = true,
      removeTrailing: Boolean = true,
      character: Expression = TrimConstants.TRIM_DEFAULT_CHAR) = {
    if (removeLeading && removeTrailing) {
      Trim(TrimMode.BOTH, character, expr)
    } else if (removeLeading) {
      Trim(TrimMode.LEADING, character, expr)
    } else if (removeTrailing) {
      Trim(TrimMode.TRAILING, character, expr)
    } else {
      expr
    }
  }

  /**
    * Returns the length of a string.
    */
  def charLength() = CharLength(expr)

  /**
    * Returns all of the characters in a string in upper case using the rules of
    * the default locale.
    */
  def upperCase() = Upper(expr)

  /**
    * Returns all of the characters in a string in lower case using the rules of
    * the default locale.
    */
  def lowerCase() = Lower(expr)

  /**
    * Converts the initial letter of each word in a string to uppercase.
    * Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.
    */
  def initCap() = InitCap(expr)

  /**
    * Returns true, if a string matches the specified LIKE pattern.
    *
    * e.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n"
    */
  def like(pattern: Expression) = Like(expr, pattern)

  /**
    * Returns true, if a string matches the specified SQL regex pattern.
    *
    * e.g. "A+" matches all strings that consist of at least one A
    */
  def similar(pattern: Expression) = Similar(expr, pattern)

  /**
    * Returns the position of string in an other string starting at 1.
    * Returns 0 if string could not be found.
    *
    * e.g. "a".position("bbbbba") leads to 6
    */
  def position(haystack: Expression) = Position(expr, haystack)

  /**
    * For windowing function to config over window
    * e.g.:
    * table
    *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
    *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
    */
  def
  over(alias: Expression) = {
    expr match {
      case _: Aggregation => OverCall(expr.asInstanceOf[Aggregation], alias)
      case _ => throw new TableException(
        "The over method can only using with aggregation expression.")
    }
  }

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6) leads to "xxxxxxxxx"
    */
  def overlay(newString: Expression, starting: Expression) = new Overlay(expr, newString, starting)

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    * The length specifies how many characters should be removed.
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6, 2) leads to "xxxxxxxxxst"
    */
  def overlay(newString: Expression, starting: Expression, length: Expression) =
    Overlay(expr, newString, starting, length)

  // Temporal operations

  /**
    * Parses a date string in the form "yy-mm-dd" to a SQL Date.
    */
  def toDate = Cast(expr, SqlTimeTypeInfo.DATE)

  /**
    * Parses a time string in the form "hh:mm:ss" to a SQL Time.
    */
  def toTime = Cast(expr, SqlTimeTypeInfo.TIME)

  /**
    * Parses a timestamp string in the form "yy-mm-dd hh:mm:ss.fff" to a SQL Timestamp.
    */
  def toTimestamp = Cast(expr, SqlTimeTypeInfo.TIMESTAMP)

  /**
    * Extracts parts of a time point or time interval. Returns the part as a long value.
    *
    * e.g. "2006-06-05".toDate.extract(DAY) leads to 5
    */
  def extract(timeIntervalUnit: TimeIntervalUnit) = Extract(timeIntervalUnit, expr)

  /**
    * Returns the quarter of a year from a SQL date.
    *
    * e.g. "1994-09-27".toDate.quarter() leads to 3
    */
  def quarter() = Quarter(expr)

  /**
    * Rounds down a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.floor(MINUTE) leads to 12:44:00
    */
  def floor(timeIntervalUnit: TimeIntervalUnit) = TemporalFloor(timeIntervalUnit, expr)

  /**
    * Rounds up a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.ceil(MINUTE) leads to 12:45:00
    */
  def ceil(timeIntervalUnit: TimeIntervalUnit) = TemporalCeil(timeIntervalUnit, expr)

  // Interval types

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def year = toMonthInterval(expr, 12)

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def years = year

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def month = toMonthInterval(expr, 1)

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def months = month

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def day = toMilliInterval(expr, MILLIS_PER_DAY)

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def days = day

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hour = toMilliInterval(expr, MILLIS_PER_HOUR)

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hours = hour

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minute = toMilliInterval(expr, MILLIS_PER_MINUTE)

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minutes = minute

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def second = toMilliInterval(expr, MILLIS_PER_SECOND)

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def seconds = second

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def milli = toMilliInterval(expr, 1)

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def millis = milli

  // row interval type

  /**
    * Creates an interval of rows.
    *
    * @return interval of rows
    */
  def rows = toRowInterval(expr)

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name and
    * returns it's value.
    *
    * @param name name of the field (similar to Flink's field expressions)
    * @return value of the field
    */
  def get(name: String) = GetCompositeField(expr, name)

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index and
    * returns it's value.
    *
    * @param index position of the field
    * @return value of the field
    */
  def get(index: Int) = GetCompositeField(expr, index)

  /**
    * Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
    * into a flat representation where every subtype is a separate field.
    */
  def flatten() = Flattening(expr)

  /**
    * Accesses the element of an array based on an index (starting at 1).
    *
    * @param index position of the element (starting at 1)
    * @return value of the element
    */
  def at(index: Expression) = ArrayElementAt(expr, index)

  /**
    * Returns the number of elements of an array.
    *
    * @return number of elements
    */
  def cardinality() = ArrayCardinality(expr)

  /**
    * Returns the sole element of an array with a single element. Returns null if the array is
    * empty. Throws an exception if the array has more than one element.
    *
    * @return the first and only element of an array with a single element
    */
  def element() = ArrayElement(expr)
}

/**
 * Implicit conversions from Scala Literals to Expression [[Literal]] and from [[Expression]]
 * to [[ImplicitExpressionOperations]].
 */
trait ImplicitExpressionConversions {

  implicit val UNBOUNDED_ROW = toRowInterval(Long.MaxValue)
  implicit val UNBOUNDED_RANGE = toMilliInterval(1, Long.MaxValue)

  implicit val CURRENT_ROW = toRowInterval(0L)
  implicit val CURRENT_RANGE = toMilliInterval(-1L, 1)

  implicit class WithOperations(e: Expression) extends ImplicitExpressionOperations {
    def expr = e
  }

  implicit class UnresolvedFieldExpression(s: Symbol) extends ImplicitExpressionOperations {
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
  implicit def javaDec2Literal(javaDec: JBigDecimal): Expression = Literal(javaDec)
  implicit def scalaDec2Literal(scalaDec: BigDecimal): Expression =
    Literal(scalaDec.bigDecimal)
  implicit def sqlDate2Literal(sqlDate: Date): Expression = Literal(sqlDate)
  implicit def sqlTime2Literal(sqlTime: Time): Expression = Literal(sqlTime)
  implicit def sqlTimestamp2Literal(sqlTimestamp: Timestamp): Expression =
    Literal(sqlTimestamp)
  implicit def array2ArrayConstructor(array: Array[_]): Expression = convertArray(array)
}

// ------------------------------------------------------------------------------------------------
// Expressions with no parameters
// ------------------------------------------------------------------------------------------------

// we disable the object checker here as it checks for capital letters of objects
// but we want that objects look like functions in certain cases e.g. array(1, 2, 3)
// scalastyle:off object.name

/**
  * Returns the current SQL date in UTC time zone.
  */
object currentDate {

  /**
    * Returns the current SQL date in UTC time zone.
    */
  def apply(): Expression = {
    CurrentDate()
  }
}

/**
  * Returns the current SQL time in UTC time zone.
  */
object currentTime {

  /**
    * Returns the current SQL time in UTC time zone.
    */
  def apply(): Expression = {
    CurrentTime()
  }
}

/**
  * Returns the current SQL timestamp in UTC time zone.
  */
object currentTimestamp {

  /**
    * Returns the current SQL timestamp in UTC time zone.
    */
  def apply(): Expression = {
    CurrentTimestamp()
  }
}

/**
  * Returns the current SQL time in local time zone.
  */
object localTime {

  /**
    * Returns the current SQL time in local time zone.
    */
  def apply(): Expression = {
    LocalTime()
  }
}

/**
  * Returns the current SQL timestamp in local time zone.
  */
object localTimestamp {

  /**
    * Returns the current SQL timestamp in local time zone.
    */
  def apply(): Expression = {
    LocalTimestamp()
  }
}

/**
  * Determines whether two anchored time intervals overlap. Time point and temporal are
  * transformed into a range defined by two time points (start, end). The function
  * evaluates <code>leftEnd >= rightStart && rightEnd >= leftStart</code>.
  *
  * It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
  *
  * e.g. temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour) leads to true
  */
object temporalOverlaps {

  /**
    * Determines whether two anchored time intervals overlap. Time point and temporal are
    * transformed into a range defined by two time points (start, end).
    *
    * It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
    *
    * e.g. temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour) leads to true
    */
  def apply(
      leftTimePoint: Expression,
      leftTemporal: Expression,
      rightTimePoint: Expression,
      rightTemporal: Expression): Expression = {
    TemporalOverlaps(leftTimePoint, leftTemporal, rightTimePoint, rightTemporal)
  }
}

/**
  * Creates an array of literals. The array will be an array of objects (not primitives).
  */
object array {

  /**
    * Creates an array of literals. The array will be an array of objects (not primitives).
    */
  def apply(head: Expression, tail: Expression*): Expression = {
    ArrayConstructor(head +: tail.toSeq)
  }
}

// scalastyle:on object.name
