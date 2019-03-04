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
import org.apache.flink.table.api.{Table, TableException}
import org.apache.flink.table.expressions.ExpressionUtils.{convertArray, toMilliInterval, toMonthInterval, toRowInterval}
import org.apache.flink.table.expressions.TimeIntervalUnit.TimeIntervalUnit
import org.apache.flink.table.expressions.TimePointUnit.TimePointUnit
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{AggregateFunction, DistinctAggregateFunction, ScalarFunction, TableFunction}

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
    * If all values are null, null is returned.
    */
  def sum = Sum(expr)

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, 0 is returned.
    */
  def sum0 = Sum0(expr)

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
    * Returns the population standard deviation of an expression (the square root of varPop()).
    */
  def stddevPop = StddevPop(expr)

  /**
    * Returns the sample standard deviation of an expression (the square root of varSamp()).
    */
  def stddevSamp = StddevSamp(expr)

  /**
    * Returns the population standard variance of an expression.
    */
  def varPop = VarPop(expr)

  /**
    *  Returns the sample variance of a given expression.
    */
  def varSamp = VarSamp(expr)

  /**
    * Returns multiset aggregate of a given expression.
    */
  def collect = Collect(expr)

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

  /**
    * Specifies ascending order of an expression i.e. a field for orderBy call.
    *
    * @return ascend expression
    */
  def asc = Asc(expr)

  /**
    * Specifies descending order of an expression i.e. a field for orderBy call.
    *
    * @return descend expression
    */
  def desc = Desc(expr)

  /**
    * Returns true if an expression exists in a given list of expressions. This is a shorthand
    * for multiple OR conditions.
    *
    * If the testing set contains null, the result will be null if the element can not be found
    * and true if it can be found. If the element is null, the result is always null.
    *
    * e.g. "42".in(1, 2, 3) leads to false.
    */
  def in(elements: Expression*) = In(expr, elements)

  /**
    * Returns true if an expression exists in a given table sub-query. The sub-query table
    * must consist of one column. This column must have the same data type as the expression.
    *
    * Note: This operation is not supported in a streaming environment yet.
    */
  def in(table: Table) = In(expr, Seq(TableReference(table.toString, table)))

  /**
    * Returns the start time (inclusive) of a window when applied on a window reference.
    */
  def start = WindowStart(expr)

  /**
    * Returns the end time (exclusive) of a window when applied on a window reference.
    *
    * e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.
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
    * Calculates the base 10 logarithm of the given value.
    */
  def log10() = Log10(expr)

  /**
    * Calculates the base 2 logarithm of the given value.
    */
  def log2() = Log2(expr)

  /**
    * Calculates the natural logarithm of the given value.
    */
  def ln() = Ln(expr)

  /**
    * Calculates the natural logarithm of the given value.
    */
  def log() = Log(null, expr)

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def log(base: Expression) = Log(base, expr)

  /**
    * Calculates the given number raised to the power of the other value.
    */
  def power(other: Expression) = Power(expr, other)

  /**
    * Calculates the hyperbolic cosine of a given value.
    */
  def cosh() = Cosh(expr)

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
    * Calculates the hyperbolic sine of a given value.
    */
  def sinh() = Sinh(expr)

  /**
    * Calculates the smallest integer greater than or equal to a given number.
    */
  def ceil() = Ceil(expr)

  /**
    * Calculates the sine of a given number.
    */
  def sin() = Sin(expr)

  /**
    * Calculates the cosine of a given number.
    */
  def cos() = Cos(expr)

  /**
    * Calculates the tangent of a given number.
    */
  def tan() = Tan(expr)

  /**
    * Calculates the cotangent of a given number.
    */
  def cot() = Cot(expr)

  /**
    * Calculates the arc sine of a given number.
    */
  def asin() = Asin(expr)

  /**
    * Calculates the arc cosine of a given number.
    */
  def acos() = Acos(expr)

  /**
    * Calculates the arc tangent of a given number.
    */
  def atan() = Atan(expr)

  /**
    * Calculates the hyperbolic tangent of a given number.
    */
  def tanh() = Tanh(expr)

  /**
    * Converts numeric from radians to degrees.
    */
  def degrees() = Degrees(expr)

  /**
    * Converts numeric from degrees to radians.
    */
  def radians() = Radians(expr)

  /**
    * Calculates the signum of a given number.
    */
  def sign() = Sign(expr)

  /**
    * Rounds the given number to integer places right to the decimal point.
    */
  def round(places: Expression) = Round(expr, places)

  /**
    * Returns a string representation of an integer numeric value in binary format. Returns null if
    * numeric is null. E.g. "4" leads to "100", "12" leads to "1100".
    */
  def bin() = Bin(expr)

  /**
    * Returns a string representation of an integer numeric value or a string in hex format. Returns
    * null if numeric or string is null.
    *
    * E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world" leads
    * to "68656c6c6f2c776f726c64".
    */
  def hex() = Hex(expr)

  /**
    * Returns a number of truncated to n decimal places.
    * If n is 0,the result has no decimal point or fractional part.
    * n can be negative to cause n digits left of the decimal point of the value to become zero.
    * E.g. truncate(42.345, 2) to 42.34.
    */
  def truncate(n: Expression) = Truncate(expr, n)

  /**
    * Returns a number of truncated to 0 decimal places.
    * E.g. truncate(42.345) to 42.0.
    */
  def truncate() = Truncate(expr)

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
    * Returns a new string which replaces all the occurrences of the search target
    * with the replacement string (non-overlapping).
    */
  def replace(search: Expression, replacement: Expression) =
    Replace(expr, search, replacement)

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
    * Returns a string left-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".lpad(4, '??') returns "??hi",  "hi".lpad(1, '??') returns "h"
    */
  def lpad(len: Expression, pad: Expression) = Lpad(expr, len, pad)

  /**
    * Returns a string right-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".rpad(4, '??') returns "hi??",  "hi".rpad(1, '??') returns "h"
    */
  def rpad(len: Expression, pad: Expression) = Rpad(expr, len, pad)

  /**
    * For windowing function to config over window
    * e.g.:
    * table
    *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
    *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
    */
  def over(alias: Expression): Expression = {
    expr match {
      case _: Aggregation => UnresolvedOverCall(
        expr.asInstanceOf[Aggregation],
        alias)
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

  /**
    * Returns a string with all substrings that match the regular expression consecutively
    * being replaced.
    */
  def regexpReplace(regex: Expression, replacement: Expression) =
    RegexpReplace(expr, regex, replacement)

  /**
    * Returns a string extracted with a specified regular expression and a regex match group index.
    */
  def regexpExtract(regex: Expression, extractIndex: Expression) =
    RegexpExtract(expr, regex, extractIndex)

  /**
    * Returns a string extracted with a specified regular expression.
    */
  def regexpExtract(regex: Expression) =
    RegexpExtract(expr, regex, null)

  /**
    * Returns the base string decoded with base64.
    */
  def fromBase64() = FromBase64(expr)

  /**
    * Returns the base64-encoded result of the input string.
    */
  def toBase64() = ToBase64(expr)

  /**
    * Returns a string that removes the left whitespaces from the given string.
    */
  def ltrim() = LTrim(expr)

  /**
    * Returns a string that removes the right whitespaces from the given string.
    */
  def rtrim() = RTrim(expr)

  /**
    * Returns a string that repeats the base string n times.
    */
  def repeat(n: Expression) = Repeat(expr, n)

  // Temporal operations

  /**
    * Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
    */
  def toDate = Cast(expr, SqlTimeTypeInfo.DATE)

  /**
    * Parses a time string in the form "HH:mm:ss" to a SQL Time.
    */
  def toTime = Cast(expr, SqlTimeTypeInfo.TIME)

  /**
    * Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
    */
  def toTimestamp = Cast(expr, SqlTimeTypeInfo.TIMESTAMP)

  /**
    * Extracts parts of a time point or time interval. Returns the part as a long value.
    *
    * e.g. "2006-06-05".toDate.extract(DAY) leads to 5
    */
  def extract(timeIntervalUnit: TimeIntervalUnit) = Extract(timeIntervalUnit, expr)

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
  def year: Expression = toMonthInterval(expr, 12)

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def years: Expression = year

  /**
   * Creates an interval of the given number of quarters.
   *
   * @return interval of months
   */
  def quarter: Expression = toMonthInterval(expr, 3)

  /**
   * Creates an interval of the given number of quarters.
   *
   * @return interval of months
   */
  def quarters: Expression = quarter

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def month: Expression = toMonthInterval(expr, 1)

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def months: Expression = month

  /**
    * Creates an interval of the given number of weeks.
    *
    * @return interval of milliseconds
    */
  def week: Expression = toMilliInterval(expr, 7 * MILLIS_PER_DAY)

  /**
    * Creates an interval of the given number of weeks.
    *
    * @return interval of milliseconds
    */
  def weeks: Expression = week

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def day: Expression = toMilliInterval(expr, MILLIS_PER_DAY)

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def days: Expression = day

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hour: Expression = toMilliInterval(expr, MILLIS_PER_HOUR)

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hours: Expression = hour

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minute: Expression = toMilliInterval(expr, MILLIS_PER_MINUTE)

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minutes: Expression = minute

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def second: Expression = toMilliInterval(expr, MILLIS_PER_SECOND)

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def seconds: Expression = second

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def milli: Expression = toMilliInterval(expr, 1)

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def millis: Expression = milli

  // Row interval type

  /**
    * Creates an interval of rows.
    *
    * @return interval of rows
    */
  def rows: Expression = toRowInterval(expr)

  // Advanced type helper functions

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
    * Accesses the element of an array or map based on a key or an index (starting at 1).
    *
    * @param index key or position of the element (array index starting at 1)
    * @return value of the element
    */
  def at(index: Expression) = ItemAt(expr, index)

  /**
    * Returns the number of elements of an array or number of entries of a map.
    *
    * @return number of elements or entries
    */
  def cardinality() = Cardinality(expr)

  /**
    * Returns the sole element of an array with a single element. Returns null if the array is
    * empty. Throws an exception if the array has more than one element.
    *
    * @return the first and only element of an array with a single element
    */
  def element() = ArrayElement(expr)

  // Time definition

  /**
    * Declares a field as the rowtime attribute for indicating, accessing, and working in
    * Flink's event time.
    */
  def rowtime = RowtimeAttribute(expr)

  /**
    * Declares a field as the proctime attribute for indicating, accessing, and working in
    * Flink's processing time.
    */
  def proctime = ProctimeAttribute(expr)

  // Hash functions

  /**
    * Returns the MD5 hash of the string argument; null if string is null.
    *
    * @return string of 32 hexadecimal digits or null
    */
  def md5() = Md5(expr)

  /**
    * Returns the SHA-1 hash of the string argument; null if string is null.
    *
    * @return string of 40 hexadecimal digits or null
    */
  def sha1() = Sha1(expr)

  /**
    * Returns the SHA-224 hash of the string argument; null if string is null.
    *
    * @return string of 56 hexadecimal digits or null
    */
  def sha224() = Sha224(expr)

  /**
    * Returns the SHA-256 hash of the string argument; null if string is null.
    *
    * @return string of 64 hexadecimal digits or null
    */
  def sha256() = Sha256(expr)

  /**
    * Returns the SHA-384 hash of the string argument; null if string is null.
    *
    * @return string of 96 hexadecimal digits or null
    */
  def sha384() = Sha384(expr)

  /**
    * Returns the SHA-512 hash of the string argument; null if string is null.
    *
    * @return string of 128 hexadecimal digits or null
    */
  def sha512() = Sha512(expr)

  /**
    * Returns the hash for the given string expression using the SHA-2 family of hash
    * functions (SHA-224, SHA-256, SHA-384, or SHA-512).
    *
    * @param hashLength bit length of the result (either 224, 256, 384, or 512)
    * @return string or null if one of the arguments is null.
    */
  def sha2(hashLength: Expression) = Sha2(expr, hashLength)

  /**
    * Returns true if the given expression is between lowerBound and upperBound (both inclusive).
    * False otherwise. The parameters must be numeric types or identical comparable types.
    *
    * @param lowerBound numeric or comparable expression
    * @param upperBound numeric or comparable expression
    * @return boolean or null
    */
  def between(lowerBound: Expression, upperBound: Expression) =
    Between(expr, lowerBound, upperBound)

  /**
    * Returns true if the given expression is not between lowerBound and upperBound (both
    * inclusive). False otherwise. The parameters must be numeric types or identical
    * comparable types.
    *
    * @param lowerBound numeric or comparable expression
    * @param upperBound numeric or comparable expression
    * @return boolean or null
    */
  def notBetween(lowerBound: Expression, upperBound: Expression) =
    NotBetween(expr, lowerBound, upperBound)
}

/**
 * Implicit conversions from Scala Literals to Expression [[Literal]] and from [[Expression]]
 * to [[ImplicitExpressionOperations]].
 */
trait ImplicitExpressionConversions {

  /**
    * Offset constant to be used in the `preceding` clause of unbounded [[Over]] windows. Use this
    * constant for a time interval. Unbounded over windows start with the first row of a partition.
    */
  implicit val UNBOUNDED_ROW = UnboundedRow()

  /**
    * Offset constant to be used in the `preceding` clause of unbounded [[Over]] windows. Use this
    * constant for a row-count interval. Unbounded over windows start with the first row of a
    * partition.
    */
  implicit val UNBOUNDED_RANGE = UnboundedRange()

  /**
    * Offset constant to be used in the `following` clause of [[Over]] windows. Use this for setting
    * the upper bound of the window to the current row.
    */
  implicit val CURRENT_ROW = CurrentRow()

  /**
    * Offset constant to be used in the `following` clause of [[Over]] windows. Use this for setting
    * the upper bound of the window to the sort key of the current row, i.e., all rows with the same
    * sort key as the current row are included in the window.
    */
  implicit val CURRENT_RANGE = CurrentRange()

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

  implicit class ScalarFunctionCallExpression(val s: ScalarFunction) {
    def apply(params: Expression*): Expression = {
      ScalarFunctionCall(s, params)
    }
  }

  implicit class TableFunctionCallExpression[T: TypeInformation](val t: TableFunction[T]) {
    def apply(params: Expression*): TableFunctionCall = {
      val resultType = if (t.getResultType == null) {
        implicitly[TypeInformation[T]]
      } else {
        t.getResultType
      }
      TableFunctionCall(t.getClass.getCanonicalName, t, params, resultType)
    }
  }

  @deprecated("Please use Table.joinLateral() or Table.leftOuterJoinLateral() instead.", "1.8")
  implicit def tableFunctionCall2Table(tfc: TableFunctionCall): Table = {
    new Table(
      tableEnv = null, // table environment will be set later.
      tfc.toLogicalTableFunctionCall(child = null) // child will be set later.
    )
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
  implicit def userDefinedAggFunctionConstructor[T: TypeInformation, ACC: TypeInformation]
      (udagg: AggregateFunction[T, ACC]): UDAGGExpression[T, ACC] = UDAGGExpression(udagg)
  implicit def toDistinct(agg: Aggregation): DistinctAgg = DistinctAgg(agg)
  implicit def toDistinct[T: TypeInformation, ACC: TypeInformation]
      (agg: AggregateFunction[T, ACC]): DistinctAggregateFunction[T, ACC] =
    DistinctAggregateFunction(agg)
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
  * Formats a timestamp as a string using a specified format.
  * The format must be compatible with MySQL's date formatting syntax as used by the
  * date_parse function.
  *
  * For example <code>dataFormat('time, "%Y, %d %M")</code> results in strings
  * formatted as "2017, 05 May".
  */
object dateFormat {

  /**
    * Formats a timestamp as a string using a specified format.
    * The format must be compatible with MySQL's date formatting syntax as used by the
    * date_parse function.
    *
    * For example dataFormat('time, "%Y, %d %M") results in strings formatted as "2017, 05 May".
    *
    * @param timestamp The timestamp to format as string.
    * @param format The format of the string.
    * @return The formatted timestamp as string.
    */
  def apply(
    timestamp: Expression,
    format: Expression
  ): Expression = {
    DateFormat(timestamp, format)
  }
}

/**
  * Returns the (signed) number of [[TimePointUnit]] between timePoint1 and timePoint2.
  *
  * For example, timestampDiff(TimePointUnit.DAY, '2016-06-15'.toDate, '2016-06-18'.toDate leads
  * to 3.
  */
object timestampDiff {

  /**
    * Returns the (signed) number of [[TimePointUnit]] between timePoint1 and timePoint2.
    *
    * For example, timestampDiff(TimePointUnit.DAY, '2016-06-15'.toDate, '2016-06-18'.toDate leads
    * to 3.
    *
    * @param timePointUnit The unit to compute diff.
    * @param timePoint1 The first point in time.
    * @param timePoint2 The second point in time.
    * @return The number of intervals as integer value.
    */
  def apply(
      timePointUnit: TimePointUnit,
      timePoint1: Expression,
      timePoint2: Expression)
    : Expression = {
    TimestampDiff(timePointUnit, timePoint1, timePoint2)
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

/**
  * Creates a row of expressions.
  */
object row {

  /**
    * Creates a row of expressions.
    */
  def apply(head: Expression, tail: Expression*): Expression = {
    RowConstructor(head +: tail.toSeq)
  }
}

/**
  * Creates a map of expressions. The map will be a map between two objects (not primitives).
  */
object map {

  /**
    * Creates a map of expressions. The map will be a map between two objects (not primitives).
    */
  def apply(key: Expression, value: Expression, tail: Expression*): Expression = {
    MapConstructor(Seq(key, value) ++ tail.toSeq)
  }
}

/**
  * Returns a value that is closer than any other value to pi.
  */
object pi {

  /**
    * Returns a value that is closer than any other value to pi.
    */
  def apply(): Expression = {
    Pi()
  }
}

/**
  * Returns a value that is closer than any other value to e.
  */
object e {

  /**
    * Returns a value that is closer than any other value to e.
    */
  def apply(): Expression = {
    E()
  }
}

/**
  * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
  */
object rand {

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
    */
  def apply(): Expression = {
    new Rand()
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
    * initial seed. Two rand() functions will return identical sequences of numbers if they
    * have same initial seed.
    */
  def apply(seed: Expression): Expression = {
    Rand(seed)
  }
}

/**
  * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
  * value (exclusive).
  */
object randInteger {

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
    * value (exclusive).
    */
  def apply(bound: Expression): Expression = {
   new RandInteger(bound)
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
    * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
    * of numbers if they have same initial seed and same bound.
    */
  def apply(seed: Expression, bound: Expression): Expression = {
    RandInteger(seed, bound)
  }
}

/**
  * Returns the string that results from concatenating the arguments.
  * Returns NULL if any argument is NULL.
  */
object concat {

  /**
    * Returns the string that results from concatenating the arguments.
    * Returns NULL if any argument is NULL.
    */
  def apply(string: Expression, strings: Expression*): Expression = {
    Concat(Seq(string) ++ strings)
  }
}

/**
  * Calculates the arc tangent of a given coordinate.
  */
object atan2 {

  /**
    * Calculates the arc tangent of a given coordinate.
    */
  def apply(y: Expression, x: Expression): Expression = {
    Atan2(y, x)
  }
}

/**
  * Returns the string that results from concatenating the arguments and separator.
  * Returns NULL If the separator is NULL.
  *
  * Note: this user-defined function does not skip empty strings. However, it does skip any NULL
  * values after the separator argument.
  **/
object concat_ws {
  def apply(separator: Expression, string: Expression, strings: Expression*): Expression = {
    ConcatWs(separator, Seq(string) ++ strings)
  }
}

/**
  * Returns an UUID (Universally Unique Identifier) string (e.g.,
  * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
  * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
  * generator.
  */
object uuid {

  /**
    * Returns an UUID (Universally Unique Identifier) string (e.g.,
    * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
    * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
    * generator.
    */
  def apply(): Expression = {
    UUID()
  }
}

/**
  * Returns a null literal value of a given type.
  *
  * e.g. nullOf(Types.INT)
  */
object nullOf {

  /**
    * Returns a null literal value of a given type.
    *
    * e.g. nullOf(Types.INT)
    */
  def apply(typeInfo: TypeInformation[_]): Expression = {
    Null(typeInfo)
  }
}

/**
  * Calculates the logarithm of the given value.
  */
object log {

  /**
    * Calculates the natural logarithm of the given value.
    */
  def apply(value: Expression): Expression = {
    Log(null, value)
  }

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def apply(base: Expression, value: Expression): Expression = {
    Log(base, value)
  }
}

/**
  * Ternary conditional operator that decides which of two other expressions should be evaluated
  * based on a evaluated boolean condition.
  *
  * e.g. ifThenElse(42 > 5, "A", "B") leads to "A"
  */
object ifThenElse {

  /**
    * Ternary conditional operator that decides which of two other expressions should be evaluated
    * based on a evaluated boolean condition.
    *
    * e.g. ifThenElse(42 > 5, "A", "B") leads to "A"
    *
    * @param condition boolean condition
    * @param ifTrue expression to be evaluated if condition holds
    * @param ifFalse expression to be evaluated if condition does not hold
    */
  def apply(condition: Expression, ifTrue: Expression, ifFalse: Expression): Expression = {
    If(condition, ifTrue, ifFalse)
  }
}

// scalastyle:on object.name
