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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.utils.ApiExpressionUtils._
import org.apache.flink.table.functions.BuiltInFunctionDefinitions._
import org.apache.flink.table.functions.{ScalarFunction, TableFunction, UserDefinedAggregateFunction, UserFunctionsTypeHelper, _}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

import _root_.scala.language.implicitConversions

/**
  * These are all the operations that can be used to construct an [[Expression]] AST for
  * expression operations.
  *
  * These operations must be kept in sync with the parser in
  * [[org.apache.flink.table.expressions.ExpressionParser]].
  */
@PublicEvolving
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
  def && (other: Expression): Expression = unresolvedCall(AND, expr, other)

  /**
    * Boolean OR in three-valued logic.
    */
  def || (other: Expression): Expression = unresolvedCall(OR, expr, other)

  /**
    * Greater than.
    */
  def > (other: Expression): Expression = unresolvedCall(GREATER_THAN, expr, other)

  /**
    * Greater than or equal.
    */
  def >= (other: Expression): Expression = unresolvedCall(GREATER_THAN_OR_EQUAL, expr, other)

  /**
    * Less than.
    */
  def < (other: Expression): Expression = unresolvedCall(LESS_THAN, expr, other)

  /**
    * Less than or equal.
    */
  def <= (other: Expression): Expression = unresolvedCall(LESS_THAN_OR_EQUAL, expr, other)

  /**
    * Equals.
    */
  def === (other: Expression): Expression = unresolvedCall(EQUALS, expr, other)

  /**
    * Not equal.
    */
  def !== (other: Expression): Expression = unresolvedCall(NOT_EQUALS, expr, other)

  /**
    * Returns true if the given expression is between lowerBound and upperBound (both inclusive).
    * False otherwise. The parameters must be numeric types or identical comparable types.
    *
    * @param lowerBound numeric or comparable expression
    * @param upperBound numeric or comparable expression
    * @return boolean or null
    */
  def between(lowerBound: Expression, upperBound: Expression): Expression =
    unresolvedCall(BETWEEN, expr, lowerBound, upperBound)

  /**
    * Returns true if the given expression is not between lowerBound and upperBound (both
    * inclusive). False otherwise. The parameters must be numeric types or identical
    * comparable types.
    *
    * @param lowerBound numeric or comparable expression
    * @param upperBound numeric or comparable expression
    * @return boolean or null
    */
  def notBetween(lowerBound: Expression, upperBound: Expression): Expression =
    unresolvedCall(NOT_BETWEEN, expr, lowerBound, upperBound)

  /**
    * Whether boolean expression is not true; returns null if boolean is null.
    */
  def unary_! : Expression = unresolvedCall(NOT, expr)

  /**
    * Returns negative numeric.
    */
  def unary_- : Expression = unresolvedCall(MINUS_PREFIX, expr)

  /**
    * Returns numeric.
    */
  def unary_+ : Expression = expr

  /**
    * Returns true if the given expression is null.
    */
  def isNull: Expression = unresolvedCall(IS_NULL, expr)

  /**
    * Returns true if the given expression is not null.
    */
  def isNotNull: Expression = unresolvedCall(IS_NOT_NULL, expr)

  /**
    * Returns true if given boolean expression is true. False otherwise (for null and false).
    */
  def isTrue: Expression = unresolvedCall(IS_TRUE, expr)

  /**
    * Returns true if given boolean expression is false. False otherwise (for null and true).
    */
  def isFalse: Expression = unresolvedCall(IS_FALSE, expr)

  /**
    * Returns true if given boolean expression is not true (for null and false). False otherwise.
    */
  def isNotTrue: Expression = unresolvedCall(IS_NOT_TRUE, expr)

  /**
    * Returns true if given boolean expression is not false (for null and true). False otherwise.
    */
  def isNotFalse: Expression = unresolvedCall(IS_NOT_FALSE, expr)

  /**
    * Returns left plus right.
    */
  def + (other: Expression): Expression = unresolvedCall(PLUS, expr, other)

  /**
    * Returns left minus right.
    */
  def - (other: Expression): Expression = unresolvedCall(MINUS, expr, other)

  /**
    * Returns left divided by right.
    */
  def / (other: Expression): Expression = unresolvedCall(DIVIDE, expr, other)

  /**
    * Returns left multiplied by right.
    */
  def * (other: Expression): Expression = unresolvedCall(TIMES, expr, other)

  /**
    * Returns the remainder (modulus) of left divided by right.
    * The result is negative only if left is negative.
    */
  def % (other: Expression): Expression = mod(other)

  /**
    * Indicates the range from left to right, i.e. [left, right], which can be used in columns
    * selection.
    *
    * e.g. withColumns(1 to 3)
    */
  def to (other: Expression): Expression = unresolvedCall(RANGE_TO, expr, other)

  /**
    * Similar to a SQL distinct aggregation clause such as COUNT(DISTINCT a), declares that an
    * aggregation function is only applied on distinct input values.
    *
    * For example:
    *
    * {{{
    * orders
    *   .groupBy('a)
    *   .select('a, 'b.sum.distinct as 'd)
    * }}}
    */
  def distinct: Expression = unresolvedCall(DISTINCT, expr)

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, null is returned.
    */
  def sum: Expression = unresolvedCall(SUM, expr)

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, 0 is returned.
    */
  def sum0: Expression = unresolvedCall(SUM0, expr)

  /**
    * Returns the minimum value of field across all input values.
    */
  def min: Expression = unresolvedCall(MIN, expr)

  /**
    * Returns the maximum value of field across all input values.
    */
  def max: Expression = unresolvedCall(MAX, expr)

  /**
    * Returns the number of input rows for which the field is not null.
    */
  def count: Expression = unresolvedCall(COUNT, expr)

  /**
    * Returns the average (arithmetic mean) of the numeric field across all input values.
    */
  def avg: Expression = unresolvedCall(AVG, expr)

  /**
    * Returns the population standard deviation of an expression (the square root of varPop()).
    */
  def stddevPop: Expression = unresolvedCall(STDDEV_POP, expr)

  /**
    * Returns the sample standard deviation of an expression (the square root of varSamp()).
    */
  def stddevSamp: Expression = unresolvedCall(STDDEV_SAMP, expr)

  /**
    * Returns the population standard variance of an expression.
    */
  def varPop: Expression = unresolvedCall(VAR_POP, expr)

  /**
    *  Returns the sample variance of a given expression.
    */
  def varSamp: Expression = unresolvedCall(VAR_SAMP, expr)

  /**
    * Returns multiset aggregate of a given expression.
    */
  def collect: Expression = unresolvedCall(COLLECT, expr)

  /**
    * Converts a value to a given data type.
    *
    * e.g. "42".cast(DataTypes.INT()) leads to 42.
    *
    * @return casted expression
    */
  def cast(toType: DataType): Expression =
    unresolvedCall(CAST, expr, typeLiteral(toType))

  /**
    * @deprecated This method will be removed in future versions as it uses the old type system. It
    *             is recommended to use [[cast(DataType)]] instead which uses the new type system
    *             based on [[DataTypes]]. Please make sure to use either the old or the new type
    *             system consistently to avoid unintended behavior. See the website documentation
    *             for more information.
    */
  @deprecated
  def cast(toType: TypeInformation[_]): Expression =
    unresolvedCall(CAST, expr, typeLiteral(fromLegacyInfoToDataType(toType)))

  /**
    * Specifies a name for an expression i.e. a field.
    *
    * @param name name for one field
    * @param extraNames additional names if the expression expands to multiple fields
    * @return field with an alias
    */
  def as(name: Symbol, extraNames: Symbol*): Expression =
    unresolvedCall(
      AS,
      expr +: valueLiteral(name.name) +: extraNames.map(name => valueLiteral(name.name)): _*)

  /**
    * Specifies ascending order of an expression i.e. a field for orderBy call.
    *
    * @return ascend expression
    */
  def asc: Expression = unresolvedCall(ORDER_ASC, expr)

  /**
    * Specifies descending order of an expression i.e. a field for orderBy call.
    *
    * @return descend expression
    */
  def desc: Expression = unresolvedCall(ORDER_DESC, expr)

  /**
    * Returns true if an expression exists in a given list of expressions. This is a shorthand
    * for multiple OR conditions.
    *
    * If the testing set contains null, the result will be null if the element can not be found
    * and true if it can be found. If the element is null, the result is always null.
    *
    * e.g. "42".in(1, 2, 3) leads to false.
    */
  def in(elements: Expression*): Expression = unresolvedCall(IN, expr +: elements: _*)

  /**
    * Returns true if an expression exists in a given table sub-query. The sub-query table
    * must consist of one column. This column must have the same data type as the expression.
    *
    * Note: This operation is not supported in a streaming environment yet.
    */
  def in(table: Table): Expression = unresolvedCall(IN, expr, tableRef(table.toString, table))

  /**
    * Returns the start time (inclusive) of a window when applied on a window reference.
    */
  def start: Expression = unresolvedCall(WINDOW_START, expr)

  /**
    * Returns the end time (exclusive) of a window when applied on a window reference.
    *
    * e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.
    */
  def end: Expression = unresolvedCall(WINDOW_END, expr)

  /**
    * Ternary conditional operator that decides which of two other expressions should be
    * based on a evaluated boolean condition.
    *
    * e.g. (42 > 5).?("A", "B") leads to "A"
    *
    * @param ifTrue expression to be evaluated if condition holds
    * @param ifFalse expression to be evaluated if condition does not hold
    */
  def ?(ifTrue: Expression, ifFalse: Expression): Expression =
    unresolvedCall(IF, expr, ifTrue, ifFalse)

  // scalar functions

  /**
    * Calculates the remainder of division the given number by another one.
    */
  def mod(other: Expression): Expression = unresolvedCall(MOD, expr, other)

  /**
    * Calculates the Euler's number raised to the given power.
    */
  def exp(): Expression = unresolvedCall(EXP, expr)

  /**
    * Calculates the base 10 logarithm of the given value.
    */
  def log10(): Expression = unresolvedCall(LOG10, expr)

  /**
    * Calculates the base 2 logarithm of the given value.
    */
  def log2(): Expression = unresolvedCall(LOG2, expr)

  /**
    * Calculates the natural logarithm of the given value.
    */
  def ln(): Expression = unresolvedCall(LN, expr)

  /**
    * Calculates the natural logarithm of the given value.
    */
  def log(): Expression = unresolvedCall(LOG, expr)

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def log(base: Expression): Expression = unresolvedCall(LOG, base, expr)

  /**
    * Calculates the given number raised to the power of the other value.
    */
  def power(other: Expression): Expression = unresolvedCall(POWER, expr, other)

  /**
    * Calculates the hyperbolic cosine of a given value.
    */
  def cosh(): Expression = unresolvedCall(COSH, expr)

  /**
    * Calculates the square root of a given value.
    */
  def sqrt(): Expression = unresolvedCall(SQRT, expr)

  /**
    * Calculates the absolute value of given value.
    */
  def abs(): Expression = unresolvedCall(ABS, expr)

  /**
    * Calculates the largest integer less than or equal to a given number.
    */
  def floor(): Expression = unresolvedCall(FLOOR, expr)

  /**
    * Calculates the hyperbolic sine of a given value.
    */
  def sinh(): Expression = unresolvedCall(SINH, expr)

  /**
    * Calculates the smallest integer greater than or equal to a given number.
    */
  def ceil(): Expression = unresolvedCall(CEIL, expr)

  /**
    * Calculates the sine of a given number.
    */
  def sin(): Expression = unresolvedCall(SIN, expr)

  /**
    * Calculates the cosine of a given number.
    */
  def cos(): Expression = unresolvedCall(COS, expr)

  /**
    * Calculates the tangent of a given number.
    */
  def tan(): Expression = unresolvedCall(TAN, expr)

  /**
    * Calculates the cotangent of a given number.
    */
  def cot(): Expression = unresolvedCall(COT, expr)

  /**
    * Calculates the arc sine of a given number.
    */
  def asin(): Expression = unresolvedCall(ASIN, expr)

  /**
    * Calculates the arc cosine of a given number.
    */
  def acos(): Expression = unresolvedCall(ACOS, expr)

  /**
    * Calculates the arc tangent of a given number.
    */
  def atan(): Expression = unresolvedCall(ATAN, expr)

  /**
    * Calculates the hyperbolic tangent of a given number.
    */
  def tanh(): Expression = unresolvedCall(TANH, expr)

  /**
    * Converts numeric from radians to degrees.
    */
  def degrees(): Expression = unresolvedCall(DEGREES, expr)

  /**
    * Converts numeric from degrees to radians.
    */
  def radians(): Expression = unresolvedCall(RADIANS, expr)

  /**
    * Calculates the signum of a given number.
    */
  def sign(): Expression = unresolvedCall(SIGN, expr)

  /**
    * Rounds the given number to integer places right to the decimal point.
    */
  def round(places: Expression): Expression = unresolvedCall(ROUND, expr, places)

  /**
    * Returns a string representation of an integer numeric value in binary format. Returns null if
    * numeric is null. E.g. "4" leads to "100", "12" leads to "1100".
    */
  def bin(): Expression = unresolvedCall(BIN, expr)

  /**
    * Returns a string representation of an integer numeric value or a string in hex format. Returns
    * null if numeric or string is null.
    *
    * E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world" leads
    * to "68656c6c6f2c776f726c64".
    */
  def hex(): Expression = unresolvedCall(HEX, expr)

  /**
    * Returns a number of truncated to n decimal places.
    * If n is 0,the result has no decimal point or fractional part.
    * n can be negative to cause n digits left of the decimal point of the value to become zero.
    * E.g. truncate(42.345, 2) to 42.34.
    */
  def truncate(n: Expression): Expression = unresolvedCall(TRUNCATE, expr, n)

  /**
    * Returns a number of truncated to 0 decimal places.
    * E.g. truncate(42.345) to 42.0.
    */
  def truncate(): Expression = unresolvedCall(TRUNCATE, expr)

  // String operations

  /**
    * Creates a substring of the given string at given index for a given length.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @param length number of characters of the substring
    * @return substring
    */
  def substring(beginIndex: Expression, length: Expression): Expression =
    unresolvedCall(SUBSTRING, expr, beginIndex, length)

  /**
    * Creates a substring of the given string beginning at the given index to the end.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression): Expression =
    unresolvedCall(SUBSTRING, expr, beginIndex)

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
      character: Expression = valueLiteral(" "))
    : Expression = {
    unresolvedCall(TRIM, valueLiteral(removeLeading), valueLiteral(removeTrailing), character, expr)
  }

  /**
    * Returns a new string which replaces all the occurrences of the search target
    * with the replacement string (non-overlapping).
    */
  def replace(search: Expression, replacement: Expression): Expression =
    unresolvedCall(REPLACE, expr, search, replacement)

  /**
    * Returns the length of a string.
    */
  def charLength(): Expression = unresolvedCall(CHAR_LENGTH, expr)

  /**
    * Returns all of the characters in a string in upper case using the rules of
    * the default locale.
    */
  def upperCase(): Expression = unresolvedCall(UPPER, expr)

  /**
    * Returns all of the characters in a string in lower case using the rules of
    * the default locale.
    */
  def lowerCase(): Expression = unresolvedCall(LOWER, expr)

  /**
    * Converts the initial letter of each word in a string to uppercase.
    * Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.
    */
  def initCap(): Expression = unresolvedCall(INIT_CAP, expr)

  /**
    * Returns true, if a string matches the specified LIKE pattern.
    *
    * e.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n"
    */
  def like(pattern: Expression): Expression = unresolvedCall(LIKE, expr, pattern)

  /**
    * Returns true, if a string matches the specified SQL regex pattern.
    *
    * e.g. "A+" matches all strings that consist of at least one A
    */
  def similar(pattern: Expression): Expression = unresolvedCall(SIMILAR, expr, pattern)

  /**
    * Returns the position of string in an other string starting at 1.
    * Returns 0 if string could not be found.
    *
    * e.g. "a".position("bbbbba") leads to 6
    */
  def position(haystack: Expression): Expression = unresolvedCall(POSITION, expr, haystack)

  /**
    * Returns a string left-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".lpad(4, '??') returns "??hi",  "hi".lpad(1, '??') returns "h"
    */
  def lpad(len: Expression, pad: Expression): Expression = unresolvedCall(LPAD, expr, len, pad)

  /**
    * Returns a string right-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".rpad(4, '??') returns "hi??",  "hi".rpad(1, '??') returns "h"
    */
  def rpad(len: Expression, pad: Expression): Expression = unresolvedCall(RPAD, expr, len, pad)

  /**
    * Defines an aggregation to be used for a previously specified over window.
    *
    * For example:
    *
    * {{{
    * table
    *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
    *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
    * }}}
    */
  def over(alias: Expression): Expression = unresolvedCall(OVER, expr, alias)

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6) leads to "xxxxxxxxx"
    */
  def overlay(newString: Expression, starting: Expression): Expression =
    unresolvedCall(OVERLAY, expr, newString, starting)

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    * The length specifies how many characters should be removed.
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6, 2) leads to "xxxxxxxxxst"
    */
  def overlay(newString: Expression, starting: Expression, length: Expression): Expression =
    unresolvedCall(OVERLAY, expr, newString, starting, length)

  /**
    * Returns a string with all substrings that match the regular expression consecutively
    * being replaced.
    */
  def regexpReplace(regex: Expression, replacement: Expression): Expression =
    unresolvedCall(REGEXP_REPLACE, expr, regex, replacement)

  /**
    * Returns a string extracted with a specified regular expression and a regex match group
    * index.
    */
  def regexpExtract(regex: Expression, extractIndex: Expression): Expression =
    unresolvedCall(REGEXP_EXTRACT, expr, regex, extractIndex)

  /**
    * Returns a string extracted with a specified regular expression.
    */
  def regexpExtract(regex: Expression): Expression =
    unresolvedCall(REGEXP_EXTRACT, expr, regex)

  /**
    * Returns the base string decoded with base64.
    */
  def fromBase64(): Expression = unresolvedCall(FROM_BASE64, expr)

  /**
    * Returns the base64-encoded result of the input string.
    */
  def toBase64(): Expression = unresolvedCall(TO_BASE64, expr)

  /**
    * Returns a string that removes the left whitespaces from the given string.
    */
  def ltrim(): Expression = unresolvedCall(LTRIM, expr)

  /**
    * Returns a string that removes the right whitespaces from the given string.
    */
  def rtrim(): Expression = unresolvedCall(RTRIM, expr)

  /**
    * Returns a string that repeats the base string n times.
    */
  def repeat(n: Expression): Expression = unresolvedCall(REPEAT, expr, n)

  // Temporal operations

  /**
    * Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
    */
  def toDate: Expression =
    unresolvedCall(CAST, expr, typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.DATE)))

  /**
    * Parses a time string in the form "HH:mm:ss" to a SQL Time.
    */
  def toTime: Expression =
    unresolvedCall(CAST, expr, typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIME)))

  /**
    * Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
    */
  def toTimestamp: Expression =
    unresolvedCall(CAST, expr, typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIMESTAMP)))

  /**
    * Extracts parts of a time point or time interval. Returns the part as a long value.
    *
    * e.g. "2006-06-05".toDate.extract(DAY) leads to 5
    */
  def extract(timeIntervalUnit: TimeIntervalUnit): Expression =
    unresolvedCall(EXTRACT, valueLiteral(timeIntervalUnit), expr)

  /**
    * Rounds down a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.floor(MINUTE) leads to 12:44:00
    */
  def floor(timeIntervalUnit: TimeIntervalUnit): Expression =
    unresolvedCall(FLOOR, valueLiteral(timeIntervalUnit), expr)

  /**
    * Rounds up a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.ceil(MINUTE) leads to 12:45:00
    */
  def ceil(timeIntervalUnit: TimeIntervalUnit): Expression =
    unresolvedCall(CEIL, valueLiteral(timeIntervalUnit), expr)

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
  def get(name: String): Expression = unresolvedCall(GET, expr, valueLiteral(name))

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index and
    * returns it's value.
    *
    * @param index position of the field
    * @return value of the field
    */
  def get(index: Int): Expression = unresolvedCall(GET, expr, valueLiteral(index))

  /**
    * Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
    * into a flat representation where every subtype is a separate field.
    */
  def flatten(): Expression = unresolvedCall(FLATTEN, expr)

  /**
    * Accesses the element of an array or map based on a key or an index (starting at 1).
    *
    * @param index key or position of the element (array index starting at 1)
    * @return value of the element
    */
  def at(index: Expression): Expression = unresolvedCall(AT, expr, index)

  /**
    * Returns the number of elements of an array or number of entries of a map.
    *
    * @return number of elements or entries
    */
  def cardinality(): Expression = unresolvedCall(CARDINALITY, expr)

  /**
    * Returns the sole element of an array with a single element. Returns null if the array is
    * empty. Throws an exception if the array has more than one element.
    *
    * @return the first and only element of an array with a single element
    */
  def element(): Expression = unresolvedCall(ARRAY_ELEMENT, expr)

  // Time definition

  /**
    * Declares a field as the rowtime attribute for indicating, accessing, and working in
    * Flink's event time.
    */
  def rowtime: Expression = unresolvedCall(ROWTIME, expr)

  /**
    * Declares a field as the proctime attribute for indicating, accessing, and working in
    * Flink's processing time.
    */
  def proctime: Expression = unresolvedCall(PROCTIME, expr)

  // Hash functions

  /**
    * Returns the MD5 hash of the string argument; null if string is null.
    *
    * @return string of 32 hexadecimal digits or null
    */
  def md5(): Expression = unresolvedCall(MD5, expr)

  /**
    * Returns the SHA-1 hash of the string argument; null if string is null.
    *
    * @return string of 40 hexadecimal digits or null
    */
  def sha1(): Expression = unresolvedCall(SHA1, expr)

  /**
    * Returns the SHA-224 hash of the string argument; null if string is null.
    *
    * @return string of 56 hexadecimal digits or null
    */
  def sha224(): Expression = unresolvedCall(SHA224, expr)

  /**
    * Returns the SHA-256 hash of the string argument; null if string is null.
    *
    * @return string of 64 hexadecimal digits or null
    */
  def sha256(): Expression = unresolvedCall(SHA256, expr)

  /**
    * Returns the SHA-384 hash of the string argument; null if string is null.
    *
    * @return string of 96 hexadecimal digits or null
    */
  def sha384(): Expression = unresolvedCall(SHA384, expr)

  /**
    * Returns the SHA-512 hash of the string argument; null if string is null.
    *
    * @return string of 128 hexadecimal digits or null
    */
  def sha512(): Expression = unresolvedCall(SHA512, expr)

  /**
    * Returns the hash for the given string expression using the SHA-2 family of hash
    * functions (SHA-224, SHA-256, SHA-384, or SHA-512).
    *
    * @param hashLength bit length of the result (either 224, 256, 384, or 512)
    * @return string or null if one of the arguments is null.
    */
  def sha2(hashLength: Expression): Expression = unresolvedCall(SHA2, expr, hashLength)
}

/**
  * Implicit conversions from Scala literals to [[Expression]] and from [[Expression]]
  * to [[ImplicitExpressionOperations]].
  */
@PublicEvolving
trait ImplicitExpressionConversions {

  // ----------------------------------------------------------------------------------------------
  // Implicit values
  // ----------------------------------------------------------------------------------------------

  /**
    * Offset constant to be used in the `preceding` clause of unbounded [[Over]] windows. Use this
    * constant for a time interval. Unbounded over windows start with the first row of a partition.
    */
  implicit val UNBOUNDED_ROW: Expression = unresolvedCall(BuiltInFunctionDefinitions.UNBOUNDED_ROW)

  /**
    * Offset constant to be used in the `preceding` clause of unbounded [[Over]] windows. Use this
    * constant for a row-count interval. Unbounded over windows start with the first row of a
    * partition.
    */
  implicit val UNBOUNDED_RANGE: Expression =
    unresolvedCall(BuiltInFunctionDefinitions.UNBOUNDED_RANGE)

  /**
    * Offset constant to be used in the `following` clause of [[Over]] windows. Use this for setting
    * the upper bound of the window to the current row.
    */
  implicit val CURRENT_ROW: Expression = unresolvedCall(BuiltInFunctionDefinitions.CURRENT_ROW)

  /**
    * Offset constant to be used in the `following` clause of [[Over]] windows. Use this for setting
    * the upper bound of the window to the sort key of the current row, i.e., all rows with the same
    * sort key as the current row are included in the window.
    */
  implicit val CURRENT_RANGE: Expression = unresolvedCall(BuiltInFunctionDefinitions.CURRENT_RANGE)

  // ----------------------------------------------------------------------------------------------
  // Implicit conversions
  // ----------------------------------------------------------------------------------------------

  implicit class WithOperations(e: Expression) extends ImplicitExpressionOperations {
    def expr: Expression = e
  }

  implicit class UnresolvedFieldExpression(s: Symbol) extends ImplicitExpressionOperations {
    def expr: Expression = unresolvedRef(s.name)
  }

  implicit class LiteralLongExpression(l: Long) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(l)
  }

  implicit class LiteralByteExpression(b: Byte) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(b)
  }

  implicit class LiteralShortExpression(s: Short) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(s)
  }

  implicit class LiteralIntExpression(i: Int) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(i)
  }

  implicit class LiteralFloatExpression(f: Float) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(f)
  }

  implicit class LiteralDoubleExpression(d: Double) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(d)
  }

  implicit class LiteralStringExpression(str: String) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(str)
  }

  implicit class LiteralBooleanExpression(bool: Boolean) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(bool)
  }

  implicit class LiteralJavaDecimalExpression(javaDecimal: JBigDecimal)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(javaDecimal)
  }

  implicit class LiteralScalaDecimalExpression(scalaDecimal: BigDecimal)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(scalaDecimal.bigDecimal)
  }

  implicit class LiteralSqlDateExpression(sqlDate: Date)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(sqlDate)
  }

  implicit class LiteralSqlTimeExpression(sqlTime: Time)
    extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(sqlTime)
  }

  implicit class LiteralSqlTimestampExpression(sqlTimestamp: Timestamp)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(sqlTimestamp)
  }

  implicit class ScalarFunctionCall(val s: ScalarFunction) {

    /**
      * Calls a scalar function for the given parameters.
      */
    def apply(params: Expression*): Expression = {
      unresolvedCall(new ScalarFunctionDefinition(s.getClass.getName, s), params:_*)
    }
  }

  implicit class TableFunctionCall[T: TypeInformation](val t: TableFunction[T]) {

    /**
      * Calls a table function for the given parameters.
      */
    def apply(params: Expression*): Expression = {
      val resultTypeInfo: TypeInformation[T] = UserFunctionsTypeHelper
        .getReturnTypeOfTableFunction(t, implicitly[TypeInformation[T]])
      unresolvedCall(new TableFunctionDefinition(t.getClass.getName, t, resultTypeInfo), params: _*)
    }
  }

  implicit class UserDefinedAggregateFunctionCall[T: TypeInformation, ACC: TypeInformation]
      (val a: UserDefinedAggregateFunction[T, ACC]) {

    private def createFunctionDefinition(): FunctionDefinition = {
      val resultTypeInfo: TypeInformation[T] = UserFunctionsTypeHelper
        .getReturnTypeOfAggregateFunction(a, implicitly[TypeInformation[T]])

      val accTypeInfo: TypeInformation[ACC] = UserFunctionsTypeHelper.
        getAccumulatorTypeOfAggregateFunction(a, implicitly[TypeInformation[ACC]])

      a match {
        case af: AggregateFunction[_, _] =>
          new AggregateFunctionDefinition(
            af.getClass.getName, af, resultTypeInfo, accTypeInfo)
        case taf: TableAggregateFunction[_, _] =>
          new TableAggregateFunctionDefinition(
            taf.getClass.getName, taf, resultTypeInfo, accTypeInfo)
      }
    }

    /**
      * Calls an aggregate function for the given parameters.
      */
    def apply(params: Expression*): Expression = {
      unresolvedCall(createFunctionDefinition(), params: _*)
    }

    /**
      * Calculates the aggregate results only for distinct values.
      */
    def distinct(params: Expression*): Expression = {
      unresolvedCall(DISTINCT, apply(params: _*))
    }
  }

  implicit def tableSymbolToExpression(sym: TableSymbol): Expression =
    valueLiteral(sym)

  implicit def symbol2FieldExpression(sym: Symbol): Expression =
    unresolvedRef(sym.name)

  implicit def scalaRange2RangeExpression(range: Range.Inclusive): Expression = {
    val startExpression = valueLiteral(range.start)
    val endExpression = valueLiteral(range.end)
    startExpression to endExpression
  }

  implicit def byte2Literal(b: Byte): Expression = valueLiteral(b)

  implicit def short2Literal(s: Short): Expression = valueLiteral(s)

  implicit def int2Literal(i: Int): Expression = valueLiteral(i)

  implicit def long2Literal(l: Long): Expression = valueLiteral(l)

  implicit def double2Literal(d: Double): Expression = valueLiteral(d)

  implicit def float2Literal(d: Float): Expression = valueLiteral(d)

  implicit def string2Literal(str: String): Expression = valueLiteral(str)

  implicit def boolean2Literal(bool: Boolean): Expression = valueLiteral(bool)

  implicit def javaDec2Literal(javaDec: JBigDecimal): Expression = valueLiteral(javaDec)

  implicit def scalaDec2Literal(scalaDec: BigDecimal): Expression =
    valueLiteral(scalaDec.bigDecimal)

  implicit def sqlDate2Literal(sqlDate: Date): Expression = valueLiteral(sqlDate)

  implicit def sqlTime2Literal(sqlTime: Time): Expression = valueLiteral(sqlTime)

  implicit def sqlTimestamp2Literal(sqlTimestamp: Timestamp): Expression =
    valueLiteral(sqlTimestamp)

  implicit def localDate2Literal(localDate: LocalDate): Expression = valueLiteral(localDate)

  implicit def localTime2Literal(localTime: LocalTime): Expression = valueLiteral(localTime)

  implicit def localDateTime2Literal(localDateTime: LocalDateTime): Expression =
    valueLiteral(localDateTime)

  implicit def array2ArrayConstructor(array: Array[_]): Expression = {

    def createArray(elements: Array[_]): Expression = {
      unresolvedCall(BuiltInFunctionDefinitions.ARRAY, elements.map(valueLiteral): _*)
    }

    def convertArray(array: Array[_]): Expression = array match {
      // primitives
      case _: Array[Boolean] => createArray(array)
      case _: Array[Byte] => createArray(array)
      case _: Array[Short] => createArray(array)
      case _: Array[Int] => createArray(array)
      case _: Array[Long] => createArray(array)
      case _: Array[Float] => createArray(array)
      case _: Array[Double] => createArray(array)

      // boxed types
      case _: Array[JBoolean] => createArray(array)
      case _: Array[JByte] => createArray(array)
      case _: Array[JShort] => createArray(array)
      case _: Array[JInteger] => createArray(array)
      case _: Array[JLong] => createArray(array)
      case _: Array[JFloat] => createArray(array)
      case _: Array[JDouble] => createArray(array)

      // others
      case _: Array[String] => createArray(array)
      case _: Array[JBigDecimal] => createArray(array)
      case _: Array[Date] => createArray(array)
      case _: Array[Time] => createArray(array)
      case _: Array[Timestamp] => createArray(array)
      case _: Array[LocalDate] => createArray(array)
      case _: Array[LocalTime] => createArray(array)
      case _: Array[LocalDateTime] => createArray(array)
      case bda: Array[BigDecimal] => createArray(bda.map(_.bigDecimal))

      case _ =>
        // nested
        if (array.length > 0 && array.head.isInstanceOf[Array[_]]) {
          unresolvedCall(
            BuiltInFunctionDefinitions.ARRAY,
            array.map { na => convertArray(na.asInstanceOf[Array[_]]) } :_*)
        } else {
          throw new ValidationException("Unsupported array type.")
        }
    }

    convertArray(array)
  }

  // ----------------------------------------------------------------------------------------------
  // Implicit expressions in prefix notation
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns the current SQL date in UTC time zone.
    */
  def currentDate(): Expression = {
    unresolvedCall(CURRENT_DATE)
  }

  /**
    * Returns the current SQL time in UTC time zone.
    */
  def currentTime(): Expression = {
    unresolvedCall(CURRENT_TIME)
  }

  /**
    * Returns the current SQL timestamp in UTC time zone.
    */
  def currentTimestamp(): Expression = {
    unresolvedCall(CURRENT_TIMESTAMP)
  }

  /**
    * Returns the current SQL time in local time zone.
    */
  def localTime(): Expression = {
    unresolvedCall(LOCAL_TIME)
  }

  /**
    * Returns the current SQL timestamp in local time zone.
    */
  def localTimestamp(): Expression = {
    unresolvedCall(LOCAL_TIMESTAMP)
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
  def temporalOverlaps(
      leftTimePoint: Expression,
      leftTemporal: Expression,
      rightTimePoint: Expression,
      rightTemporal: Expression)
    : Expression = {
    unresolvedCall(TEMPORAL_OVERLAPS, leftTimePoint, leftTemporal, rightTimePoint, rightTemporal)
  }

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
  def dateFormat(
      timestamp: Expression,
      format: Expression)
    : Expression = {
    unresolvedCall(DATE_FORMAT, timestamp, format)
  }

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
  def timestampDiff(
      timePointUnit: TimePointUnit,
      timePoint1: Expression,
      timePoint2: Expression)
    : Expression = {
    unresolvedCall(TIMESTAMP_DIFF, timePointUnit, timePoint1, timePoint2)
  }

  /**
    * Creates an array of literals.
    */
  def array(head: Expression, tail: Expression*): Expression = {
    unresolvedCall(ARRAY, head +: tail: _*)
  }

  /**
    * Creates a row of expressions.
    */
  def row(head: Expression, tail: Expression*): Expression = {
    unresolvedCall(ROW, head +: tail: _*)
  }

  /**
    * Creates a map of expressions.
    */
  def map(key: Expression, value: Expression, tail: Expression*): Expression = {
    unresolvedCall(MAP, key +: value +: tail: _*)
  }

  /**
    * Returns a value that is closer than any other value to pi.
    */
  def pi(): Expression = {
    unresolvedCall(PI)
  }

  /**
    * Returns a value that is closer than any other value to e.
    */
  def e(): Expression = {
    unresolvedCall(E)
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
    */
  def rand(): Expression = {
    unresolvedCall(RAND)
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
    * initial seed. Two rand() functions will return identical sequences of numbers if they
    * have same initial seed.
    */
  def rand(seed: Expression): Expression = {
    unresolvedCall(RAND, seed)
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
    * value (exclusive).
    */
  def randInteger(bound: Expression): Expression = {
    unresolvedCall(RAND_INTEGER, bound)
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
    * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
    * of numbers if they have same initial seed and same bound.
    */
  def randInteger(seed: Expression, bound: Expression): Expression = {
    unresolvedCall(RAND_INTEGER, seed, bound)
  }

  /**
    * Returns the string that results from concatenating the arguments.
    * Returns NULL if any argument is NULL.
    */
  def concat(string: Expression, strings: Expression*): Expression = {
    unresolvedCall(CONCAT, string +: strings: _*)
  }

  /**
    * Calculates the arc tangent of a given coordinate.
    */
  def atan2(y: Expression, x: Expression): Expression = {
    unresolvedCall(ATAN2, y, x)
  }

  /**
    * Returns the string that results from concatenating the arguments and separator.
    * Returns NULL If the separator is NULL.
    *
    * Note: this user-defined function does not skip empty strings. However, it does skip any NULL
    * values after the separator argument.
    **/
  def concat_ws(separator: Expression, string: Expression, strings: Expression*): Expression = {
    unresolvedCall(CONCAT_WS, separator +: string +: strings: _*)
  }

  /**
    * Returns an UUID (Universally Unique Identifier) string (e.g.,
    * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
    * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
    * generator.
    */
  def uuid(): Expression = {
    unresolvedCall(UUID)
  }

  /**
    * Returns a null literal value of a given data type.
    *
    * e.g. nullOf(DataTypes.INT())
    */
  def nullOf(dataType: DataType): Expression = {
    valueLiteral(null, dataType)
  }

  /**
    * @deprecated This method will be removed in future versions as it uses the old type system.
    *             It is recommended to use [[nullOf(DataType)]] instead which uses the new type
    *             system based on [[DataTypes]]. Please make sure to use either the old or the new
    *             type system consistently to avoid unintended behavior. See the website
    *             documentation for more information.
    */
  def nullOf(typeInfo: TypeInformation[_]): Expression = {
    nullOf(TypeConversions.fromLegacyInfoToDataType(typeInfo))
  }

  /**
    * Calculates the logarithm of the given value.
    */
  def log(value: Expression): Expression = {
    unresolvedCall(LOG, value)
  }

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def log(base: Expression, value: Expression): Expression = {
    unresolvedCall(LOG, base, value)
  }

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
  def ifThenElse(condition: Expression, ifTrue: Expression, ifFalse: Expression): Expression = {
    unresolvedCall(IF, condition, ifTrue, ifFalse)
  }

  /**
    * Creates an expression that selects a range of columns. It can be used wherever an array of
    * expression is accepted such as function calls, projections, or groupings.
    *
    * A range can either be index-based or name-based. Indices start at 1 and boundaries are
    * inclusive.
    *
    * e.g. withColumns('b to 'c) or withColumns('*)
    */
  def withColumns(head: Expression, tail: Expression*): Expression = {
    unresolvedCall(WITH_COLUMNS, head +: tail: _*)
  }

  /**
    * Creates an expression that selects all columns except for the given range of columns. It can
    * be used wherever an array of expression is accepted such as function calls, projections, or
    * groupings.
    *
    * A range can either be index-based or name-based. Indices start at 1 and boundaries are
    * inclusive.
    *
    * e.g. withoutColumns('b to 'c) or withoutColumns('c)
    */
  def withoutColumns(head: Expression, tail: Expression*): Expression = {
    unresolvedCall(WITHOUT_COLUMNS, head +: tail: _*)
  }
}
