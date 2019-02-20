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
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.ExpressionUtils._
import org.apache.flink.table.expressions.FunctionDefinitions.{E => FDE, UUID => FDUUID, _}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{AggregateFunction, DistinctAggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.logical.LogicalTableFunctionCall

import _root_.scala.collection.JavaConversions._
import _root_.scala.language.implicitConversions

/**
  * These are all the operations that can be used to construct an [[Expression]] AST for
  * expression operations.
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
  def && (other: Expression) = call(AND, Seq(expr, other))

  /**
    * Boolean OR in three-valued logic.
    */
  def || (other: Expression) = call(OR, Seq(expr, other))

  /**
    * Greater than.
    */
  def > (other: Expression) = call(GREATER_THAN, Seq(expr, other))

  /**
    * Greater than or equal.
    */
  def >= (other: Expression) = call(GREATER_THAN_OR_EQUAL, Seq(expr, other))

  /**
    * Less than.
    */
  def < (other: Expression) = call(LESS_THAN, Seq(expr, other))

  /**
    * Less than or equal.
    */
  def <= (other: Expression) = call(LESS_THAN_OR_EQUAL, Seq(expr, other))

  /**
    * Equals.
    */
  def === (other: Expression) = call(EQUALS, Seq(expr, other))

  /**
    * Not equal.
    */
  def !== (other: Expression) = call(NOT_EQUALS, Seq(expr, other))

  /**
    * Whether boolean expression is not true; returns null if boolean is null.
    */
  def unary_! = call(NOT, Seq(expr))

  /**
    * Returns negative numeric.
    */
  def unary_- = call(MINUS_PREFIX, Seq(expr))

  /**
    * Returns numeric.
    */
  def unary_+ : Expression = expr

  /**
    * Returns true if the given expression is null.
    */
  def isNull = call(IS_NULL, Seq(expr))

  /**
    * Returns true if the given expression is not null.
    */
  def isNotNull = call(IS_NOT_NULL, Seq(expr))

  /**
    * Returns true if given boolean expression is true. False otherwise (for null and false).
    */
  def isTrue = call(IS_TRUE, Seq(expr))

  /**
    * Returns true if given boolean expression is false. False otherwise (for null and true).
    */
  def isFalse = call(IS_FALSE, Seq(expr))

  /**
    * Returns true if given boolean expression is not true (for null and false). False otherwise.
    */
  def isNotTrue = call(IS_NOT_TRUE, Seq(expr))

  /**
    * Returns true if given boolean expression is not false (for null and true). False otherwise.
    */
  def isNotFalse = call(IS_NOT_FALSE, Seq(expr))

  /**
    * Returns left plus right.
    */
  def + (other: Expression) = call(PLUS, Seq(expr, other))

  /**
    * Returns left minus right.
    */
  def - (other: Expression) = call(MINUS, Seq(expr, other))

  /**
    * Returns left divided by right.
    */
  def / (other: Expression) = call(DIVIDE, Seq(expr, other))

  /**
    * Returns left multiplied by right.
    */
  def * (other: Expression) = call(TIMES, Seq(expr, other))

  /**
    * Returns the remainder (modulus) of left divided by right.
    * The result is negative only if left is negative.
    */
  def % (other: Expression) = mod(other)

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, null is returned.
    */
  def sum = aggCall(SUM, Seq(expr))

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, 0 is returned.
    */
  def sum0 = aggCall(SUM0, Seq(expr))

  /**
    * Returns the minimum value of field across all input values.
    */
  def min = aggCall(MIN, Seq(expr))

  /**
    * Returns the maximum value of field across all input values.
    */
  def max = aggCall(MAX, Seq(expr))

  /**
    * Returns the number of input rows for which the field is not null.
    */
  def count = aggCall(COUNT, Seq(expr))

  /**
    * Returns the average (arithmetic mean) of the numeric field across all input values.
    */
  def avg = aggCall(AVG, Seq(expr))

  /**
    * Returns the population standard deviation of an expression (the square root of varPop()).
    */
  def stddevPop = aggCall(STDDEV_POP, Seq(expr))

  /**
    * Returns the sample standard deviation of an expression (the square root of varSamp()).
    */
  def stddevSamp = aggCall(STDDEV_SAMP, Seq(expr))

  /**
    * Returns the population standard variance of an expression.
    */
  def varPop = aggCall(VAR_POP, Seq(expr))

  /**
    *  Returns the sample variance of a given expression.
    */
  def varSamp = aggCall(VAR_SAMP, Seq(expr))

  /**
    * Returns multiset aggregate of a given expression.
    */
  def collect = aggCall(COLLECT, Seq(expr))

  /**
    * Converts a value to a given type.
    *
    * e.g. "42".cast(Types.INT) leads to 42.
    *
    * @return casted expression
    */
  def cast(toType: TypeLiteralExpression) = call(CAST, Seq(expr, toType))

  /**
    * Specifies a name for an expression i.e. a field.
    *
    * @param name name for one field
    * @param extraNames additional names if the expression expands to multiple fields
    * @return field with an alias
    */
  def as(name: Symbol, extraNames: Symbol*) =
    call(AS, Seq(expr) ++ Seq(Literal(name.name)) ++ extraNames.map(name => Literal(name.name)))

  /**
    * Specifies ascending order of an expression i.e. a field for orderBy call.
    *
    * @return ascend expression
    */
  def asc = call(ASC, Seq(expr))

  /**
    * Specifies descending order of an expression i.e. a field for orderBy call.
    *
    * @return descend expression
    */
  def desc = call(DESC, Seq(expr))

  /**
    * Returns true if an expression exists in a given list of expressions. This is a shorthand
    * for multiple OR conditions.
    *
    * If the testing set contains null, the result will be null if the element can not be found
    * and true if it can be found. If the element is null, the result is always null.
    *
    * e.g. "42".in(1, 2, 3) leads to false.
    */
  def in(elements: Expression*) = call(IN, Seq(expr) ++ elements)

  /**
    * Returns true if an expression exists in a given table sub-query. The sub-query table
    * must consist of one column. This column must have the same data type as the expression.
    *
    * Note: This operation is not supported in a streaming environment yet.
    */
  def in(table: Table) = call(IN, Seq(expr, new TableReferenceExpression(table.toString, table)))

  /**
    * Returns the start time (inclusive) of a window when applied on a window reference.
    */
  def start = call(WIN_START, Seq(expr))

  /**
    * Returns the end time (exclusive) of a window when applied on a window reference.
    *
    * e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.
    */
  def end = call(WIN_END, Seq(expr))

  /**
    * Ternary conditional operator that decides which of two other expressions should be
    * based on a evaluated boolean condition.
    *
    * e.g. (42 > 5).?("A", "B") leads to "A"
    *
    * @param ifTrue expression to be evaluated if condition holds
    * @param ifFalse expression to be evaluated if condition does not hold
    */
  def ?(ifTrue: Expression, ifFalse: Expression) = {
    call(IF, Seq(expr, ifTrue, ifFalse))
  }

  // scalar functions

  /**
    * Calculates the remainder of division the given number by another one.
    */
  def mod(other: Expression) = call(MOD, Seq(expr, other))

  /**
    * Calculates the Euler's number raised to the given power.
    */
  def exp() = call(EXP, Seq(expr))

  /**
    * Calculates the base 10 logarithm of the given value.
    */
  def log10() = call(LOG10, Seq(expr))

  /**
    * Calculates the base 2 logarithm of the given value.
    */
  def log2() = call(LOG2, Seq(expr))

  /**
    * Calculates the natural logarithm of the given value.
    */
  def ln() = call(LN, Seq(expr))

  /**
    * Calculates the natural logarithm of the given value.
    */
  def log() = call(LOG, Seq(expr))

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def log(base: Expression) = call(LOG, Seq(base, expr))

  /**
    * Calculates the given number raised to the power of the other value.
    */
  def power(other: Expression) = call(POWER, Seq(expr, other))

  /**
    * Calculates the hyperbolic cosine of a given value.
    */
  def cosh() = call(COSH, Seq(expr))

  /**
    * Calculates the square root of a given value.
    */
  def sqrt() = call(SQRT, Seq(expr))

  /**
    * Calculates the absolute value of given value.
    */
  def abs() = call(ABS, Seq(expr))

  /**
    * Calculates the largest integer less than or equal to a given number.
    */
  def floor() = call(FLOOR, Seq(expr))

  /**
    * Calculates the hyperbolic sine of a given value.
    */
  def sinh() = call(SINH, Seq(expr))

  /**
    * Calculates the smallest integer greater than or equal to a given number.
    */
  def ceil() = call(CEIL, Seq(expr))

  /**
    * Calculates the sine of a given number.
    */
  def sin() = call(SIN, Seq(expr))

  /**
    * Calculates the cosine of a given number.
    */
  def cos() = call(COS, Seq(expr))

  /**
    * Calculates the tangent of a given number.
    */
  def tan() = call(TAN, Seq(expr))

  /**
    * Calculates the cotangent of a given number.
    */
  def cot() = call(COT, Seq(expr))

  /**
    * Calculates the arc sine of a given number.
    */
  def asin() = call(ASIN, Seq(expr))

  /**
    * Calculates the arc cosine of a given number.
    */
  def acos() = call(ACOS, Seq(expr))

  /**
    * Calculates the arc tangent of a given number.
    */
  def atan() = call(ATAN, Seq(expr))

  /**
    * Calculates the hyperbolic tangent of a given number.
    */
  def tanh() = call(TANH, Seq(expr))

  /**
    * Converts numeric from radians to degrees.
    */
  def degrees() = call(DEGREES, Seq(expr))

  /**
    * Converts numeric from degrees to radians.
    */
  def radians() = call(RADIANS, Seq(expr))

  /**
    * Calculates the signum of a given number.
    */
  def sign() = call(SIGN, Seq(expr))

  /**
    * Rounds the given number to integer places right to the decimal point.
    */
  def round(places: Expression) = call(ROUND, Seq(expr, places))

  /**
    * Returns a string representation of an integer numeric value in binary format. Returns null if
    * numeric is null. E.g. "4" leads to "100", "12" leads to "1100".
    */
  def bin() = call(BIN, Seq(expr))

  /**
    * Returns a string representation of an integer numeric value or a string in hex format. Returns
    * null if numeric or string is null.
    *
    * E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world" leads
    * to "68656c6c6f2c776f726c64".
    */
  def hex() = call(HEX, Seq(expr))

  /**
    * Returns a number of truncated to n decimal places.
    * If n is 0,the result has no decimal point or fractional part.
    * n can be negative to cause n digits left of the decimal point of the value to become zero.
    * E.g. truncate(42.345, 2) to 42.34.
    */
  def truncate(n: Expression) = call(TRUNCATE, Seq(expr, n))

  /**
    * Returns a number of truncated to 0 decimal places.
    * E.g. truncate(42.345) to 42.0.
    */
  def truncate() = call(TRUNCATE, Seq(expr))

  // String operations

  /**
    * Creates a substring of the given string at given index for a given length.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @param length number of characters of the substring
    * @return substring
    */
  def substring(beginIndex: Expression, length: Expression) =
    call(SUBSTRING, Seq(expr, beginIndex, length))

  /**
    * Creates a substring of the given string beginning at the given index to the end.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression) =
    call(SUBSTRING, Seq(expr, beginIndex))

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
      character: Expression = new ValueLiteralExpression(" ")) = {
    if (removeLeading && removeTrailing) {
      call(TRIM, Seq(TrimMode.BOTH, character, expr))
    } else if (removeLeading) {
      call(TRIM, Seq(TrimMode.LEADING, character, expr))
    } else if (removeTrailing) {
      call(TRIM, Seq(TrimMode.TRAILING, character, expr))
    } else {
      expr
    }
  }

  /**
    * Returns a new string which replaces all the occurrences of the search target
    * with the replacement string (non-overlapping).
    */
  def replace(search: Expression, replacement: Expression) =
    call(REPLACE, Seq(expr, search, replacement))

  /**
    * Returns the length of a string.
    */
  def charLength() = call(CHAR_LENGTH, Seq(expr))

  /**
    * Returns all of the characters in a string in upper case using the rules of
    * the default locale.
    */
  def upperCase() = call(UPPER, Seq(expr))

  /**
    * Returns all of the characters in a string in lower case using the rules of
    * the default locale.
    */
  def lowerCase() = call(LOWER, Seq(expr))

  /**
    * Converts the initial letter of each word in a string to uppercase.
    * Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.
    */
  def initCap() = call(INIT_CAP, Seq(expr))

  /**
    * Returns true, if a string matches the specified LIKE pattern.
    *
    * e.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n"
    */
  def like(pattern: Expression) = call(LIKE, Seq(expr, pattern))

  /**
    * Returns true, if a string matches the specified SQL regex pattern.
    *
    * e.g. "A+" matches all strings that consist of at least one A
    */
  def similar(pattern: Expression) = call(SIMILAR, Seq(expr, pattern))

  /**
    * Returns the position of string in an other string starting at 1.
    * Returns 0 if string could not be found.
    *
    * e.g. "a".position("bbbbba") leads to 6
    */
  def position(haystack: Expression) = call(POSITION, Seq(expr, haystack))

  /**
    * Returns a string left-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".lpad(4, '??') returns "??hi",  "hi".lpad(1, '??') returns "h"
    */
  def lpad(len: Expression, pad: Expression) = call(LPAD, Seq(expr, len, pad))

  /**
    * Returns a string right-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".rpad(4, '??') returns "hi??",  "hi".rpad(1, '??') returns "h"
    */
  def rpad(len: Expression, pad: Expression) = call(RPAD, Seq(expr, len, pad))

  /**
    * For windowing function to config over window
    * e.g.:
    * table
    *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
    *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
    */
  def over(alias: Expression): Expression = {
    expr match {
      case call: CallExpression
        if call.getFunctionDefinition.getFunctionType == FunctionType.AGGREGATION =>
        ExpressionUtils.call(FunctionDefinitions.OVER_CALL, Seq(expr, alias))
      case _ => throw new TableException(
        "The over method can only using with aggregation expression.")
    }
  }

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6) leads to "xxxxxxxxx"
    */
  def overlay(newString: Expression, starting: Expression) =
    call(OVERLAY, Seq(expr, newString, starting))

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    * The length specifies how many characters should be removed.
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6, 2) leads to "xxxxxxxxxst"
    */
  def overlay(newString: Expression, starting: Expression, length: Expression) =
    call(OVERLAY, Seq(expr, newString, starting, length))

  /**
    * Returns a string with all substrings that match the regular expression consecutively
    * being replaced.
    */
  def regexpReplace(regex: Expression, replacement: Expression) =
    call(REGEXP_REPLACE, Seq(expr, regex, replacement))

  /**
    * Returns a string extracted with a specified regular expression and a regex match group
    * index.
    */
  def regexpExtract(regex: Expression, extractIndex: Expression) =
    call(REGEXP_EXTRACT, Seq(expr, regex, extractIndex))

  /**
    * Returns a string extracted with a specified regular expression.
    */
  def regexpExtract(regex: Expression) =
    call(REGEXP_EXTRACT, Seq(expr, regex))

  /**
    * Returns the base string decoded with base64.
    */
  def fromBase64() = call(FROM_BASE64, Seq(expr))

  /**
    * Returns the base64-encoded result of the input string.
    */
  def toBase64() = call(TO_BASE64, Seq(expr))

  /**
    * Returns a string that removes the left whitespaces from the given string.
    */
  def ltrim() = call(LTRIM, Seq(expr))

  /**
    * Returns a string that removes the right whitespaces from the given string.
    */
  def rtrim() = call(RTRIM, Seq(expr))

  /**
    * Returns a string that repeats the base string n times.
    */
  def repeat(n: Expression) = call(REPEAT, Seq(expr, n))

  // Temporal operations

  /**
    * Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
    */
  def toDate = call(CAST, Seq(expr, SqlTimeTypeInfo.DATE))

  /**
    * Parses a time string in the form "HH:mm:ss" to a SQL Time.
    */
  def toTime = call(CAST, Seq(expr, SqlTimeTypeInfo.TIME))

  /**
    * Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
    */
  def toTimestamp = call(CAST, Seq(expr, SqlTimeTypeInfo.TIMESTAMP))

  /**
    * Extracts parts of a time point or time interval. Returns the part as a long value.
    *
    * e.g. "2006-06-05".toDate.extract(DAY) leads to 5
    */
  def extract(timeIntervalUnit: TimeIntervalUnit) =
    call(EXTRACT, Seq(timeIntervalUnit, expr))

  /**
    * Rounds down a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.floor(MINUTE) leads to 12:44:00
    */
  def floor(timeIntervalUnit: TimeIntervalUnit) =
    call(TEMPORAL_FLOOR, Seq(timeIntervalUnit, expr))

  /**
    * Rounds up a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.ceil(MINUTE) leads to 12:45:00
    */
  def ceil(timeIntervalUnit: TimeIntervalUnit) =
    call(TEMPORAL_CEIL, Seq(timeIntervalUnit, expr))

  // Interval types

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def year: Expression = ExpressionUtils.toMonthInterval(expr, 12)

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
  def quarter: Expression = ExpressionUtils.toMonthInterval(expr, 3)

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
  def month: Expression = ExpressionUtils.toMonthInterval(expr, 1)

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
  def week: Expression = ExpressionUtils.toMilliInterval(expr, 7 * MILLIS_PER_DAY)

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
  def get(name: String) = call(GET_COMPOSITE_FIELD, Seq(expr, name))

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index and
    * returns it's value.
    *
    * @param index position of the field
    * @return value of the field
    */
  def get(index: Int) = call(GET_COMPOSITE_FIELD, Seq(expr, index))

  /**
    * Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
    * into a flat representation where every subtype is a separate field.
    */
  def flatten() = call(FLATTEN, Seq(expr))

  /**
    * Accesses the element of an array or map based on a key or an index (starting at 1).
    *
    * @param index key or position of the element (array index starting at 1)
    * @return value of the element
    */
  def at(index: Expression) = call(AT, Seq(expr, index))

  /**
    * Returns the number of elements of an array or number of entries of a map.
    *
    * @return number of elements or entries
    */
  def cardinality() = call(CARDINALITY, Seq(expr))

  /**
    * Returns the sole element of an array with a single element. Returns null if the array is
    * empty. Throws an exception if the array has more than one element.
    *
    * @return the first and only element of an array with a single element
    */
  def element() = call(ARRAY_ELEMENT, Seq(expr))

  // Time definition

  /**
    * Declares a field as the rowtime attribute for indicating, accessing, and working in
    * Flink's event time.
    */
  def rowtime = call(ROW_TIME, Seq(expr))

  /**
    * Declares a field as the proctime attribute for indicating, accessing, and working in
    * Flink's processing time.
    */
  def proctime = call(PROC_TIME, Seq(expr))

  // Hash functions

  /**
    * Returns the MD5 hash of the string argument; null if string is null.
    *
    * @return string of 32 hexadecimal digits or null
    */
  def md5() = call(MD5, Seq(expr))

  /**
    * Returns the SHA-1 hash of the string argument; null if string is null.
    *
    * @return string of 40 hexadecimal digits or null
    */
  def sha1() = call(SHA1, Seq(expr))

  /**
    * Returns the SHA-224 hash of the string argument; null if string is null.
    *
    * @return string of 56 hexadecimal digits or null
    */
  def sha224() = call(SHA224, Seq(expr))

  /**
    * Returns the SHA-256 hash of the string argument; null if string is null.
    *
    * @return string of 64 hexadecimal digits or null
    */
  def sha256() = call(SHA256, Seq(expr))

  /**
    * Returns the SHA-384 hash of the string argument; null if string is null.
    *
    * @return string of 96 hexadecimal digits or null
    */
  def sha384() = call(SHA384, Seq(expr))

  /**
    * Returns the SHA-512 hash of the string argument; null if string is null.
    *
    * @return string of 128 hexadecimal digits or null
    */
  def sha512() = call(SHA512, Seq(expr))

  /**
    * Returns the hash for the given string expression using the SHA-2 family of hash
    * functions (SHA-224, SHA-256, SHA-384, or SHA-512).
    *
    * @param hashLength bit length of the result (either 224, 256, 384, or 512)
    * @return string or null if one of the arguments is null.
    */
  def sha2(hashLength: Expression) = call(SHA2, Seq(expr, hashLength))

  /**
    * Returns true if the given expression is between lowerBound and upperBound (both
    * inclusive). False otherwise. The parameters must be numeric types or identical
    * comparable types.
    *
    * @param lowerBound numeric or comparable expression
    * @param upperBound numeric or comparable expression
    * @return boolean or null
    */
  def between(lowerBound: Expression, upperBound: Expression) =
    call(BETWEEN, Seq(expr, lowerBound, upperBound))

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
    call(NOT_BETWEEN, Seq(expr, lowerBound, upperBound))
}

/**
  * Implicit conversions from Scala Literals to expression [[Literal]] and from [[Expression]]
  * to [[ImplicitExpressionOperations]].
  */
trait ImplicitExpressionConversions {

  implicit val UNBOUNDED_ROW = call(FunctionDefinitions.UNBOUNDED_ROW, Seq())
  implicit val UNBOUNDED_RANGE = call(FunctionDefinitions.UNBOUNDED_RANGE, Seq())

  implicit val CURRENT_ROW = call(FunctionDefinitions.CURRENT_ROW, Seq())
  implicit val CURRENT_RANGE = call(FunctionDefinitions.CURRENT_RANGE, Seq())

  implicit class WithOperations(e: Expression) extends ImplicitExpressionOperations {
    def expr = e
  }

  implicit class UnresolvedFieldExpression(s: Symbol) extends ImplicitExpressionOperations {
    def expr = new FieldReferenceExpression(s.name)
  }

  implicit class LiteralLongExpression(l: Long) extends ImplicitExpressionOperations {
    def expr = literal(l)
  }

  implicit class LiteralByteExpression(b: Byte) extends ImplicitExpressionOperations {
    def expr = literal(b)
  }

  implicit class LiteralShortExpression(s: Short) extends ImplicitExpressionOperations {
    def expr = literal(s)
  }

  implicit class LiteralIntExpression(i: Int) extends ImplicitExpressionOperations {
    def expr = literal(i)
  }

  implicit class LiteralFloatExpression(f: Float) extends ImplicitExpressionOperations {
    def expr = literal(f)
  }

  implicit class LiteralDoubleExpression(d: Double) extends ImplicitExpressionOperations {
    def expr = literal(d)
  }

  implicit class LiteralStringExpression(str: String) extends ImplicitExpressionOperations {
    def expr = literal(str)
  }

  implicit class LiteralBooleanExpression(bool: Boolean)
    extends ImplicitExpressionOperations {
    def expr = literal(bool)
  }

  implicit class LiteralJavaDecimalExpression(javaDecimal: JBigDecimal)
    extends ImplicitExpressionOperations {
    def expr = literal(javaDecimal)
  }

  implicit class LiteralScalaDecimalExpression(scalaDecimal: BigDecimal)
    extends ImplicitExpressionOperations {
    def expr = literal(scalaDecimal.bigDecimal)
  }

  implicit class LiteralSqlDateExpression(sqlDate: Date)
    extends ImplicitExpressionOperations {
    def expr = literal(sqlDate)
  }

  implicit class LiteralSqlTimeExpression(sqlTime: Time)
    extends ImplicitExpressionOperations {
    def expr = literal(sqlTime)
  }

  implicit class LiteralSqlTimestampExpression(sqlTimestamp: Timestamp)
    extends ImplicitExpressionOperations {
    def expr = literal(sqlTimestamp)
  }

  implicit class ScalarFunctionCallExpression(val s: ScalarFunction) {
    def apply(params: Expression*): Expression = {
      call(new ScalarFunctionDefinition(s), params)
    }
  }

  implicit class TableFunctionConversion
  [T: TypeInformation](val t: TableFunction[T]) {
    def apply(params: Expression*): TableFunctionWrapper[T] = {
      val resultType = if (t.getResultType == null) {
        implicitly[TypeInformation[T]]
      } else {
        t.getResultType
      }
      new TableFunctionWrapper(t, params, resultType)
    }
  }

  class TableFunctionWrapper[T](
      val tableFunction: TableFunction[T],
      val params: Seq[Expression],
      val resultType: TypeInformation[T]) {

    def as(name: Symbol, extraNames: Symbol*): TableFunctionWrapperWithAlias[T] = {
      new TableFunctionWrapperWithAlias(
        tableFunction,
        params,
        resultType,
        name.name +: extraNames.map(_.name)
      )
    }
  }

  class TableFunctionWrapperWithAlias[T](
      val tableFunction: TableFunction[T],
      val params: Seq[Expression],
      val resultType: TypeInformation[T],
      val alias: Seq[String]
  )

  implicit def tableFunctionWrapper2Expression(
      tfc: TableFunctionWrapper[_]): Expression = {
    val tfd = new TableFunctionDefinition(tfc.tableFunction, tfc.resultType)
    call(tfd, tfc.params)
  }

  implicit def tableFunctionWrapperWithAlias2Expression(
      tfc: TableFunctionWrapperWithAlias[_]): Expression = {
    val tfd = new TableFunctionDefinition(tfc.tableFunction, tfc.resultType)
    val expr = call(tfd, tfc.params)
    if (tfc.alias.nonEmpty) {
      call(AS, Seq(expr) ++ tfc.alias.map(Literal(_)))
    } else {
      expr
    }
  }

  implicit def tableSymbolToExpression(symbol: TableSymbol): SymbolExpression =
    new SymbolExpression(symbol)
  implicit def symbol2FieldExpression(sym: Symbol): Expression =
    new FieldReferenceExpression(sym.name)
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
    literal(scalaDec.bigDecimal)
  implicit def sqlDate2Literal(sqlDate: Date): Expression = literal(sqlDate)
  implicit def sqlTime2Literal(sqlTime: Time): Expression = literal(sqlTime)
  implicit def sqlTimestamp2Literal(sqlTimestamp: Timestamp): Expression =
    literal(sqlTimestamp)
  implicit def array2ArrayConstructor(array: Array[_]): Expression =
    ExpressionUtils.convertArray(array)
  implicit def typeInformation2TypeLiteral(t: TypeInformation[_]): TypeLiteralExpression =
    new TypeLiteralExpression(t)
  implicit def userDefinedAggFunctionConstructor[T: TypeInformation, ACC: TypeInformation]
  (udagg: AggregateFunction[T, ACC]): UDAGGExpression[T, ACC] = UDAGGExpression(udagg)
  implicit def toDistinct(agg: AggregateCallExpression): DistinctAggExpression =
    DistinctAggExpression(agg)
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
    call(CURRENT_DATE, Seq())
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
    call(CURRENT_TIME, Seq())
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
    call(CURRENT_TIMESTAMP, Seq())
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
    call(LOCAL_TIME, Seq())
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
    call(LOCAL_TIMESTAMP, Seq())
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
    call(TEMPORAL_OVERLAPS, Seq(leftTimePoint, leftTemporal, rightTimePoint, rightTemporal))
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
      format: Expression): Expression = {
    call(DATE_FORMAT, Seq(timestamp, format))
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
    call(TIMESTAMP_DIFF, Seq(timePointUnit, timePoint1, timePoint2))
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
    call(ARRAY, head +: tail.toSeq)
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
    call(ROW, head +: tail.toSeq)
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
    call(MAP, Seq(key, value) ++ tail.toSeq)
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
    call(PI, Seq())
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
    call(FDE, Seq())
  }
}

object log {

  /**
    * Returns a value that is closer than any other value to e.
    */
  def apply(antilogarithm: Expression): Expression = {
    call(LOG, Seq(antilogarithm))
  }

  def apply(base: Expression, antilogarithm: Expression): Expression = {
    call(LOG, Seq(base, antilogarithm))
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
    call(RAND, Seq())
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
    * initial seed. Two rand() functions will return identical sequences of numbers if they
    * have same initial seed.
    */
  def apply(seed: Expression): Expression = {
    call(RAND, Seq(seed))
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
    call(RAND_INTEGER, Seq(bound))
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
    * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
    * of numbers if they have same initial seed and same bound.
    */
  def apply(seed: Expression, bound: Expression): Expression = {
    call(RAND_INTEGER, Seq(seed, bound))
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
    call(CONCAT, Seq(string) ++ strings)
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
    call(ATAN2, Seq(y, x))
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
    call(CONCAT_WS, Seq(separator) ++ Seq(string) ++ strings)
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
    call(FDUUID, Seq())
  }
}

/**
  * Returns null literal expression.
  */
object Null {
  def apply(t: TypeInformation[_]): Expression =
    new ValueLiteralExpression(null, t)
}

/**
  * Returns literal expression.
  */
object Literal {
  def apply(l: Any): ValueLiteralExpression = literal(l)
}

// scalastyle:on object.name
