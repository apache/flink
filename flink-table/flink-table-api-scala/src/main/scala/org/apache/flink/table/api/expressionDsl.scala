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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.internal.BaseExpressions
import org.apache.flink.table.expressions.ApiExpressionUtils._
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.BuiltInFunctionDefinitions._
import org.apache.flink.table.functions.{ScalarFunction, TableFunction, UserDefinedAggregateFunction, UserDefinedFunctionHelper, _}
import org.apache.flink.table.types.DataType

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime}

import _root_.scala.language.implicitConversions

/**
  * These are all the operations that can be used to construct an [[Expression]] AST for
  * expression operations.
  */
@PublicEvolving
trait ImplicitExpressionOperations extends BaseExpressions[Expression, Expression] {
  private[flink] def expr: Expression

  override def toExpr: Expression = expr

  override protected def toApiSpecificExpression(expression: Expression): Expression = expression

  /**
   * Specifies a name for an expression i.e. a field.
   *
   * @param name name for one field
   * @param extraNames additional names if the expression expands to multiple fields
   * @return field with an alias
   */
  def as(name: Symbol, extraNames: Symbol*): Expression = as(name.name, extraNames.map(_.name): _*)

  /**
    * Boolean AND in three-valued logic.
    */
  def && (other: Expression): Expression = and(other)

  /**
    * Boolean OR in three-valued logic.
    */
  def || (other: Expression): Expression = or(other)

  /**
    * Greater than.
    */
  def > (other: Expression): Expression = isGreater(other)

  /**
    * Greater than or equal.
    */
  def >= (other: Expression): Expression = isGreaterOrEqual(other)

  /**
    * Less than.
    */
  def < (other: Expression): Expression = isLess(other)

  /**
    * Less than or equal.
    */
  def <= (other: Expression): Expression = isLessOrEqual(other)

  /**
    * Equals.
    */
  def === (other: Expression): Expression = isEqual(other)

  /**
    * Not equal.
    */
  def !== (other: Expression): Expression = isNotEqual(other)

  /**
    * Whether boolean expression is not true; returns null if boolean is null.
    */
  def unary_! : Expression = unresolvedCall(NOT, expr)

  /**
    * Returns negative numeric.
    */
  def unary_- : Expression = Expressions.negative(expr)

  /**
    * Returns numeric.
    */
  def unary_+ : Expression = expr

  /**
    * Returns left plus right.
    */
  def + (other: Expression): Expression = plus(other)

  /**
    * Returns left minus right.
    */
  def - (other: Expression): Expression = minus(other)

  /**
    * Returns left divided by right.
    */
  def / (other: Expression): Expression = dividedBy(other)

  /**
    * Returns left multiplied by right.
    */
  def * (other: Expression): Expression = times(other)

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
  def to (other: Expression): Expression = unresolvedCall(RANGE_TO, expr, objectToExpression(other))

  /**
    * Ternary conditional operator that decides which of two other expressions should be
    * based on a evaluated boolean condition.
    *
    * e.g. ($"f0" > 5).?("A", "B") leads to "A"
    *
    * @param ifTrue expression to be evaluated if condition holds
    * @param ifFalse expression to be evaluated if condition does not hold
    */
  def ?(ifTrue: Expression, ifFalse: Expression): Expression =
    Expressions.ifThenElse(expr, ifTrue, ifFalse).toExpr

  // scalar functions

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
    * @return interval of rows
    */
  def rows: Expression = toRowInterval(expr)

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
      unresolvedCall(
        new ScalarFunctionDefinition(s.getClass.getName, s),
        params.map(ApiExpressionUtils.objectToExpression): _*)
    }
  }

  implicit class TableFunctionCall[T: TypeInformation](val t: TableFunction[T]) {

    /**
      * Calls a table function for the given parameters.
      */
    def apply(params: Expression*): Expression = {
      val resultTypeInfo: TypeInformation[T] = UserDefinedFunctionHelper
        .getReturnTypeOfTableFunction(t, implicitly[TypeInformation[T]])
      unresolvedCall(
        new TableFunctionDefinition(t.getClass.getName, t, resultTypeInfo),
        params.map(ApiExpressionUtils.objectToExpression): _*)
    }
  }

  implicit class UserDefinedAggregateFunctionCall[T: TypeInformation, ACC: TypeInformation]
      (val a: UserDefinedAggregateFunction[T, ACC]) {

    private def createFunctionDefinition(): FunctionDefinition = {
      val resultTypeInfo: TypeInformation[T] = UserDefinedFunctionHelper
        .getReturnTypeOfAggregateFunction(a, implicitly[TypeInformation[T]])

      val accTypeInfo: TypeInformation[ACC] = UserDefinedFunctionHelper.
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
      unresolvedCall(
        createFunctionDefinition(),
        params.map(ApiExpressionUtils.objectToExpression): _*)
    }

    /**
      * Calculates the aggregate results only for distinct values.
      */
    def distinct(params: Expression*): Expression = {
      unresolvedCall(DISTINCT, apply(params: _*))
    }
  }


  /**
   * Extends Scala's StringContext with a method for creating an unresolved reference via
   * string interpolation.
   */
  implicit class FieldExpression(val sc: StringContext) {

    /**
     * Creates an unresolved reference to a table's field.
     *
     * Example:
     * ```
     * tab.select($"key", $"value")
     * ```
     * </pre>
     */
    def $(args: Any*): Expression = unresolvedRef(sc.s(args: _*))
  }

  implicit def tableSymbolToExpression(sym: TableSymbol): Expression =
    valueLiteral(sym)

  implicit def symbol2FieldExpression(sym: Symbol): Expression =
    unresolvedRef(sym.name)

  implicit def scalaRange2RangeExpression(range: Range.Inclusive): Expression = {
    val startExpression = valueLiteral(range.start)
    val endExpression = valueLiteral(range.end)
    unresolvedCall(RANGE_TO, startExpression, endExpression)
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
  // Function calls
  // ----------------------------------------------------------------------------------------------

  /**
   * A call to a function that will be looked up in a catalog. There are two kinds of functions:
   *
   *  - System functions - which are identified with one part names
   *  - Catalog functions - which are identified always with three parts names
   *    (catalog, database, function)
   *
   * Moreover each function can either be a temporary function or permanent one
   * (which is stored in a catalog).
   *
   * Based on those two properties, the resolution order for looking up a function based on
   * the provided path is as follows:
   *
   *  - Temporary system function
   *  - System function
   *  - Temporary catalog function
   *  - Catalog function
   *
   * @see TableEnvironment#useCatalog(String)
   * @see TableEnvironment#useDatabase(String)
   * @see TableEnvironment#createTemporaryFunction
   * @see TableEnvironment#createTemporarySystemFunction
   */
  def call(path: String, params: Expression*): Expression = Expressions.call(path, params: _*)

  /**
   * A call to an unregistered, inline function. For functions that have been registered before and
   * are identified by a name, use [[call(String, Object...)]].
   */
  def call(function: UserDefinedFunction, params: Expression*): Expression = Expressions.call(
    function,
    params: _*)

  // ----------------------------------------------------------------------------------------------
  // Implicit expressions in prefix notation
  // ----------------------------------------------------------------------------------------------

  /**
   * Creates a SQL literal.
   *
   * The data type is derived from the object's class and its value.
   *
   * For example:
   *
   *  - `lit(12)`` leads to `INT`
   *  - `lit("abc")`` leads to `CHAR(3)`
   *  - `lit(new java.math.BigDecimal("123.45"))` leads to `DECIMAL(5, 2)`
   *
   * See [[org.apache.flink.table.types.utils.ValueDataTypeConverter]] for a list of supported
   * literal values.
   */
  def lit(v: Any): Expression = Expressions.lit(v)

  /**
   * Creates a SQL literal of a given [[DataType]].
   *
   * The method [[lit(Object)]] is preferred as it extracts the [[DataType]]
   * automatically. The class of `v` must be supported according to the
   * [[org.apache.flink.table.types.logical.LogicalType#supportsInputConversion(Class)]].
   */
  def lit(v: Any, dataType: DataType): Expression = Expressions.lit(v, dataType)

  /**
   * Returns negative numeric.
   */
  def negative(v: Expression): Expression = {
    Expressions.negative(v)
  }

  /**
    * Returns the current SQL date in UTC time zone.
    */
  def currentDate(): Expression = {
    Expressions.currentDate()
  }

  /**
    * Returns the current SQL time in UTC time zone.
    */
  def currentTime(): Expression = {
    Expressions.currentTime()
  }

  /**
    * Returns the current SQL timestamp in UTC time zone.
    */
  def currentTimestamp(): Expression = {
    Expressions.currentTimestamp()
  }

  /**
    * Returns the current SQL time in local time zone.
    */
  def localTime(): Expression = {
    Expressions.localTime()
  }

  /**
    * Returns the current SQL timestamp in local time zone.
    */
  def localTimestamp(): Expression = {
    Expressions.localTimestamp()
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
    Expressions.temporalOverlaps(leftTimePoint, leftTemporal, rightTimePoint, rightTemporal)
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
    Expressions.dateFormat(timestamp, format)
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
    Expressions.timestampDiff(timePointUnit, timePoint1, timePoint2)
  }

  /**
    * Creates an array of literals.
    */
  def array(head: Expression, tail: Expression*): Expression = {
    Expressions.array(head, tail: _*)
  }

  /**
    * Creates a row of expressions.
    */
  def row(head: Expression, tail: Expression*): Expression = {
    Expressions.row(head, tail: _*)
  }

  /**
    * Creates a map of expressions.
    */
  def map(key: Expression, value: Expression, tail: Expression*): Expression = {
    Expressions.map(key, value, tail: _*)
  }

  /**
    * Returns a value that is closer than any other value to pi.
    */
  def pi(): Expression = {
    Expressions.pi()
  }

  /**
    * Returns a value that is closer than any other value to e.
    */
  def e(): Expression = {
    Expressions.e()
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
    */
  def rand(): Expression = {
    Expressions.rand()
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
    * initial seed. Two rand() functions will return identical sequences of numbers if they
    * have same initial seed.
    */
  def rand(seed: Expression): Expression = {
    Expressions.rand(seed)
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
    * value (exclusive).
    */
  def randInteger(bound: Expression): Expression = {
    Expressions.randInteger(bound)
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
    * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
    * of numbers if they have same initial seed and same bound.
    */
  def randInteger(seed: Expression, bound: Expression): Expression = {
    Expressions.randInteger(seed, bound)
  }

  /**
    * Returns the string that results from concatenating the arguments.
    * Returns NULL if any argument is NULL.
    */
  def concat(string: Expression, strings: Expression*): Expression = {
    Expressions.concat(string, strings: _*)
  }

  /**
    * Calculates the arc tangent of a given coordinate.
    */
  def atan2(y: Expression, x: Expression): Expression = {
    Expressions.atan2(y, x)
  }

  /**
    * Returns the string that results from concatenating the arguments and separator.
    * Returns NULL If the separator is NULL.
    *
    * Note: This function does not skip empty strings. However, it does skip any NULL
    * values after the separator argument.
    * @deprecated use [[ImplicitExpressionConversions.concatWs()]]
    **/
  @deprecated
  def concat_ws(separator: Expression, string: Expression, strings: Expression*): Expression = {
    concatWs(separator, string, strings: _*)
  }

  /**
   * Returns the string that results from concatenating the arguments and separator.
   * Returns NULL If the separator is NULL.
   *
   * Note: this user-defined function does not skip empty strings. However, it does skip any NULL
   * values after the separator argument.
   **/
  def concatWs(separator: Expression, string: Expression, strings: Expression*): Expression = {
    Expressions.concatWs(separator, string, strings: _*)
  }

  /**
    * Returns an UUID (Universally Unique Identifier) string (e.g.,
    * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
    * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
    * generator.
    */
  def uuid(): Expression = {
    Expressions.uuid()
  }

  /**
    * Returns a null literal value of a given data type.
    *
    * e.g. nullOf(DataTypes.INT())
    */
  def nullOf(dataType: DataType): Expression = {
    Expressions.nullOf(dataType)
  }

  /**
    * @deprecated This method will be removed in future versions as it uses the old type system.
    *             It is recommended to use [[nullOf(DataType)]] instead which uses the new type
    *             system based on [[DataTypes]]. Please make sure to use either the old or the new
    *             type system consistently to avoid unintended behavior. See the website
    *             documentation for more information.
    */
  def nullOf(typeInfo: TypeInformation[_]): Expression = {
    Expressions.nullOf(typeInfo)
  }

  /**
    * Calculates the logarithm of the given value.
    */
  def log(value: Expression): Expression = {
    Expressions.log(value)
  }

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def log(base: Expression, value: Expression): Expression = {
    Expressions.log(base, value)
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
    Expressions.ifThenElse(condition, ifTrue, ifFalse)
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
    Expressions.withColumns(head, tail: _*)
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
    Expressions.withoutColumns(head, tail: _*)
  }

  /**
   * Boolean AND in three-valued logic.
   */
  def and(predicate0: Expression, predicate1: Expression, predicates: Expression*): Expression = {
    Expressions.and(predicate0, predicate1, predicates: _*)
  }

  /**
   * Boolean OR in three-valued logic.
   */
  def or(predicate0: Expression, predicate1: Expression, predicates: Expression*): Expression = {
    Expressions.or(predicate0, predicate1, predicates: _*)
  }
}
