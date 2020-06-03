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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_DAY;
import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_HOUR;
import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_MINUTE;
import static org.apache.flink.table.expressions.ApiExpressionUtils.MILLIS_PER_SECOND;
import static org.apache.flink.table.expressions.ApiExpressionUtils.objectToExpression;
import static org.apache.flink.table.expressions.ApiExpressionUtils.tableRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.toMilliInterval;
import static org.apache.flink.table.expressions.ApiExpressionUtils.toMonthInterval;
import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

//CHECKSTYLE.OFF: AvoidStarImport|ImportOrder
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.*;
//CHECKSTYLE.ON: AvoidStarImport|ImportOrder

/**
 * These are Java and Scala common operations that can be used to construct an {@link Expression} AST for
 * expression operations.
 *
 * @param <InType>  The accepted type of input expressions, it is {@code Expression} for Scala and
 *                  {@code Object} for Java. Generally the expression DSL works on expressions, the
 *                  reason why Java accepts Object is to remove cumbersome call to {@code lit()} for
 *                  literals. Scala alleviates this problem via implicit conversions.
 * @param <OutType> The produced type of the DSL. It is {@code ApiExpression} for Java and {@code Expression}
 *                  for Scala. In Scala the infix operations are included via implicit conversions. In Java
 *                  we introduced a wrapper that enables the operations without pulling them through the whole stack.
 */
@PublicEvolving
public abstract class BaseExpressions<InType, OutType> {
	protected abstract Expression toExpr();

	protected abstract OutType toApiSpecificExpression(Expression expression);

	/**
	 * Specifies a name for an expression i.e. a field.
	 *
	 * @param name       name for one field
	 * @param extraNames additional names if the expression expands to multiple fields
	 */
	public OutType as(String name, String... extraNames) {
		return toApiSpecificExpression(ApiExpressionUtils.unresolvedCall(
			BuiltInFunctionDefinitions.AS,
			Stream.concat(
				Stream.of(toExpr(), ApiExpressionUtils.valueLiteral(name)),
				Stream.of(extraNames).map(ApiExpressionUtils::valueLiteral)
			).toArray(Expression[]::new)));
	}

	/**
	 * Boolean AND in three-valued logic. This is an infix notation. See also
	 * {@link Expressions#and(Object, Object, Object...)} for prefix notation with multiple arguments.
	 *
	 * @see Expressions#and(Object, Object, Object...)
	 */
	public OutType and(InType other) {
		return toApiSpecificExpression(unresolvedCall(AND, toExpr(), objectToExpression(other)));
	}

	/**
	 * Boolean OR in three-valued logic. This is an infix notation. See also
	 * {@link Expressions#or(Object, Object, Object...)} for prefix notation with multiple arguments.
	 *
	 * @see Expressions#or(Object, Object, Object...)
	 */
	public OutType or(InType other) {
		return toApiSpecificExpression(unresolvedCall(OR, toExpr(), objectToExpression(other)));
	}

	/**
	 * Greater than.
	 */
	public OutType isGreater(InType other) {
		return toApiSpecificExpression(unresolvedCall(GREATER_THAN, toExpr(), objectToExpression(other)));
	}

	/**
	 * Greater than or equal.
	 */
	public OutType isGreaterOrEqual(InType other) {
		return toApiSpecificExpression(unresolvedCall(GREATER_THAN_OR_EQUAL, toExpr(), objectToExpression(other)));
	}

	/**
	 * Less than.
	 */
	public OutType isLess(InType other) {
		return toApiSpecificExpression(unresolvedCall(LESS_THAN, toExpr(), objectToExpression(other)));
	}

	/**
	 * Less than or equal.
	 */
	public OutType isLessOrEqual(InType other) {
		return toApiSpecificExpression(unresolvedCall(LESS_THAN_OR_EQUAL, toExpr(), objectToExpression(other)));
	}

	/**
	 * Equals.
	 */
	public OutType isEqual(InType other) {
		return toApiSpecificExpression(unresolvedCall(EQUALS, toExpr(), objectToExpression(other)));
	}

	/**
	 * Not equal.
	 */
	public OutType isNotEqual(InType other) {
		return toApiSpecificExpression(unresolvedCall(NOT_EQUALS, toExpr(), objectToExpression(other)));
	}

	/**
	 * Returns left plus right.
	 */
	public OutType plus(InType other) {
		return toApiSpecificExpression(unresolvedCall(PLUS, toExpr(), objectToExpression(other)));
	}

	/**
	 * Returns left minus right.
	 */
	public OutType minus(InType other) {
		return toApiSpecificExpression(unresolvedCall(MINUS, toExpr(), objectToExpression(other)));
	}

	/**
	 * Returns left divided by right.
	 */
	public OutType dividedBy(InType other) {
		return toApiSpecificExpression(unresolvedCall(DIVIDE, toExpr(), objectToExpression(other)));
	}

	/**
	 * Returns left multiplied by right.
	 */
	public OutType times(InType other) {
		return toApiSpecificExpression(unresolvedCall(TIMES, toExpr(), objectToExpression(other)));
	}

	/**
	 * Returns true if the given expression is between lowerBound and upperBound (both inclusive).
	 * False otherwise. The parameters must be numeric types or identical comparable types.
	 *
	 * @param lowerBound numeric or comparable expression
	 * @param upperBound numeric or comparable expression
	 */
	public OutType between(InType lowerBound, InType upperBound) {
		return toApiSpecificExpression(unresolvedCall(
			BETWEEN,
			toExpr(),
			objectToExpression(lowerBound),
			objectToExpression(upperBound)));
	}

	/**
	 * Returns true if the given expression is not between lowerBound and upperBound (both
	 * inclusive). False otherwise. The parameters must be numeric types or identical
	 * comparable types.
	 *
	 * @param lowerBound numeric or comparable expression
	 * @param upperBound numeric or comparable expression
	 */
	public OutType notBetween(InType lowerBound, InType upperBound) {
		return toApiSpecificExpression(unresolvedCall(
			NOT_BETWEEN,
			toExpr(),
			objectToExpression(lowerBound),
			objectToExpression(upperBound)));
	}

	/**
	 * Ternary conditional operator that decides which of two other expressions should be evaluated
	 * based on a evaluated boolean condition.
	 *
	 * <p>e.g. lit(42).isGreater(5).then("A", "B") leads to "A"
	 *
	 * @param ifTrue expression to be evaluated if condition holds
	 * @param ifFalse expression to be evaluated if condition does not hold
	 */
	public OutType then(InType ifTrue, InType ifFalse) {
		return toApiSpecificExpression(unresolvedCall(
			IF,
			toExpr(),
			objectToExpression(ifTrue),
			objectToExpression(ifFalse)));
	}

	/**
	 * Returns true if the given expression is null.
	 */
	public OutType isNull() {
		return toApiSpecificExpression(unresolvedCall(IS_NULL, toExpr()));
	}

	/**
	 * Returns true if the given expression is not null.
	 */
	public OutType isNotNull() {
		return toApiSpecificExpression(unresolvedCall(IS_NOT_NULL, toExpr()));
	}

	/**
	 * Returns true if given boolean expression is true. False otherwise (for null and false).
	 */
	public OutType isTrue() {
		return toApiSpecificExpression(unresolvedCall(IS_TRUE, toExpr()));
	}

	/**
	 * Returns true if given boolean expression is false. False otherwise (for null and true).
	 */
	public OutType isFalse() {
		return toApiSpecificExpression(unresolvedCall(IS_FALSE, toExpr()));
	}

	/**
	 * Returns true if given boolean expression is not true (for null and false). False otherwise.
	 */
	public OutType isNotTrue() {
		return toApiSpecificExpression(unresolvedCall(IS_NOT_TRUE, toExpr()));
	}

	/**
	 * Returns true if given boolean expression is not false (for null and true). False otherwise.
	 */
	public OutType isNotFalse() {
		return toApiSpecificExpression(unresolvedCall(IS_NOT_FALSE, toExpr()));
	}

	/**
	 * Similar to a SQL distinct aggregation clause such as COUNT(DISTINCT a), declares that an
	 * aggregation function is only applied on distinct input values.
	 *
	 * <p>For example:
	 * <pre>
	 * {@code
	 * orders
	 *  .groupBy($("a"))
	 *  .select($("a"), $("b").sum().distinct().as("d"))
	 * }
	 * </pre>
	 */
	public OutType distinct() {
		return toApiSpecificExpression(unresolvedCall(DISTINCT, toExpr()));
	}

	/**
	 * Returns the sum of the numeric field across all input values.
	 * If all values are null, null is returned.
	 */
	public OutType sum() {
		return toApiSpecificExpression(unresolvedCall(SUM, toExpr()));
	}

	/**
	 * Returns the sum of the numeric field across all input values.
	 * If all values are null, 0 is returned.
	 */
	public OutType sum0() {
		return toApiSpecificExpression(unresolvedCall(SUM0, toExpr()));
	}

	/**
	 * Returns the minimum value of field across all input values.
	 */
	public OutType min() {
		return toApiSpecificExpression(unresolvedCall(MIN, toExpr()));
	}

	/**
	 * Returns the maximum value of field across all input values.
	 */
	public OutType max() {
		return toApiSpecificExpression(unresolvedCall(MAX, toExpr()));
	}

	/**
	 * Returns the number of input rows for which the field is not null.
	 */
	public OutType count() {
		return toApiSpecificExpression(unresolvedCall(COUNT, toExpr()));
	}

	/**
	 * Returns the average (arithmetic mean) of the numeric field across all input values.
	 */
	public OutType avg() {
		return toApiSpecificExpression(unresolvedCall(AVG, toExpr()));
	}

	/**
	 * Returns the population standard deviation of an expression (the square root of varPop()).
	 */
	public OutType stddevPop() {
		return toApiSpecificExpression(unresolvedCall(STDDEV_POP, toExpr()));
	}

	/**
	 * Returns the sample standard deviation of an expression (the square root of varSamp()).
	 */
	public OutType stddevSamp() {
		return toApiSpecificExpression(unresolvedCall(STDDEV_SAMP, toExpr()));
	}

	/**
	 * Returns the population standard variance of an expression.
	 */
	public OutType varPop() {
		return toApiSpecificExpression(unresolvedCall(VAR_POP, toExpr()));
	}

	/**
	 * Returns the sample variance of a given expression.
	 */
	public OutType varSamp() {
		return toApiSpecificExpression(unresolvedCall(VAR_SAMP, toExpr()));
	}

	/**
	 * Returns multiset aggregate of a given expression.
	 */
	public OutType collect() {
		return toApiSpecificExpression(unresolvedCall(COLLECT, toExpr()));
	}

	/**
	 * Converts a value to a given data type.
	 *
	 * <p>e.g. "42".cast(DataTypes.INT()) leads to 42.
	 */
	public OutType cast(DataType toType) {
		return toApiSpecificExpression(unresolvedCall(CAST, toExpr(), typeLiteral(toType)));
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #cast(DataType)} instead which uses the new type system
	 *             based on {@link org.apache.flink.table.api.DataTypes}. Please make sure to use either the old
	 *             or the new type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public OutType cast(TypeInformation<?> toType) {
		return toApiSpecificExpression(unresolvedCall(CAST, toExpr(), typeLiteral(fromLegacyInfoToDataType(toType))));
	}

	/**
	 * Specifies ascending order of an expression i.e. a field for orderBy unresolvedCall.
	 */
	public OutType asc() {
		return toApiSpecificExpression(unresolvedCall(ORDER_ASC, toExpr()));
	}

	/**
	 * Specifies descending order of an expression i.e. a field for orderBy unresolvedCall.
	 */
	public OutType desc() {
		return toApiSpecificExpression(unresolvedCall(ORDER_DESC, toExpr()));
	}

	/**
	 * Returns true if an expression exists in a given list of expressions. This is a shorthand
	 * for multiple OR conditions.
	 *
	 * <p>If the testing set contains null, the result will be null if the element can not be found
	 * and true if it can be found. If the element is null, the result is always null.
	 *
	 * <p>e.g. lit("42").in(1, 2, 3) leads to false.
	 */
	@SafeVarargs
	public final OutType in(InType... elements) {
		Expression[] args = Stream.concat(
			Stream.of(toExpr()),
			Arrays.stream(elements).map(ApiExpressionUtils::objectToExpression))
			.toArray(Expression[]::new);
		return toApiSpecificExpression(unresolvedCall(IN, args));
	}

	/**
	 * Returns true if an expression exists in a given table sub-query. The sub-query table
	 * must consist of one column. This column must have the same data type as the expression.
	 *
	 * <p>Note: This operation is not supported in a streaming environment yet.
	 */
	public OutType in(Table table) {
		return toApiSpecificExpression(unresolvedCall(IN, toExpr(), tableRef(table.toString(), table)));
	}

	/**
	 * Returns the start time (inclusive) of a window when applied on a window reference.
	 */
	public OutType start() {
		return toApiSpecificExpression(unresolvedCall(WINDOW_START, toExpr()));
	}

	/**
	 * Returns the end time (exclusive) of a window when applied on a window reference.
	 *
	 * <p>e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.
	 */
	public OutType end() {
		return toApiSpecificExpression(unresolvedCall(WINDOW_END, toExpr()));
	}

	/**
	 * Calculates the remainder of division the given number by another one.
	 */
	public OutType mod(InType other) {
		return toApiSpecificExpression(unresolvedCall(MOD, toExpr(), objectToExpression(other)));
	}

	/**
	 * Calculates the Euler's number raised to the given power.
	 */
	public OutType exp() {
		return toApiSpecificExpression(unresolvedCall(EXP, toExpr()));
	}

	/**
	 * Calculates the base 10 logarithm of the given value.
	 */
	public OutType log10() {
		return toApiSpecificExpression(unresolvedCall(LOG10, toExpr()));
	}

	/**
	 * Calculates the base 2 logarithm of the given value.
	 */
	public OutType log2() {
		return toApiSpecificExpression(unresolvedCall(LOG2, toExpr()));
	}

	/**
	 * Calculates the natural logarithm of the given value.
	 */
	public OutType ln() {
		return toApiSpecificExpression(unresolvedCall(LN, toExpr()));
	}

	/**
	 * Calculates the natural logarithm of the given value.
	 */
	public OutType log() {
		return toApiSpecificExpression(unresolvedCall(LOG, toExpr()));
	}

	/**
	 * Calculates the logarithm of the given value to the given base.
	 */
	public OutType log(InType base) {
		return toApiSpecificExpression(unresolvedCall(LOG, objectToExpression(base), toExpr()));
	}

	/**
	 * Calculates the given number raised to the power of the other value.
	 */
	public OutType power(InType other) {
		return toApiSpecificExpression(unresolvedCall(POWER, toExpr(), objectToExpression(other)));
	}

	/**
	 * Calculates the hyperbolic cosine of a given value.
	 */
	public OutType cosh() {
		return toApiSpecificExpression(unresolvedCall(COSH, toExpr()));
	}

	/**
	 * Calculates the square root of a given value.
	 */
	public OutType sqrt() {
		return toApiSpecificExpression(unresolvedCall(SQRT, toExpr()));
	}

	/**
	 * Calculates the absolute value of given value.
	 */
	public OutType abs() {
		return toApiSpecificExpression(unresolvedCall(ABS, toExpr()));
	}

	/**
	 * Calculates the largest integer less than or equal to a given number.
	 */
	public OutType floor() {
		return toApiSpecificExpression(unresolvedCall(FLOOR, toExpr()));
	}

	/**
	 * Calculates the hyperbolic sine of a given value.
	 */
	public OutType sinh() {
		return toApiSpecificExpression(unresolvedCall(SINH, toExpr()));
	}

	/**
	 * Calculates the smallest integer greater than or equal to a given number.
	 */
	public OutType ceil() {
		return toApiSpecificExpression(unresolvedCall(CEIL, toExpr()));
	}

	/**
	 * Calculates the sine of a given number.
	 */
	public OutType sin() {
		return toApiSpecificExpression(unresolvedCall(SIN, toExpr()));
	}

	/**
	 * Calculates the cosine of a given number.
	 */
	public OutType cos() {
		return toApiSpecificExpression(unresolvedCall(COS, toExpr()));
	}

	/**
	 * Calculates the tangent of a given number.
	 */
	public OutType tan() {
		return toApiSpecificExpression(unresolvedCall(TAN, toExpr()));
	}

	/**
	 * Calculates the cotangent of a given number.
	 */
	public OutType cot() {
		return toApiSpecificExpression(unresolvedCall(COT, toExpr()));
	}

	/**
	 * Calculates the arc sine of a given number.
	 */
	public OutType asin() {
		return toApiSpecificExpression(unresolvedCall(ASIN, toExpr()));
	}

	/**
	 * Calculates the arc cosine of a given number.
	 */
	public OutType acos() {
		return toApiSpecificExpression(unresolvedCall(ACOS, toExpr()));
	}

	/**
	 * Calculates the arc tangent of a given number.
	 */
	public OutType atan() {
		return toApiSpecificExpression(unresolvedCall(ATAN, toExpr()));
	}

	/**
	 * Calculates the hyperbolic tangent of a given number.
	 */
	public OutType tanh() {
		return toApiSpecificExpression(unresolvedCall(TANH, toExpr()));
	}

	/**
	 * Converts numeric from radians to degrees.
	 */
	public OutType degrees() {
		return toApiSpecificExpression(unresolvedCall(DEGREES, toExpr()));
	}

	/**
	 * Converts numeric from degrees to radians.
	 */
	public OutType radians() {
		return toApiSpecificExpression(unresolvedCall(RADIANS, toExpr()));
	}

	/**
	 * Calculates the signum of a given number.
	 */
	public OutType sign() {
		return toApiSpecificExpression(unresolvedCall(SIGN, toExpr()));
	}

	/**
	 * Rounds the given number to integer places right to the decimal point.
	 */
	public OutType round(InType places) {
		return toApiSpecificExpression(unresolvedCall(ROUND, toExpr(), objectToExpression(places)));
	}

	/**
	 * Returns a string representation of an integer numeric value in binary format. Returns null if
	 * numeric is null. E.g. "4" leads to "100", "12" leads to "1100".
	 */
	public OutType bin() {
		return toApiSpecificExpression(unresolvedCall(BIN, toExpr()));
	}

	/**
	 * Returns a string representation of an integer numeric value or a string in hex format. Returns
	 * null if numeric or string is null.
	 *
	 * <p>E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world" leads
	 * to "68656c6c6f2c776f726c64".
	 */
	public OutType hex() {
		return toApiSpecificExpression(unresolvedCall(HEX, toExpr()));
	}

	/**
	 * Returns a number of truncated to n decimal places.
	 * If n is 0,the result has no decimal point or fractional part.
	 * n can be negative to cause n digits left of the decimal point of the value to become zero.
	 * E.g. truncate(42.345, 2) to 42.34.
	 */
	public OutType truncate(InType n) {
		return toApiSpecificExpression(unresolvedCall(TRUNCATE, toExpr(), objectToExpression(n)));
	}

	/**
	 * Returns a number of truncated to 0 decimal places.
	 * E.g. truncate(42.345) to 42.0.
	 */
	public OutType truncate() {
		return toApiSpecificExpression(unresolvedCall(TRUNCATE, toExpr()));
	}

	// String operations

	/**
	 * Creates a substring of the given string at given index for a given length.
	 *
	 * @param beginIndex first character of the substring (starting at 1, inclusive)
	 * @param length number of characters of the substring
	 */
	public OutType substring(InType beginIndex, InType length) {
		return toApiSpecificExpression(unresolvedCall(SUBSTRING, toExpr(), objectToExpression(beginIndex), objectToExpression(length)));
	}

	/**
	 * Creates a substring of the given string beginning at the given index to the end.
	 *
	 * @param beginIndex first character of the substring (starting at 1, inclusive)
	 */
	public OutType substring(InType beginIndex) {
		return toApiSpecificExpression(unresolvedCall(SUBSTRING, toExpr(), objectToExpression(beginIndex)));
	}

	/**
	 * Removes leading space characters from the given string.
	 */
	public OutType trimLeading() {
		return toApiSpecificExpression(unresolvedCall(
			TRIM,
			valueLiteral(true),
			valueLiteral(false),
			valueLiteral(" "),
			toExpr()));
	}

	/**
	 * Removes leading characters from the given string.
	 *
	 * @param character string containing the character
	 */
	public OutType trimLeading(InType character) {
		return toApiSpecificExpression(unresolvedCall(
			TRIM,
			valueLiteral(true),
			valueLiteral(false),
			objectToExpression(character),
			toExpr()));
	}

	/**
	 * Removes trailing space characters from the given string.
	 */
	public OutType trimTrailing() {
		return toApiSpecificExpression(unresolvedCall(
			TRIM,
			valueLiteral(false),
			valueLiteral(true),
			valueLiteral(" "),
			toExpr()));
	}

	/**
	 * Removes trailing characters from the given string.
	 *
	 * @param character string containing the character
	 */
	public OutType trimTrailing(InType character) {
		return toApiSpecificExpression(unresolvedCall(
			TRIM,
			valueLiteral(false),
			valueLiteral(true),
			objectToExpression(character),
			toExpr()));
	}

	/**
	 * Removes leading and trailing space characters from the given string.
	 */
	public OutType trim() {
		return toApiSpecificExpression(unresolvedCall(
			TRIM,
			valueLiteral(true),
			valueLiteral(true),
			valueLiteral(" "),
			toExpr()));
	}

	/**
	 * Removes leading and trailing characters from the given string.
	 *
	 * @param character string containing the character
	 */
	public OutType trim(InType character) {
		return toApiSpecificExpression(unresolvedCall(
			TRIM,
			valueLiteral(true),
			valueLiteral(true),
			objectToExpression(character),
			toExpr()));
	}

	/**
	 * Returns a new string which replaces all the occurrences of the search target
	 * with the replacement string (non-overlapping).
	 */
	public OutType replace(InType search, InType replacement) {
		return toApiSpecificExpression(unresolvedCall(REPLACE, toExpr(), objectToExpression(search), objectToExpression(replacement)));
	}

	/**
	 * Returns the length of a string.
	 */
	public OutType charLength() {
		return toApiSpecificExpression(unresolvedCall(CHAR_LENGTH, toExpr()));
	}

	/**
	 * Returns all of the characters in a string in upper case using the rules of
	 * the default locale.
	 */
	public OutType upperCase() {
		return toApiSpecificExpression(unresolvedCall(UPPER, toExpr()));
	}

	/**
	 * Returns all of the characters in a string in lower case using the rules of
	 * the default locale.
	 */
	public OutType lowerCase() {
		return toApiSpecificExpression(unresolvedCall(LOWER, toExpr()));
	}

	/**
	 * Converts the initial letter of each word in a string to uppercase.
	 * Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.
	 */
	public OutType initCap() {
		return toApiSpecificExpression(unresolvedCall(INIT_CAP, toExpr()));
	}

	/**
	 * Returns true, if a string matches the specified LIKE pattern.
	 *
	 * <p>e.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n"
	 */
	public OutType like(InType pattern) {
		return toApiSpecificExpression(unresolvedCall(LIKE, toExpr(), objectToExpression(pattern)));
	}

	/**
	 * Returns true, if a string matches the specified SQL regex pattern.
	 *
	 * <p>e.g. "A+" matches all strings that consist of at least one A
	 */
	public OutType similar(InType pattern) {
		return toApiSpecificExpression(unresolvedCall(SIMILAR, toExpr(), objectToExpression(pattern)));
	}

	/**
	 * Returns the position of string in an other string starting at 1.
	 * Returns 0 if string could not be found.
	 *
	 * <p>e.g. lit("a").position("bbbbba") leads to 6
	 */
	public OutType position(InType haystack) {
		return toApiSpecificExpression(unresolvedCall(POSITION, toExpr(), objectToExpression(haystack)));
	}

	/**
	 * Returns a string left-padded with the given pad string to a length of len characters. If
	 * the string is longer than len, the return value is shortened to len characters.
	 *
	 * <p>e.g. lit("hi").lpad(4, "??") returns "??hi",  lit("hi").lpad(1, '??') returns "h"
	 */
	public OutType lpad(InType len, InType pad) {
		return toApiSpecificExpression(unresolvedCall(LPAD, toExpr(), objectToExpression(len), objectToExpression(pad)));
	}

	/**
	 * Returns a string right-padded with the given pad string to a length of len characters. If
	 * the string is longer than len, the return value is shortened to len characters.
	 *
	 * <p>e.g. lit("hi").rpad(4, "??") returns "hi??",  lit("hi").rpad(1, '??') returns "h"
	 */
	public OutType rpad(InType len, InType pad) {
		return toApiSpecificExpression(unresolvedCall(RPAD, toExpr(), objectToExpression(len), objectToExpression(pad)));
	}

	/**
	 * Defines an aggregation to be used for a previously specified over window.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 * {@code
	 * table
	 *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
	 *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
	 * }</pre>
	 */
	public OutType over(InType alias) {
		return toApiSpecificExpression(unresolvedCall(OVER, toExpr(), objectToExpression(alias)));
	}

	/**
	 * Replaces a substring of string with a string starting at a position (starting at 1).
	 *
	 * <p>e.g. lit("xxxxxtest").overlay("xxxx", 6) leads to "xxxxxxxxx"
	 */
	public OutType overlay(InType newString, InType starting) {
		return toApiSpecificExpression(unresolvedCall(
			OVERLAY,
			toExpr(),
			objectToExpression(newString),
			objectToExpression(starting)));
	}

	/**
	 * Replaces a substring of string with a string starting at a position (starting at 1).
	 * The length specifies how many characters should be removed.
	 *
	 * <p>e.g. lit("xxxxxtest").overlay("xxxx", 6, 2) leads to "xxxxxxxxxst"
	 */
	public OutType overlay(InType newString, InType starting, InType length) {
		return toApiSpecificExpression(unresolvedCall(
			OVERLAY,
			toExpr(),
			objectToExpression(newString),
			objectToExpression(starting),
			objectToExpression(length)));
	}

	/**
	 * Returns a string with all substrings that match the regular expression consecutively
	 * being replaced.
	 */
	public OutType regexpReplace(InType regex, InType replacement) {
		return toApiSpecificExpression(unresolvedCall(
			REGEXP_REPLACE,
			toExpr(),
			objectToExpression(regex),
			objectToExpression(replacement)));
	}

	/**
	 * Returns a string extracted with a specified regular expression and a regex match group
	 * index.
	 */
	public OutType regexpExtract(InType regex, InType extractIndex) {
		return toApiSpecificExpression(unresolvedCall(
			REGEXP_EXTRACT,
			toExpr(),
			objectToExpression(regex),
			objectToExpression(extractIndex)));
	}

	/**
	 * Returns a string extracted with a specified regular expression.
	 */
	public OutType regexpExtract(InType regex) {
		return toApiSpecificExpression(unresolvedCall(REGEXP_EXTRACT, toExpr(), objectToExpression(regex)));
	}

	/**
	 * Returns the base string decoded with base64.
	 */
	public OutType fromBase64() {
		return toApiSpecificExpression(unresolvedCall(FROM_BASE64, toExpr()));
	}

	/**
	 * Returns the base64-encoded result of the input string.
	 */
	public OutType toBase64() {
		return toApiSpecificExpression(unresolvedCall(TO_BASE64, toExpr()));
	}

	/**
	 * Returns a string that removes the left whitespaces from the given string.
	 */
	public OutType ltrim() {
		return toApiSpecificExpression(unresolvedCall(LTRIM, toExpr()));
	}

	/**
	 * Returns a string that removes the right whitespaces from the given string.
	 */
	public OutType rtrim() {
		return toApiSpecificExpression(unresolvedCall(RTRIM, toExpr()));
	}

	/**
	 * Returns a string that repeats the base string n times.
	 */
	public OutType repeat(InType n) {
		return toApiSpecificExpression(unresolvedCall(REPEAT, toExpr(), objectToExpression(n)));
	}

	// Temporal operations

	/**
	 * Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
	 */
	public OutType toDate() {
		return toApiSpecificExpression(unresolvedCall(
			CAST,
			toExpr(),
			typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.DATE))));
	}

	/**
	 * Parses a time string in the form "HH:mm:ss" to a SQL Time.
	 */
	public OutType toTime() {
		return toApiSpecificExpression(unresolvedCall(
			CAST,
			toExpr(),
			typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIME))));
	}

	/**
	 * Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
	 */
	public OutType toTimestamp() {
		return toApiSpecificExpression(unresolvedCall(
			CAST,
			toExpr(),
			typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIMESTAMP))));
	}

	/**
	 * Extracts parts of a time point or time interval. Returns the part as a long value.
	 *
	 * <p>e.g. lit("2006-06-05").toDate().extract(DAY) leads to 5
	 */
	public OutType extract(TimeIntervalUnit timeIntervalUnit) {
		return toApiSpecificExpression(unresolvedCall(EXTRACT, valueLiteral(timeIntervalUnit), toExpr()));
	}

	/**
	 * Rounds down a time point to the given unit.
	 *
	 * <p>e.g. lit("12:44:31").toDate().floor(MINUTE) leads to 12:44:00
	 */
	public OutType floor(TimeIntervalUnit timeIntervalUnit) {
		return toApiSpecificExpression(unresolvedCall(FLOOR, toExpr(), valueLiteral(timeIntervalUnit)));
	}

	/**
	 * Rounds up a time point to the given unit.
	 *
	 * <p>e.g. lit("12:44:31").toDate().ceil(MINUTE) leads to 12:45:00
	 */
	public OutType ceil(TimeIntervalUnit timeIntervalUnit) {
		return toApiSpecificExpression(unresolvedCall(
			CEIL,
			toExpr(),
			valueLiteral(timeIntervalUnit)));
	}

	// Advanced type helper functions

	/**
	 * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name and
	 * returns it's value.
	 *
	 * @param name name of the field (similar to Flink's field expressions)
	 */
	public OutType get(String name) {
		return toApiSpecificExpression(unresolvedCall(GET, toExpr(), valueLiteral(name)));
	}

	/**
	 * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index and
	 * returns it's value.
	 *
	 * @param index position of the field
	 */
	public OutType get(int index) {
		return toApiSpecificExpression(unresolvedCall(GET, toExpr(), valueLiteral(index)));
	}

	/**
	 * Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
	 * into a flat representation where every subtype is a separate field.
	 */
	public OutType flatten() {
		return toApiSpecificExpression(unresolvedCall(FLATTEN, toExpr()));
	}

	/**
	 * Accesses the element of an array or map based on a key or an index (starting at 1).
	 *
	 * @param index key or position of the element (array index starting at 1)
	 */
	public OutType at(InType index) {
		return toApiSpecificExpression(unresolvedCall(AT, toExpr(), objectToExpression(index)));
	}

	/**
	 * Returns the number of elements of an array or number of entries of a map.
	 */
	public OutType cardinality() {
		return toApiSpecificExpression(unresolvedCall(CARDINALITY, toExpr()));
	}

	/**
	 * Returns the sole element of an array with a single element. Returns null if the array is
	 * empty. Throws an exception if the array has more than one element.
	 */
	public OutType element() {
		return toApiSpecificExpression(unresolvedCall(ARRAY_ELEMENT, toExpr()));
	}

	// Time definition

	/**
	 * Declares a field as the rowtime attribute for indicating, accessing, and working in
	 * Flink's event time.
	 */
	public OutType rowtime() {
		return toApiSpecificExpression(unresolvedCall(ROWTIME, toExpr()));
	}

	/**
	 * Declares a field as the proctime attribute for indicating, accessing, and working in
	 * Flink's processing time.
	 */
	public OutType proctime() {
		return toApiSpecificExpression(unresolvedCall(PROCTIME, toExpr()));
	}

	/**
	 * Creates an interval of the given number of years.
	 *
	 * <p>The produced expression is of type {@code DataTypes.INTERVAL}
	 */
	public OutType year() {
		return toApiSpecificExpression(toMonthInterval(toExpr(), 12));
	}

	/**
	 * Creates an interval of the given number of years.
	 */
	public OutType years() {
		return year();
	}

	/**
	 * Creates an interval of the given number of quarters.
	 */
	public OutType quarter() {
		return toApiSpecificExpression(toMonthInterval(toExpr(), 3));
	}

	/**
	 * Creates an interval of the given number of quarters.
	 */
	public OutType quarters() {
		return quarter();
	}

	/**
	 * Creates an interval of the given number of months.
	 */
	public OutType month() {
		return toApiSpecificExpression(toMonthInterval(toExpr(), 1));
	}

	/**
	 * Creates an interval of the given number of months.
	 */
	public OutType months() {
		return month();
	}

	/**
	 * Creates an interval of the given number of weeks.
	 */
	public OutType week() {
		return toApiSpecificExpression(toMilliInterval(toExpr(), 7 * MILLIS_PER_DAY));
	}

	/**
	 * Creates an interval of the given number of weeks.
	 */
	public OutType weeks() {
		return week();
	}

	/**
	 * Creates an interval of the given number of days.
	 */
	public OutType day() {
		return toApiSpecificExpression(toMilliInterval(toExpr(), MILLIS_PER_DAY));
	}

	/**
	 * Creates an interval of the given number of days.
	 */
	public OutType days() {
		return day();
	}

	/**
	 * Creates an interval of the given number of hours.
	 */
	public OutType hour() {
		return toApiSpecificExpression(toMilliInterval(toExpr(), MILLIS_PER_HOUR));
	}

	/**
	 * Creates an interval of the given number of hours.
	 */
	public OutType hours() {
		return hour();
	}

	/**
	 * Creates an interval of the given number of minutes.
	 */
	public OutType minute() {
		return toApiSpecificExpression(toMilliInterval(toExpr(), MILLIS_PER_MINUTE));
	}

	/**
	 * Creates an interval of the given number of minutes.
	 */
	public OutType minutes() {
		return minute();
	}

	/**
	 * Creates an interval of the given number of seconds.
	 */
	public OutType second() {
		return toApiSpecificExpression(toMilliInterval(toExpr(), MILLIS_PER_SECOND));
	}

	/**
	 * Creates an interval of the given number of seconds.
	 */
	public OutType seconds() {
		return second();
	}

	/**
	 * Creates an interval of the given number of milliseconds.
	 */
	public OutType milli() {
		return toApiSpecificExpression(toMilliInterval(toExpr(), 1));
	}

	/**
	 * Creates an interval of the given number of milliseconds.
	 */
	public OutType millis() {
		return milli();
	}

	// Hash functions

	/**
	 * Returns the MD5 hash of the string argument; null if string is null.
	 *
	 * @return string of 32 hexadecimal digits or null
	 */
	public OutType md5() {
		return toApiSpecificExpression(unresolvedCall(MD5, toExpr()));
	}

	/**
	 * Returns the SHA-1 hash of the string argument; null if string is null.
	 *
	 * @return string of 40 hexadecimal digits or null
	 */
	public OutType sha1() {
		return toApiSpecificExpression(unresolvedCall(SHA1, toExpr()));
	}

	/**
	 * Returns the SHA-224 hash of the string argument; null if string is null.
	 *
	 * @return string of 56 hexadecimal digits or null
	 */
	public OutType sha224() {
		return toApiSpecificExpression(unresolvedCall(SHA224, toExpr()));
	}

	/**
	 * Returns the SHA-256 hash of the string argument; null if string is null.
	 *
	 * @return string of 64 hexadecimal digits or null
	 */
	public OutType sha256() {
		return toApiSpecificExpression(unresolvedCall(SHA256, toExpr()));
	}

	/**
	 * Returns the SHA-384 hash of the string argument; null if string is null.
	 *
	 * @return string of 96 hexadecimal digits or null
	 */
	public OutType sha384() {
		return toApiSpecificExpression(unresolvedCall(SHA384, toExpr()));
	}

	/**
	 * Returns the SHA-512 hash of the string argument; null if string is null.
	 *
	 * @return string of 128 hexadecimal digits or null
	 */
	public OutType sha512() {
		return toApiSpecificExpression(unresolvedCall(SHA512, toExpr()));
	}

	/**
	 * Returns the hash for the given string expression using the SHA-2 family of hash
	 * functions (SHA-224, SHA-256, SHA-384, or SHA-512).
	 *
	 * @param hashLength bit length of the result (either 224, 256, 384, or 512)
	 * @return string or null if one of the arguments is null.
	 */
	public OutType sha2(InType hashLength) {
		return toApiSpecificExpression(unresolvedCall(SHA2, toExpr(), objectToExpression(hashLength)));
	}
}

