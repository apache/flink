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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.types.utils.ValueDataTypeConverter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.objectToExpression;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;

/**
 * Entry point of the Table API Expression DSL such as: {@code $("myField").plus(10).abs()}
 *
 * <p>This class contains static methods for referencing table columns, creating literals, and
 * building more complex {@link Expression} chains. {@link ApiExpression ApiExpressions} are pure
 * API entities that are further translated into {@link ResolvedExpression ResolvedExpressions}
 * under the hood.
 *
 * <p>For fluent definition of expressions and easier readability, we recommend to add a star import
 * to the methods of this class:
 *
 * <pre>
 * import static org.apache.flink.table.api.Expressions.*;
 * </pre>
 *
 * <p>Check the documentation for more programming language specific APIs, for example, by using
 * Scala implicits.
 */
@PublicEvolving
public final class Expressions {
    /**
     * Creates an unresolved reference to a table's field.
     *
     * <p>Example:
     *
     * <pre>{@code
     * tab.select($("key"), $("value"))
     * }</pre>
     */
    // CHECKSTYLE.OFF: MethodName
    public static ApiExpression $(String name) {
        return new ApiExpression(unresolvedRef(name));
    }
    // CHECKSTYLE.ON: MethodName

    /**
     * Creates a SQL literal.
     *
     * <p>The data type is derived from the object's class and its value.
     *
     * <p>For example:
     *
     * <ul>
     *   <li>{@code lit(12)} leads to {@code INT}
     *   <li>{@code lit("abc")} leads to {@code CHAR(3)}
     *   <li>{@code lit(new BigDecimal("123.45"))} leads to {@code DECIMAL(5, 2)}
     * </ul>
     *
     * <p>See {@link ValueDataTypeConverter} for a list of supported literal values.
     */
    public static ApiExpression lit(Object v) {
        return new ApiExpression(valueLiteral(v));
    }

    /**
     * Creates a SQL literal of a given {@link DataType}.
     *
     * <p>The method {@link #lit(Object)} is preferred as it extracts the {@link DataType}
     * automatically. Use this method only when necessary. The class of {@code v} must be supported
     * according to the {@link
     * org.apache.flink.table.types.logical.LogicalType#supportsInputConversion(Class)}.
     */
    public static ApiExpression lit(Object v, DataType dataType) {
        return new ApiExpression(valueLiteral(v, dataType));
    }

    /**
     * Indicates a range from 'start' to 'end', which can be used in columns selection.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table table = ...
     * table.select(withColumns(range(b, c)))
     * }</pre>
     *
     * @see #withColumns(Object, Object...)
     * @see #withoutColumns(Object, Object...)
     */
    public static ApiExpression range(String start, String end) {
        return apiCall(
                BuiltInFunctionDefinitions.RANGE_TO, unresolvedRef(start), unresolvedRef(end));
    }

    /**
     * Indicates an index based range, which can be used in columns selection.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table table = ...
     * table.select(withColumns(range(3, 4)))
     * }</pre>
     *
     * @see #withColumns(Object, Object...)
     * @see #withoutColumns(Object, Object...)
     */
    public static ApiExpression range(int start, int end) {
        return apiCall(BuiltInFunctionDefinitions.RANGE_TO, valueLiteral(start), valueLiteral(end));
    }

    /** Boolean AND in three-valued logic. */
    public static ApiExpression and(Object predicate0, Object predicate1, Object... predicates) {
        return apiCallAtLeastTwoArgument(
                BuiltInFunctionDefinitions.AND, predicate0, predicate1, predicates);
    }

    /** Boolean OR in three-valued logic. */
    public static ApiExpression or(Object predicate0, Object predicate1, Object... predicates) {
        return apiCallAtLeastTwoArgument(
                BuiltInFunctionDefinitions.OR, predicate0, predicate1, predicates);
    }

    /**
     * Offset constant to be used in the {@code preceding} clause of unbounded {@code Over} windows.
     * Use this constant for a time interval. Unbounded over windows start with the first row of a
     * partition.
     */
    public static final ApiExpression UNBOUNDED_ROW =
            apiCall(BuiltInFunctionDefinitions.UNBOUNDED_ROW);

    /**
     * Offset constant to be used in the {@code preceding} clause of unbounded {@link Over} windows.
     * Use this constant for a row-count interval. Unbounded over windows start with the first row
     * of a partition.
     */
    public static final ApiExpression UNBOUNDED_RANGE =
            apiCall(BuiltInFunctionDefinitions.UNBOUNDED_RANGE);

    /**
     * Offset constant to be used in the {@code following} clause of {@link Over} windows. Use this
     * for setting the upper bound of the window to the current row.
     */
    public static final ApiExpression CURRENT_ROW = apiCall(BuiltInFunctionDefinitions.CURRENT_ROW);

    /**
     * Offset constant to be used in the {@code following} clause of {@link Over} windows. Use this
     * for setting the upper bound of the window to the sort key of the current row, i.e., all rows
     * with the same sort key as the current row are included in the window.
     */
    public static final ApiExpression CURRENT_RANGE =
            apiCall(BuiltInFunctionDefinitions.CURRENT_RANGE);

    /**
     * Returns the current SQL date in local time zone, the return type of this expression is {@link
     * DataTypes#DATE()}.
     */
    public static ApiExpression currentDate() {
        return apiCall(BuiltInFunctionDefinitions.CURRENT_DATE);
    }

    /**
     * Returns the current SQL time in local time zone, the return type of this expression is {@link
     * DataTypes#TIME()}.
     */
    public static ApiExpression currentTime() {
        return apiCall(BuiltInFunctionDefinitions.CURRENT_TIME);
    }

    /**
     * Returns the current SQL timestamp in local time zone, the return type of this expression is
     * {@link DataTypes#TIMESTAMP_WITH_LOCAL_TIME_ZONE()}.
     */
    public static ApiExpression currentTimestamp() {
        return apiCall(BuiltInFunctionDefinitions.CURRENT_TIMESTAMP);
    }

    /**
     * Returns the current watermark for the given rowtime attribute, or {@code NULL} if no common
     * watermark of all upstream operations is available at the current operation in the pipeline.
     *
     * <p>The function returns the watermark with the same type as the rowtime attribute, but with
     * an adjusted precision of 3. For example, if the rowtime attribute is {@link
     * DataTypes#TIMESTAMP_LTZ(int) TIMESTAMP_LTZ(9)}, the function will return {@link
     * DataTypes#TIMESTAMP_LTZ(int) TIMESTAMP_LTZ(3)}.
     *
     * <p>If no watermark has been emitted yet, the function will return {@code NULL}. Users must
     * take care of this when comparing against it, e.g. in order to filter out late data you can
     * use
     *
     * <pre>{@code
     * WHERE CURRENT_WATERMARK(ts) IS NULL OR ts > CURRENT_WATERMARK(ts)
     * }</pre>
     */
    public static ApiExpression currentWatermark(Object rowtimeAttribute) {
        return apiCall(BuiltInFunctionDefinitions.CURRENT_WATERMARK, rowtimeAttribute);
    }

    /**
     * Returns the current SQL time in local time zone, the return type of this expression is {@link
     * DataTypes#TIME()}, this is a synonym for {@link Expressions#currentTime()}.
     */
    public static ApiExpression localTime() {
        return apiCall(BuiltInFunctionDefinitions.LOCAL_TIME);
    }

    /**
     * Returns the current SQL timestamp in local time zone, the return type of this expression is
     * {@link DataTypes#TIMESTAMP()}.
     */
    public static ApiExpression localTimestamp() {
        return apiCall(BuiltInFunctionDefinitions.LOCAL_TIMESTAMP);
    }

    /**
     * Converts a numeric type epoch time to {@link DataTypes#TIMESTAMP_LTZ(int)}.
     *
     * <p>The supported precision is 0 or 3:
     *
     * <ul>
     *   <li>0 means the numericEpochTime is in second.
     *   <li>3 means the numericEpochTime is in millisecond.
     * </ul>
     *
     * @param numericEpochTime The epoch time with numeric type.
     * @param precision The precision to indicate the epoch time is in second or millisecond.
     * @return The timestamp value with {@link DataTypes#TIMESTAMP_LTZ(int)} type.
     */
    public static ApiExpression toTimestampLtz(Object numericEpochTime, Object precision) {
        return apiCall(BuiltInFunctionDefinitions.TO_TIMESTAMP_LTZ, numericEpochTime, precision);
    }

    /**
     * Determines whether two anchored time intervals overlap. Time point and temporal are
     * transformed into a range defined by two time points (start, end). The function evaluates
     * <code>leftEnd >= rightStart && rightEnd >= leftStart</code>.
     *
     * <p>It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
     *
     * <p>e.g.
     *
     * <pre>{@code
     * temporalOverlaps(
     *      lit("2:55:00").toTime(),
     *      lit(1).hours(),
     *      lit("3:30:00").toTime(),
     *      lit(2).hours()
     * )
     * }</pre>
     *
     * <p>leads to true
     */
    public static ApiExpression temporalOverlaps(
            Object leftTimePoint,
            Object leftTemporal,
            Object rightTimePoint,
            Object rightTemporal) {
        return apiCall(
                BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS,
                leftTimePoint,
                leftTemporal,
                rightTimePoint,
                rightTemporal);
    }

    /**
     * Formats a timestamp as a string using a specified format. The format must be compatible with
     * MySQL's date formatting syntax as used by the date_parse function.
     *
     * <p>For example {@code dataFormat($("time"), "%Y, %d %M")} results in strings formatted as
     * "2017, 05 May".
     *
     * @param timestamp The timestamp to format as string.
     * @param format The format of the string.
     * @return The formatted timestamp as string.
     */
    public static ApiExpression dateFormat(Object timestamp, Object format) {
        return apiCall(BuiltInFunctionDefinitions.DATE_FORMAT, timestamp, format);
    }

    /**
     * Returns the (signed) number of {@link TimePointUnit} between timePoint1 and timePoint2.
     *
     * <p>For example, {@code timestampDiff(TimePointUnit.DAY, lit("2016-06-15").toDate(),
     * lit("2016-06-18").toDate()} leads to 3.
     *
     * @param timePointUnit The unit to compute diff.
     * @param timePoint1 The first point in time.
     * @param timePoint2 The second point in time.
     * @return The number of intervals as integer value.
     */
    public static ApiExpression timestampDiff(
            TimePointUnit timePointUnit, Object timePoint1, Object timePoint2) {
        return apiCall(
                BuiltInFunctionDefinitions.TIMESTAMP_DIFF,
                valueLiteral(timePointUnit),
                timePoint1,
                timePoint2);
    }

    /** Creates an array of literals. */
    public static ApiExpression array(Object head, Object... tail) {
        return apiCallAtLeastOneArgument(BuiltInFunctionDefinitions.ARRAY, head, tail);
    }

    /** Creates a row of expressions. */
    public static ApiExpression row(Object head, Object... tail) {
        return apiCallAtLeastOneArgument(BuiltInFunctionDefinitions.ROW, head, tail);
    }

    /**
     * Creates a map of expressions.
     *
     * <pre>{@code
     * table.select(
     *     map(
     *         "key1", 1,
     *         "key2", 2,
     *         "key3", 3
     *     ))
     * }</pre>
     *
     * <p>Note keys and values should have the same types for all entries.
     */
    public static ApiExpression map(Object key, Object value, Object... tail) {
        return apiCallAtLeastTwoArgument(BuiltInFunctionDefinitions.MAP, key, value, tail);
    }

    /**
     * Creates an interval of rows.
     *
     * @see Table#window(GroupWindow)
     * @see Table#window(OverWindow...)
     */
    public static ApiExpression rowInterval(Long rows) {
        return new ApiExpression(valueLiteral(rows));
    }

    /** Returns a value that is closer than any other value to pi. */
    public static ApiExpression pi() {
        return apiCall(BuiltInFunctionDefinitions.PI);
    }

    /** Returns a value that is closer than any other value to e. */
    public static ApiExpression e() {
        return apiCall(BuiltInFunctionDefinitions.E);
    }

    /** Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive). */
    public static ApiExpression rand() {
        return apiCall(BuiltInFunctionDefinitions.RAND);
    }

    /**
     * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
     * initial seed. Two rand() functions will return identical sequences of numbers if they have
     * same initial seed.
     */
    public static ApiExpression rand(Object seed) {
        return apiCall(BuiltInFunctionDefinitions.RAND, objectToExpression(seed));
    }

    /**
     * Returns a pseudorandom integer value between 0 (inclusive) and the specified value
     * (exclusive).
     */
    public static ApiExpression randInteger(Object bound) {
        return apiCall(BuiltInFunctionDefinitions.RAND_INTEGER, objectToExpression(bound));
    }

    /**
     * Returns a pseudorandom integer value between 0 (inclusive) and the specified value
     * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
     * of numbers if they have same initial seed and same bound.
     */
    public static ApiExpression randInteger(Object seed, Object bound) {
        return apiCall(
                BuiltInFunctionDefinitions.RAND_INTEGER,
                objectToExpression(seed),
                objectToExpression(bound));
    }

    /**
     * Returns the string that results from concatenating the arguments. Returns NULL if any
     * argument is NULL.
     */
    public static ApiExpression concat(Object string, Object... strings) {
        return apiCallAtLeastOneArgument(BuiltInFunctionDefinitions.CONCAT, string, strings);
    }

    /** Calculates the arc tangent of a given coordinate. */
    public static ApiExpression atan2(Object y, Object x) {
        return apiCallAtLeastOneArgument(BuiltInFunctionDefinitions.ATAN2, y, x);
    }

    /** Returns negative numeric. */
    public static ApiExpression negative(Object v) {
        return apiCall(BuiltInFunctionDefinitions.MINUS_PREFIX, v);
    }

    /**
     * Returns the string that results from concatenating the arguments and separator. Returns NULL
     * If the separator is NULL.
     *
     * <p>Note: this function does not skip empty strings. However, it does skip any NULL values
     * after the separator argument.
     */
    public static ApiExpression concatWs(Object separator, Object string, Object... strings) {
        return apiCallAtLeastTwoArgument(
                BuiltInFunctionDefinitions.CONCAT_WS, separator, string, strings);
    }

    /**
     * Returns an UUID (Universally Unique Identifier) string (e.g.,
     * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
     * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
     * generator.
     */
    public static ApiExpression uuid() {
        return apiCall(BuiltInFunctionDefinitions.UUID);
    }

    /**
     * Returns a null literal value of a given data type.
     *
     * <p>e.g. {@code nullOf(DataTypes.INT())}
     */
    public static ApiExpression nullOf(DataType dataType) {
        return new ApiExpression(valueLiteral(null, dataType));
    }

    /**
     * @deprecated This method will be removed in future versions as it uses the old type system. It
     *     is recommended to use {@link #nullOf(DataType)} instead which uses the new type system
     *     based on {@link DataTypes}. Please make sure to use either the old or the new type system
     *     consistently to avoid unintended behavior. See the website documentation for more
     *     information.
     */
    public static ApiExpression nullOf(TypeInformation<?> typeInfo) {
        return nullOf(TypeConversions.fromLegacyInfoToDataType(typeInfo));
    }

    /** Calculates the logarithm of the given value. */
    public static ApiExpression log(Object value) {
        return apiCall(BuiltInFunctionDefinitions.LOG, value);
    }

    /** Calculates the logarithm of the given value to the given base. */
    public static ApiExpression log(Object base, Object value) {
        return apiCall(BuiltInFunctionDefinitions.LOG, base, value);
    }

    /**
     * Source watermark declaration for {@link Schema}.
     *
     * <p>This is a marker function that doesn't have concrete runtime implementation. It can only
     * be used as a single expression in {@link Schema.Builder#watermark(String, Expression)}. The
     * declaration will be pushed down into a table source that implements the {@link
     * SupportsSourceWatermark} interface. The source will emit system-defined watermarks
     * afterwards.
     *
     * <p>Please check the documentation whether the connector supports source watermarks.
     */
    public static ApiExpression sourceWatermark() {
        return apiCall(BuiltInFunctionDefinitions.SOURCE_WATERMARK);
    }

    /**
     * Ternary conditional operator that decides which of two other expressions should be evaluated
     * based on a evaluated boolean condition.
     *
     * <p>e.g. ifThenElse($("f0") > 5, "A", "B") leads to "A"
     *
     * @param condition boolean condition
     * @param ifTrue expression to be evaluated if condition holds
     * @param ifFalse expression to be evaluated if condition does not hold
     */
    public static ApiExpression ifThenElse(Object condition, Object ifTrue, Object ifFalse) {
        return apiCall(BuiltInFunctionDefinitions.IF, condition, ifTrue, ifFalse);
    }

    /**
     * Creates an expression that selects a range of columns. It can be used wherever an array of
     * expression is accepted such as function calls, projections, or groupings.
     *
     * <p>A range can either be index-based or name-based. Indices start at 1 and boundaries are
     * inclusive.
     *
     * <p>e.g. withColumns(range("b", "c")) or withoutColumns($("*"))
     */
    public static ApiExpression withColumns(Object head, Object... tail) {
        return apiCallAtLeastOneArgument(BuiltInFunctionDefinitions.WITH_COLUMNS, head, tail);
    }

    /**
     * Creates an expression that selects all columns except for the given range of columns. It can
     * be used wherever an array of expression is accepted such as function calls, projections, or
     * groupings.
     *
     * <p>A range can either be index-based or name-based. Indices start at 1 and boundaries are
     * inclusive.
     *
     * <p>e.g. withoutColumns(range("b", "c")) or withoutColumns($("c"))
     */
    public static ApiExpression withoutColumns(Object head, Object... tail) {
        return apiCallAtLeastOneArgument(BuiltInFunctionDefinitions.WITHOUT_COLUMNS, head, tail);
    }

    /**
     * A call to a function that will be looked up in a catalog. There are two kinds of functions:
     *
     * <ul>
     *   <li>System functions - which are identified with one part names
     *   <li>Catalog functions - which are identified always with three parts names (catalog,
     *       database, function)
     * </ul>
     *
     * <p>Moreover each function can either be a temporary function or permanent one (which is
     * stored in an external catalog).
     *
     * <p>Based on that two properties the resolution order for looking up a function based on the
     * provided {@code functionName} is following:
     *
     * <ul>
     *   <li>Temporary system function
     *   <li>System function
     *   <li>Temporary catalog function
     *   <li>Catalog function
     * </ul>
     *
     * @see TableEnvironment#useCatalog(String)
     * @see TableEnvironment#useDatabase(String)
     * @see TableEnvironment#createTemporaryFunction
     * @see TableEnvironment#createTemporarySystemFunction
     */
    public static ApiExpression call(String path, Object... arguments) {
        return new ApiExpression(
                ApiExpressionUtils.lookupCall(
                        path,
                        Arrays.stream(arguments)
                                .map(ApiExpressionUtils::objectToExpression)
                                .toArray(Expression[]::new)));
    }

    /**
     * A call to an unregistered, inline function.
     *
     * <p>For functions that have been registered before and are identified by a name, use {@link
     * #call(String, Object...)}.
     */
    public static ApiExpression call(UserDefinedFunction function, Object... arguments) {
        return apiCall(function, arguments);
    }

    /**
     * A call to an unregistered, inline function.
     *
     * <p>For functions that have been registered before and are identified by a name, use {@link
     * #call(String, Object...)}.
     */
    public static ApiExpression call(
            Class<? extends UserDefinedFunction> function, Object... arguments) {
        final UserDefinedFunction functionInstance =
                UserDefinedFunctionHelper.instantiateFunction(function);
        return apiCall(functionInstance, arguments);
    }

    /**
     * A call to a SQL expression.
     *
     * <p>The given string is parsed and translated into an {@link Expression} during planning. Only
     * the translated expression is evaluated during runtime.
     *
     * <p>Note: Currently, calls are limited to simple scalar expressions. Calls to aggregate or
     * table-valued functions are not supported. Sub-queries are also not allowed.
     */
    public static ApiExpression callSql(String sqlExpression) {
        return apiSqlCall(sqlExpression);
    }

    private static ApiExpression apiCall(FunctionDefinition functionDefinition, Object... args) {
        List<Expression> arguments =
                Stream.of(args)
                        .map(ApiExpressionUtils::objectToExpression)
                        .collect(Collectors.toList());
        return new ApiExpression(unresolvedCall(functionDefinition, arguments));
    }

    private static ApiExpression apiCallAtLeastOneArgument(
            FunctionDefinition functionDefinition, Object arg0, Object... args) {
        List<Expression> arguments =
                Stream.concat(Stream.of(arg0), Stream.of(args))
                        .map(ApiExpressionUtils::objectToExpression)
                        .collect(Collectors.toList());
        return new ApiExpression(unresolvedCall(functionDefinition, arguments));
    }

    private static ApiExpression apiCallAtLeastTwoArgument(
            FunctionDefinition functionDefinition, Object arg0, Object arg1, Object... args) {
        List<Expression> arguments =
                Stream.concat(Stream.of(arg0, arg1), Stream.of(args))
                        .map(ApiExpressionUtils::objectToExpression)
                        .collect(Collectors.toList());
        return new ApiExpression(unresolvedCall(functionDefinition, arguments));
    }

    private static ApiExpression apiSqlCall(String sqlExpression) {
        return new ApiExpression(new SqlCallExpression(sqlExpression));
    }
}
