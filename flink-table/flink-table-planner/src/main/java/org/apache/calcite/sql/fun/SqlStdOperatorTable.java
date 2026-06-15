/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDescriptorOperator;
import org.apache.calcite.sql.SqlFilterOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlHopTableFunction;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLateralOperator;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNullTreatmentOperator;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlProcedureCallOperator;
import org.apache.calcite.sql.SqlRankFunction;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSessionTableFunction;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSetSemanticsTableOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlTumbleTableFunction;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWithinDistinctOperator;
import org.apache.calcite.sql.SqlWithinGroupOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql2rel.AuxiliaryConverter;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Implementation of {@link org.apache.calcite.sql.SqlOperatorTable} containing the standard
 * operators and functions.
 *
 * <p>Lines 765 ~ 767, 785 ~ 787, 796 ~ 798, 807 ~ 809, 818 ~ 820, 829 ~ 831, 840 ~ 842, Flink
 * changes the return type of the {@code IS [NOT] JSON ...} predicates from {@link
 * ReturnTypes#BOOLEAN_NULLABLE} to {@link ReturnTypes#BOOLEAN} so that they always return a
 * non-nullable {@code BOOLEAN}, see also FLINK-39943.
 */
public class SqlStdOperatorTable extends ReflectiveSqlOperatorTable {

    // ~ Static fields/initializers ---------------------------------------------

    /** The standard operator table. */
    private static final Supplier<SqlStdOperatorTable> INSTANCE =
            Suppliers.memoize(() -> (SqlStdOperatorTable) new SqlStdOperatorTable().init());

    // -------------------------------------------------------------
    //                   SET OPERATORS
    // -------------------------------------------------------------
    // The set operators can be compared to the arithmetic operators
    // UNION -> +
    // EXCEPT -> -
    // INTERSECT -> *
    // which explains the different precedence values
    public static final SqlSetOperator UNION =
            new SqlSetOperator("UNION", SqlKind.UNION, 12, false);

    public static final SqlSetOperator UNION_ALL =
            new SqlSetOperator("UNION ALL", SqlKind.UNION, 12, true);

    public static final SqlSetOperator EXCEPT =
            new SqlSetOperator("EXCEPT", SqlKind.EXCEPT, 12, false);

    public static final SqlSetOperator EXCEPT_ALL =
            new SqlSetOperator("EXCEPT ALL", SqlKind.EXCEPT, 12, true);

    public static final SqlSetOperator INTERSECT =
            new SqlSetOperator("INTERSECT", SqlKind.INTERSECT, 14, false);

    public static final SqlSetOperator INTERSECT_ALL =
            new SqlSetOperator("INTERSECT ALL", SqlKind.INTERSECT, 14, true);

    /** The {@code MULTISET UNION DISTINCT} operator. */
    public static final SqlMultisetSetOperator MULTISET_UNION_DISTINCT =
            new SqlMultisetSetOperator("MULTISET UNION DISTINCT", 12, false);

    /** The {@code MULTISET UNION [ALL]} operator. */
    public static final SqlMultisetSetOperator MULTISET_UNION =
            new SqlMultisetSetOperator("MULTISET UNION ALL", 12, true);

    /** The {@code MULTISET EXCEPT DISTINCT} operator. */
    public static final SqlMultisetSetOperator MULTISET_EXCEPT_DISTINCT =
            new SqlMultisetSetOperator("MULTISET EXCEPT DISTINCT", 12, false);

    /** The {@code MULTISET EXCEPT [ALL]} operator. */
    public static final SqlMultisetSetOperator MULTISET_EXCEPT =
            new SqlMultisetSetOperator("MULTISET EXCEPT ALL", 12, true);

    /** The {@code MULTISET INTERSECT DISTINCT} operator. */
    public static final SqlMultisetSetOperator MULTISET_INTERSECT_DISTINCT =
            new SqlMultisetSetOperator("MULTISET INTERSECT DISTINCT", 14, false);

    /** The {@code MULTISET INTERSECT [ALL]} operator. */
    public static final SqlMultisetSetOperator MULTISET_INTERSECT =
            new SqlMultisetSetOperator("MULTISET INTERSECT ALL", 14, true);

    // -------------------------------------------------------------
    //                   BINARY OPERATORS
    // -------------------------------------------------------------

    /** Logical <code>AND</code> operator. */
    public static final SqlBinaryOperator AND =
            new SqlBinaryOperator(
                    "AND",
                    SqlKind.AND,
                    24,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN_BOOLEAN);

    /** <code>AS</code> operator associates an expression in the SELECT clause with an alias. */
    public static final SqlAsOperator AS = new SqlAsOperator();

    /**
     * <code>ARGUMENT_ASSIGNMENT</code> operator (<code>=&lt;</code>) assigns an argument to a
     * function call to a particular named parameter.
     */
    public static final SqlSpecialOperator ARGUMENT_ASSIGNMENT =
            new SqlArgumentAssignmentOperator();

    /**
     * <code>DEFAULT</code> operator indicates that an argument to a function call is to take its
     * default value..
     */
    public static final SqlSpecialOperator DEFAULT = new SqlDefaultOperator();

    /** <code>FILTER</code> operator filters which rows are included in an aggregate function. */
    public static final SqlFilterOperator FILTER = new SqlFilterOperator();

    /** <code>WITHIN_GROUP</code> operator performs aggregations on ordered data input. */
    public static final SqlWithinGroupOperator WITHIN_GROUP = new SqlWithinGroupOperator();

    /** <code>WITHIN_DISTINCT</code> operator performs aggregations on distinct data input. */
    public static final SqlWithinDistinctOperator WITHIN_DISTINCT = new SqlWithinDistinctOperator();

    /**
     * {@code CUBE} operator, occurs within {@code GROUP BY} clause or nested within a {@code
     * GROUPING SETS}.
     */
    public static final SqlInternalOperator CUBE = new SqlRollupOperator("CUBE", SqlKind.CUBE);

    /**
     * {@code ROLLUP} operator, occurs within {@code GROUP BY} clause or nested within a {@code
     * GROUPING SETS}.
     */
    public static final SqlInternalOperator ROLLUP =
            new SqlRollupOperator("ROLLUP", SqlKind.ROLLUP);

    /**
     * {@code GROUPING SETS} operator, occurs within {@code GROUP BY} clause or nested within a
     * {@code GROUPING SETS}.
     */
    public static final SqlInternalOperator GROUPING_SETS =
            new SqlRollupOperator("GROUPING SETS", SqlKind.GROUPING_SETS);

    /**
     * {@code GROUPING(c1 [, c2, ...])} function.
     *
     * <p>Occurs in similar places to an aggregate function ({@code SELECT}, {@code HAVING} clause,
     * etc. of an aggregate query), but not technically an aggregate function.
     */
    public static final SqlAggFunction GROUPING = new SqlGroupingFunction("GROUPING");

    /** {@code GROUP_ID()} function. (Oracle-specific.) */
    public static final SqlAggFunction GROUP_ID = new SqlGroupIdFunction();

    /**
     * {@code GROUPING_ID} function is a synonym for {@code GROUPING}.
     *
     * <p>Some history. The {@code GROUPING} function is in the SQL standard, and originally
     * supported only one argument. {@code GROUPING_ID} is not standard (though supported in Oracle
     * and SQL Server) and supports one or more arguments.
     *
     * <p>The SQL standard has changed to allow {@code GROUPING} to have multiple arguments. It is
     * now equivalent to {@code GROUPING_ID}, so we made {@code GROUPING_ID} a synonym for {@code
     * GROUPING}.
     */
    public static final SqlAggFunction GROUPING_ID = new SqlGroupingFunction("GROUPING_ID");

    /** {@code EXTEND} operator. */
    public static final SqlInternalOperator EXTEND = new SqlExtendOperator();

    /**
     * String and array-to-array concatenation operator, '<code>||</code>'.
     *
     * @see SqlLibraryOperators#CONCAT_FUNCTION
     */
    public static final SqlBinaryOperator CONCAT =
            new SqlBinaryOperator(
                    "||",
                    SqlKind.OTHER,
                    60,
                    true,
                    ReturnTypes.ARG0.andThen(
                            (opBinding, typeToTransform) -> {
                                SqlReturnTypeInference returnType =
                                        typeToTransform.getSqlTypeName().getFamily()
                                                        == SqlTypeFamily.ARRAY
                                                ? ReturnTypes.LEAST_RESTRICTIVE
                                                : ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE;

                                return requireNonNull(
                                        returnType.inferReturnType(opBinding),
                                        "inferred CONCAT element type");
                            }),
                    null,
                    OperandTypes.STRING_SAME_SAME_OR_ARRAY_SAME_SAME);

    /** Arithmetic division operator, '<code>/</code>'. */
    public static final SqlBinaryOperator DIVIDE =
            new SqlBinaryOperator(
                    "/",
                    SqlKind.DIVIDE,
                    60,
                    true,
                    ReturnTypes.QUOTIENT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.DIVISION_OPERATOR);

    /**
     * Arithmetic remainder operator, '<code>%</code>', an alternative to {@link #MOD} allowed if
     * under certain conformance levels.
     *
     * @see SqlConformance#isPercentRemainderAllowed
     */
    public static final SqlBinaryOperator PERCENT_REMAINDER =
            new SqlBinaryOperator(
                    "%",
                    SqlKind.MOD,
                    60,
                    true,
                    ReturnTypes.NULLABLE_MOD,
                    null,
                    OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC);

    /**
     * The {@code RAND_INTEGER([seed, ] bound)} function, which yields a random integer, optionally
     * with seed.
     */
    public static final SqlRandIntegerFunction RAND_INTEGER = new SqlRandIntegerFunction();

    /** The {@code RAND([seed])} function, which yields a random double, optionally with seed. */
    public static final SqlBasicFunction RAND =
            SqlBasicFunction.create(
                            "RAND",
                            ReturnTypes.DOUBLE,
                            OperandTypes.NILADIC.or(OperandTypes.NUMERIC),
                            SqlFunctionCategory.NUMERIC)
                    .withDeterministic(false)
                    .withDynamic(true);

    /**
     * Internal integer arithmetic division operator, '<code>/INT</code>'. This is only used to
     * adjust scale for numerics. We distinguish it from user-requested division since some
     * personalities want a floating-point computation, whereas for the internal scaling use of
     * division, we always want integer division.
     */
    public static final SqlBinaryOperator DIVIDE_INTEGER =
            new SqlBinaryOperator(
                    "/INT",
                    SqlKind.DIVIDE,
                    60,
                    true,
                    ReturnTypes.INTEGER_QUOTIENT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.DIVISION_OPERATOR);

    /** Dot operator, '<code>.</code>', used for referencing fields of records. */
    public static final SqlOperator DOT = new SqlDotOperator();

    /** Logical equals operator, '<code>=</code>'. */
    public static final SqlBinaryOperator EQUALS =
            new SqlBinaryOperator(
                    "=",
                    SqlKind.EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    /** Logical greater-than operator, '<code>&gt;</code>'. */
    public static final SqlBinaryOperator GREATER_THAN =
            new SqlBinaryOperator(
                    ">",
                    SqlKind.GREATER_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    /** <code>IS DISTINCT FROM</code> operator. */
    public static final SqlBinaryOperator IS_DISTINCT_FROM =
            new SqlBinaryOperator(
                    "IS DISTINCT FROM",
                    SqlKind.IS_DISTINCT_FROM,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    /**
     * <code>IS NOT DISTINCT FROM</code> operator. Is equivalent to <code>NOT(x
     * IS DISTINCT FROM y)</code>
     */
    public static final SqlBinaryOperator IS_NOT_DISTINCT_FROM =
            new SqlBinaryOperator(
                    "IS NOT DISTINCT FROM",
                    SqlKind.IS_NOT_DISTINCT_FROM,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    /**
     * The internal <code>$IS_DIFFERENT_FROM</code> operator is the same as the user-level {@link
     * #IS_DISTINCT_FROM} in all respects except that the test for equality on character datatypes
     * treats trailing spaces as significant.
     */
    public static final SqlBinaryOperator IS_DIFFERENT_FROM =
            new SqlBinaryOperator(
                    "$IS_DIFFERENT_FROM",
                    SqlKind.OTHER,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    /** Logical greater-than-or-equal operator, '<code>&gt;=</code>'. */
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    ">=",
                    SqlKind.GREATER_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    /**
     * <code>IN</code> operator tests for a value's membership in a sub-query or a list of values.
     */
    public static final SqlBinaryOperator IN = new SqlInOperator(SqlKind.IN);

    /**
     * <code>NOT IN</code> operator tests for a value's membership in a sub-query or a list of
     * values.
     */
    public static final SqlBinaryOperator NOT_IN = new SqlInOperator(SqlKind.NOT_IN);

    /**
     * Operator that tests whether its left operand is included in the range of values covered by
     * search arguments.
     */
    public static final SqlInternalOperator SEARCH = new SqlSearchOperator();

    /** The <code>&lt; SOME</code> operator (synonymous with <code>&lt; ANY</code>). */
    public static final SqlQuantifyOperator SOME_LT =
            new SqlQuantifyOperator(SqlKind.SOME, SqlKind.LESS_THAN);

    public static final SqlQuantifyOperator SOME_LE =
            new SqlQuantifyOperator(SqlKind.SOME, SqlKind.LESS_THAN_OR_EQUAL);

    public static final SqlQuantifyOperator SOME_GT =
            new SqlQuantifyOperator(SqlKind.SOME, SqlKind.GREATER_THAN);

    public static final SqlQuantifyOperator SOME_GE =
            new SqlQuantifyOperator(SqlKind.SOME, SqlKind.GREATER_THAN_OR_EQUAL);

    public static final SqlQuantifyOperator SOME_EQ =
            new SqlQuantifyOperator(SqlKind.SOME, SqlKind.EQUALS);

    public static final SqlQuantifyOperator SOME_NE =
            new SqlQuantifyOperator(SqlKind.SOME, SqlKind.NOT_EQUALS);

    /** The <code>&lt; ALL</code> operator. */
    public static final SqlQuantifyOperator ALL_LT =
            new SqlQuantifyOperator(SqlKind.ALL, SqlKind.LESS_THAN);

    public static final SqlQuantifyOperator ALL_LE =
            new SqlQuantifyOperator(SqlKind.ALL, SqlKind.LESS_THAN_OR_EQUAL);

    public static final SqlQuantifyOperator ALL_GT =
            new SqlQuantifyOperator(SqlKind.ALL, SqlKind.GREATER_THAN);

    public static final SqlQuantifyOperator ALL_GE =
            new SqlQuantifyOperator(SqlKind.ALL, SqlKind.GREATER_THAN_OR_EQUAL);

    public static final SqlQuantifyOperator ALL_EQ =
            new SqlQuantifyOperator(SqlKind.ALL, SqlKind.EQUALS);

    public static final SqlQuantifyOperator ALL_NE =
            new SqlQuantifyOperator(SqlKind.ALL, SqlKind.NOT_EQUALS);

    public static final List<SqlQuantifyOperator> QUANTIFY_OPERATORS =
            ImmutableList.of(
                    SqlStdOperatorTable.SOME_EQ,
                    SqlStdOperatorTable.SOME_GT,
                    SqlStdOperatorTable.SOME_GE,
                    SqlStdOperatorTable.SOME_LE,
                    SqlStdOperatorTable.SOME_LT,
                    SqlStdOperatorTable.SOME_NE,
                    SqlStdOperatorTable.ALL_EQ,
                    SqlStdOperatorTable.ALL_GT,
                    SqlStdOperatorTable.ALL_GE,
                    SqlStdOperatorTable.ALL_LE,
                    SqlStdOperatorTable.ALL_LT,
                    SqlStdOperatorTable.ALL_NE);

    /** Logical less-than operator, '<code>&lt;</code>'. */
    public static final SqlBinaryOperator LESS_THAN =
            new SqlBinaryOperator(
                    "<",
                    SqlKind.LESS_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    /** Logical less-than-or-equal operator, '<code>&lt;=</code>'. */
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    "<=",
                    SqlKind.LESS_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    /**
     * Infix arithmetic minus operator, '<code>-</code>'.
     *
     * <p>Its precedence is less than the prefix {@link #UNARY_PLUS +} and {@link #UNARY_MINUS -}
     * operators.
     */
    public static final SqlBinaryOperator MINUS =
            new SqlMonotonicBinaryOperator(
                    "-",
                    SqlKind.MINUS,
                    40,
                    true,

                    // Same type inference strategy as sum
                    ReturnTypes.NULLABLE_SUM,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.MINUS_OPERATOR);

    /** Arithmetic multiplication operator, '<code>*</code>'. */
    public static final SqlBinaryOperator MULTIPLY =
            new SqlMonotonicBinaryOperator(
                    "*",
                    SqlKind.TIMES,
                    60,
                    true,
                    ReturnTypes.PRODUCT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.MULTIPLY_OPERATOR);

    /** Logical not-equals operator, '<code>&lt;&gt;</code>'. */
    public static final SqlBinaryOperator NOT_EQUALS =
            new SqlBinaryOperator(
                    "<>",
                    SqlKind.NOT_EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    /** Logical <code>OR</code> operator. */
    public static final SqlBinaryOperator OR =
            new SqlBinaryOperator(
                    "OR",
                    SqlKind.OR,
                    22,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN_BOOLEAN);

    /** Infix arithmetic plus operator, '<code>+</code>'. */
    public static final SqlBinaryOperator PLUS =
            new SqlMonotonicBinaryOperator(
                    "+",
                    SqlKind.PLUS,
                    40,
                    true,
                    ReturnTypes.NULLABLE_SUM,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.PLUS_OPERATOR);

    /** Infix datetime plus operator, '<code>DATETIME + INTERVAL</code>'. */
    public static final SqlSpecialOperator DATETIME_PLUS = new SqlDatetimePlusOperator();

    /** Interval expression, '<code>INTERVAL n timeUnit</code>'. */
    public static final SqlSpecialOperator INTERVAL = new SqlIntervalOperator();

    /**
     * Multiset {@code MEMBER OF}, which returns whether a element belongs to a multiset.
     *
     * <p>For example, the following returns <code>false</code>:
     *
     * <blockquote>
     *
     * <code>'green' MEMBER OF MULTISET ['red','almost green','blue']</code>
     *
     * </blockquote>
     */
    public static final SqlBinaryOperator MEMBER_OF = new SqlMultisetMemberOfOperator();

    /**
     * Submultiset. Checks to see if an multiset is a sub-set of another multiset.
     *
     * <p>For example, the following returns <code>false</code>:
     *
     * <blockquote>
     *
     * <code>MULTISET ['green'] SUBMULTISET OF
     * MULTISET['red', 'almost green', 'blue']</code>
     *
     * </blockquote>
     *
     * <p>The following returns <code>true</code>, in part because multisets are order-independent:
     *
     * <blockquote>
     *
     * <code>MULTISET ['blue', 'red'] SUBMULTISET OF
     * MULTISET ['red', 'almost green', 'blue']</code>
     *
     * </blockquote>
     */
    public static final SqlBinaryOperator SUBMULTISET_OF =

            // TODO: check if precedence is correct
            new SqlBinaryOperator(
                    "SUBMULTISET OF",
                    SqlKind.OTHER,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    null,
                    OperandTypes.MULTISET_MULTISET);

    public static final SqlBinaryOperator NOT_SUBMULTISET_OF =

            // TODO: check if precedence is correct
            new SqlBinaryOperator(
                    "NOT SUBMULTISET OF",
                    SqlKind.OTHER,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    null,
                    OperandTypes.MULTISET_MULTISET);

    // -------------------------------------------------------------
    //                   POSTFIX OPERATORS
    // -------------------------------------------------------------
    public static final SqlPostfixOperator DESC =
            new SqlPostfixOperator(
                    "DESC",
                    SqlKind.DESCENDING,
                    20,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.ANY);

    public static final SqlPostfixOperator NULLS_FIRST =
            new SqlPostfixOperator(
                    "NULLS FIRST",
                    SqlKind.NULLS_FIRST,
                    18,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.ANY);

    public static final SqlPostfixOperator NULLS_LAST =
            new SqlPostfixOperator(
                    "NULLS LAST",
                    SqlKind.NULLS_LAST,
                    18,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.ANY);

    public static final SqlPostfixOperator IS_NOT_NULL =
            new SqlPostfixOperator(
                    "IS NOT NULL",
                    SqlKind.IS_NOT_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.VARCHAR_1024,
                    OperandTypes.ANY);

    public static final SqlPostfixOperator IS_NULL =
            new SqlPostfixOperator(
                    "IS NULL",
                    SqlKind.IS_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.VARCHAR_1024,
                    OperandTypes.ANY);

    public static final SqlPostfixOperator IS_NOT_TRUE =
            new SqlPostfixOperator(
                    "IS NOT TRUE",
                    SqlKind.IS_NOT_TRUE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN);

    public static final SqlPostfixOperator IS_TRUE =
            new SqlPostfixOperator(
                    "IS TRUE",
                    SqlKind.IS_TRUE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN);

    public static final SqlPostfixOperator IS_NOT_FALSE =
            new SqlPostfixOperator(
                    "IS NOT FALSE",
                    SqlKind.IS_NOT_FALSE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN);

    public static final SqlPostfixOperator IS_FALSE =
            new SqlPostfixOperator(
                    "IS FALSE",
                    SqlKind.IS_FALSE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN);

    public static final SqlPostfixOperator IS_NOT_UNKNOWN =
            new SqlPostfixOperator(
                    "IS NOT UNKNOWN",
                    SqlKind.IS_NOT_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN);

    public static final SqlPostfixOperator IS_UNKNOWN =
            new SqlPostfixOperator(
                    "IS UNKNOWN",
                    SqlKind.IS_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN);

    public static final SqlPostfixOperator IS_A_SET =
            new SqlPostfixOperator(
                    "IS A SET",
                    SqlKind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.MULTISET);

    public static final SqlPostfixOperator IS_NOT_A_SET =
            new SqlPostfixOperator(
                    "IS NOT A SET",
                    SqlKind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.MULTISET);

    public static final SqlPostfixOperator IS_EMPTY =
            new SqlPostfixOperator(
                    "IS EMPTY",
                    SqlKind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.COLLECTION_OR_MAP);

    public static final SqlPostfixOperator IS_NOT_EMPTY =
            new SqlPostfixOperator(
                    "IS NOT EMPTY",
                    SqlKind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.COLLECTION_OR_MAP);

    public static final SqlPostfixOperator IS_JSON_VALUE =
            new SqlPostfixOperator(
                    "IS JSON VALUE",
                    SqlKind.OTHER,
                    28,
                    // FLINK MODIFICATION BEGIN
                    ReturnTypes.BOOLEAN,
                    // FLINK MODIFICATION END
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator IS_NOT_JSON_VALUE =
            new SqlPostfixOperator(
                    "IS NOT JSON VALUE",
                    SqlKind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator IS_JSON_OBJECT =
            new SqlPostfixOperator(
                    "IS JSON OBJECT",
                    SqlKind.OTHER,
                    28,
                    // FLINK MODIFICATION BEGIN
                    ReturnTypes.BOOLEAN,
                    // FLINK MODIFICATION END
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator IS_NOT_JSON_OBJECT =
            new SqlPostfixOperator(
                    "IS NOT JSON OBJECT",
                    SqlKind.OTHER,
                    28,
                    // FLINK MODIFICATION BEGIN
                    ReturnTypes.BOOLEAN,
                    // FLINK MODIFICATION END
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator IS_JSON_ARRAY =
            new SqlPostfixOperator(
                    "IS JSON ARRAY",
                    SqlKind.OTHER,
                    28,
                    // FLINK MODIFICATION BEGIN
                    ReturnTypes.BOOLEAN,
                    // FLINK MODIFICATION END
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator IS_NOT_JSON_ARRAY =
            new SqlPostfixOperator(
                    "IS NOT JSON ARRAY",
                    SqlKind.OTHER,
                    28,
                    // FLINK MODIFICATION BEGIN
                    ReturnTypes.BOOLEAN,
                    // FLINK MODIFICATION END
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator IS_JSON_SCALAR =
            new SqlPostfixOperator(
                    "IS JSON SCALAR",
                    SqlKind.OTHER,
                    28,
                    // FLINK MODIFICATION BEGIN
                    ReturnTypes.BOOLEAN,
                    // FLINK MODIFICATION END
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator IS_NOT_JSON_SCALAR =
            new SqlPostfixOperator(
                    "IS NOT JSON SCALAR",
                    SqlKind.OTHER,
                    28,
                    // FLINK MODIFICATION BEGIN
                    ReturnTypes.BOOLEAN,
                    // FLINK MODIFICATION END
                    null,
                    OperandTypes.CHARACTER);

    public static final SqlPostfixOperator JSON_VALUE_EXPRESSION =
            new SqlJsonValueExpressionOperator();

    public static final SqlJsonTypeOperator JSON_TYPE_OPERATOR = new SqlJsonTypeOperator();

    // -------------------------------------------------------------
    //                   PREFIX OPERATORS
    // -------------------------------------------------------------
    public static final SqlPrefixOperator EXISTS =
            new SqlPrefixOperator(
                    "EXISTS", SqlKind.EXISTS, 40, ReturnTypes.BOOLEAN, null, OperandTypes.ANY) {
                @Override
                public boolean argumentMustBeScalar(int ordinal) {
                    return false;
                }

                @Override
                public boolean validRexOperands(int count, Litmus litmus) {
                    if (count != 0) {
                        return litmus.fail("wrong operand count {} for {}", count, this);
                    }
                    return litmus.succeed();
                }
            };

    public static final SqlPrefixOperator UNIQUE =
            new SqlPrefixOperator(
                    "UNIQUE", SqlKind.UNIQUE, 40, ReturnTypes.BOOLEAN, null, OperandTypes.ANY) {
                @Override
                public boolean argumentMustBeScalar(int ordinal) {
                    return false;
                }

                @Override
                public boolean validRexOperands(int count, Litmus litmus) {
                    if (count != 0) {
                        return litmus.fail("wrong operand count {} for {}", count, this);
                    }
                    return litmus.succeed();
                }
            };

    public static final SqlPrefixOperator NOT =
            new SqlPrefixOperator(
                    "NOT",
                    SqlKind.NOT,
                    26,
                    ReturnTypes.ARG0,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN);

    /**
     * Prefix arithmetic minus operator, '<code>-</code>'.
     *
     * <p>Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}'
     * operators.
     */
    public static final SqlPrefixOperator UNARY_MINUS =
            new SqlPrefixOperator(
                    "-",
                    SqlKind.MINUS_PREFIX,
                    80,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.NUMERIC_OR_INTERVAL);

    /**
     * Prefix arithmetic plus operator, '<code>+</code>'.
     *
     * <p>Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}'
     * operators.
     */
    public static final SqlPrefixOperator UNARY_PLUS =
            new SqlPrefixOperator(
                    "+",
                    SqlKind.PLUS_PREFIX,
                    80,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.NUMERIC_OR_INTERVAL);

    /**
     * Keyword which allows an identifier to be explicitly flagged as a table. For example, <code>
     * select * from (TABLE t)</code> or <code>TABLE
     * t</code>. See also {@link #COLLECTION_TABLE}.
     */
    public static final SqlPrefixOperator EXPLICIT_TABLE =
            new SqlPrefixOperator("TABLE", SqlKind.EXPLICIT_TABLE, 2, null, null, null);

    /** {@code FINAL} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlPrefixOperator FINAL =
            new SqlPrefixOperator(
                    "FINAL", SqlKind.FINAL, 80, ReturnTypes.ARG0_NULLABLE, null, OperandTypes.ANY);

    /** {@code RUNNING} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlPrefixOperator RUNNING =
            new SqlPrefixOperator(
                    "RUNNING",
                    SqlKind.RUNNING,
                    80,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY);

    // -------------------------------------------------------------
    // AGGREGATE OPERATORS
    // -------------------------------------------------------------
    /** <code>SUM</code> aggregate function. */
    public static final SqlAggFunction SUM = new SqlSumAggFunction(castNonNull(null));

    /** <code>COUNT</code> aggregate function. */
    public static final SqlAggFunction COUNT = new SqlCountAggFunction("COUNT");

    /** <code>MODE</code> aggregate function. */
    public static final SqlAggFunction MODE =
            SqlBasicAggFunction.create(
                            "MODE",
                            SqlKind.MODE,
                            ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                            OperandTypes.ANY)
                    .withGroupOrder(Optionality.FORBIDDEN)
                    .withFunctionType(SqlFunctionCategory.SYSTEM);

    /** <code>APPROX_COUNT_DISTINCT</code> aggregate function. */
    public static final SqlAggFunction APPROX_COUNT_DISTINCT =
            new SqlCountAggFunction("APPROX_COUNT_DISTINCT");

    /** <code>ARG_MAX</code> aggregate function. */
    public static final SqlBasicAggFunction ARG_MAX =
            SqlBasicAggFunction.create(
                            "ARG_MAX",
                            SqlKind.ARG_MAX,
                            ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                            OperandTypes.ANY_COMPARABLE)
                    .withGroupOrder(Optionality.FORBIDDEN)
                    .withFunctionType(SqlFunctionCategory.SYSTEM);

    /** <code>ARG_MIN</code> aggregate function. */
    public static final SqlBasicAggFunction ARG_MIN =
            SqlBasicAggFunction.create(
                            "ARG_MIN",
                            SqlKind.ARG_MIN,
                            ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                            OperandTypes.ANY_COMPARABLE)
                    .withGroupOrder(Optionality.FORBIDDEN)
                    .withFunctionType(SqlFunctionCategory.SYSTEM);

    /** <code>MIN</code> aggregate function. */
    public static final SqlAggFunction MIN = new SqlMinMaxAggFunction(SqlKind.MIN);

    /** <code>MAX</code> aggregate function. */
    public static final SqlAggFunction MAX = new SqlMinMaxAggFunction(SqlKind.MAX);

    /** <code>EVERY</code> aggregate function. */
    public static final SqlAggFunction EVERY =
            new SqlMinMaxAggFunction("EVERY", SqlKind.MIN, OperandTypes.BOOLEAN);

    /** <code>SOME</code> aggregate function. */
    public static final SqlAggFunction SOME =
            new SqlMinMaxAggFunction("SOME", SqlKind.MAX, OperandTypes.BOOLEAN);

    /** <code>LAST_VALUE</code> aggregate function. */
    public static final SqlAggFunction LAST_VALUE =
            new SqlFirstLastValueAggFunction(SqlKind.LAST_VALUE);

    /** <code>ANY_VALUE</code> aggregate function. */
    public static final SqlAggFunction ANY_VALUE = new SqlAnyValueAggFunction(SqlKind.ANY_VALUE);

    /** <code>FIRST_VALUE</code> aggregate function. */
    public static final SqlAggFunction FIRST_VALUE =
            new SqlFirstLastValueAggFunction(SqlKind.FIRST_VALUE);

    /** <code>NTH_VALUE</code> aggregate function. */
    public static final SqlAggFunction NTH_VALUE = new SqlNthValueAggFunction(SqlKind.NTH_VALUE);

    /** <code>LEAD</code> aggregate function. */
    public static final SqlAggFunction LEAD = new SqlLeadLagAggFunction(SqlKind.LEAD);

    /** <code>LAG</code> aggregate function. */
    public static final SqlAggFunction LAG = new SqlLeadLagAggFunction(SqlKind.LAG);

    /** <code>NTILE</code> aggregate function. */
    public static final SqlAggFunction NTILE = new SqlNtileAggFunction();

    /** <code>SINGLE_VALUE</code> aggregate function. */
    public static final SqlAggFunction SINGLE_VALUE =
            new SqlSingleValueAggFunction(castNonNull(null));

    /** <code>AVG</code> aggregate function. */
    public static final SqlAggFunction AVG = new SqlAvgAggFunction(SqlKind.AVG);

    /** <code>STDDEV_POP</code> aggregate function. */
    public static final SqlAggFunction STDDEV_POP = new SqlAvgAggFunction(SqlKind.STDDEV_POP);

    /** <code>REGR_COUNT</code> aggregate function. */
    public static final SqlAggFunction REGR_COUNT = new SqlRegrCountAggFunction(SqlKind.REGR_COUNT);

    /** <code>REGR_SXX</code> aggregate function. */
    public static final SqlAggFunction REGR_SXX = new SqlCovarAggFunction(SqlKind.REGR_SXX);

    /** <code>REGR_SYY</code> aggregate function. */
    public static final SqlAggFunction REGR_SYY = new SqlCovarAggFunction(SqlKind.REGR_SYY);

    /** <code>COVAR_POP</code> aggregate function. */
    public static final SqlAggFunction COVAR_POP = new SqlCovarAggFunction(SqlKind.COVAR_POP);

    /** <code>COVAR_SAMP</code> aggregate function. */
    public static final SqlAggFunction COVAR_SAMP = new SqlCovarAggFunction(SqlKind.COVAR_SAMP);

    /** <code>STDDEV_SAMP</code> aggregate function. */
    public static final SqlAggFunction STDDEV_SAMP = new SqlAvgAggFunction(SqlKind.STDDEV_SAMP);

    /** <code>STDDEV</code> aggregate function. */
    public static final SqlAggFunction STDDEV =
            new SqlAvgAggFunction("STDDEV", SqlKind.STDDEV_SAMP);

    /** <code>VAR_POP</code> aggregate function. */
    public static final SqlAggFunction VAR_POP = new SqlAvgAggFunction(SqlKind.VAR_POP);

    /** <code>VAR_SAMP</code> aggregate function. */
    public static final SqlAggFunction VAR_SAMP = new SqlAvgAggFunction(SqlKind.VAR_SAMP);

    /** <code>VARIANCE</code> aggregate function. */
    public static final SqlAggFunction VARIANCE =
            new SqlAvgAggFunction("VARIANCE", SqlKind.VAR_SAMP);

    public static final SqlBasicFunction BITCOUNT =
            SqlBasicFunction.create(
                    "BITCOUNT",
                    ReturnTypes.BIGINT_NULLABLE,
                    OperandTypes.INTEGER.or(OperandTypes.BINARY),
                    SqlFunctionCategory.NUMERIC);

    /** <code>BITAND</code> scalar function. */
    public static final SqlFunction BITAND =
            SqlBasicFunction.create(
                    "BITAND",
                    SqlKind.BITAND,
                    ReturnTypes.LARGEST_INT_OR_FIRST_NON_NULL,
                    OperandTypes.INTEGER_INTEGER.or(OperandTypes.BINARY_BINARY));

    /** <code>BITOR</code> scalar function. */
    public static final SqlFunction BITOR =
            SqlBasicFunction.create(
                    "BITOR",
                    SqlKind.BITOR,
                    ReturnTypes.LARGEST_INT_OR_FIRST_NON_NULL,
                    OperandTypes.INTEGER_INTEGER.or(OperandTypes.BINARY_BINARY));

    /** <code>BITXOR</code> scalar function. */
    public static final SqlFunction BITXOR =
            SqlBasicFunction.create(
                    "BITXOR",
                    SqlKind.BITXOR,
                    ReturnTypes.LARGEST_INT_OR_FIRST_NON_NULL,
                    OperandTypes.INTEGER_INTEGER.or(OperandTypes.BINARY_BINARY));

    /** <code>BITNOT</code> scalar function. */
    public static final SqlFunction BITNOT =
            SqlBasicFunction.create(
                    "BITNOT",
                    SqlKind.BITNOT,
                    ReturnTypes.ARG0_OR_INTEGER,
                    OperandTypes.INTEGER.or(OperandTypes.BINARY));

    /** <code>BIT_AND</code> aggregate function. */
    public static final SqlAggFunction BIT_AND = new SqlBitOpAggFunction(SqlKind.BIT_AND);

    /** <code>BIT_OR</code> aggregate function. */
    public static final SqlAggFunction BIT_OR = new SqlBitOpAggFunction(SqlKind.BIT_OR);

    /** <code>BIT_XOR</code> aggregate function. */
    public static final SqlAggFunction BIT_XOR = new SqlBitOpAggFunction(SqlKind.BIT_XOR);

    // -------------------------------------------------------------
    // WINDOW Aggregate Functions
    // -------------------------------------------------------------
    /**
     * <code>HISTOGRAM</code> aggregate function support. Used by window aggregate versions of
     * MIN/MAX
     */
    public static final SqlAggFunction HISTOGRAM_AGG =
            new SqlHistogramAggFunction(castNonNull(null));

    /** <code>HISTOGRAM_MIN</code> window aggregate function. */
    public static final SqlFunction HISTOGRAM_MIN =
            SqlBasicFunction.create(
                    "$HISTOGRAM_MIN",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.NUMERIC_OR_STRING,
                    SqlFunctionCategory.NUMERIC);

    /** <code>HISTOGRAM_MAX</code> window aggregate function. */
    public static final SqlFunction HISTOGRAM_MAX =
            SqlBasicFunction.create(
                    "$HISTOGRAM_MAX",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.NUMERIC_OR_STRING,
                    SqlFunctionCategory.NUMERIC);

    /** <code>HISTOGRAM_FIRST_VALUE</code> window aggregate function. */
    public static final SqlFunction HISTOGRAM_FIRST_VALUE =
            SqlBasicFunction.create(
                    "$HISTOGRAM_FIRST_VALUE",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.NUMERIC_OR_STRING,
                    SqlFunctionCategory.NUMERIC);

    /** <code>HISTOGRAM_LAST_VALUE</code> window aggregate function. */
    public static final SqlFunction HISTOGRAM_LAST_VALUE =
            SqlBasicFunction.create(
                    "$HISTOGRAM_LAST_VALUE",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.NUMERIC_OR_STRING,
                    SqlFunctionCategory.NUMERIC);

    /** <code>SUM0</code> aggregate function. */
    public static final SqlAggFunction SUM0 = new SqlSumEmptyIsZeroAggFunction();

    // -------------------------------------------------------------
    // WINDOW Rank Functions
    // -------------------------------------------------------------
    /** <code>CUME_DIST</code> window function. */
    public static final SqlRankFunction CUME_DIST =
            new SqlRankFunction(SqlKind.CUME_DIST, ReturnTypes.FRACTIONAL_RANK, true);

    /** <code>DENSE_RANK</code> window function. */
    public static final SqlRankFunction DENSE_RANK =
            new SqlRankFunction(SqlKind.DENSE_RANK, ReturnTypes.RANK, true);

    /** <code>PERCENT_RANK</code> window function. */
    public static final SqlRankFunction PERCENT_RANK =
            new SqlRankFunction(SqlKind.PERCENT_RANK, ReturnTypes.FRACTIONAL_RANK, true);

    /** <code>RANK</code> window function. */
    public static final SqlRankFunction RANK =
            new SqlRankFunction(SqlKind.RANK, ReturnTypes.RANK, true);

    /** <code>ROW_NUMBER</code> window function. */
    public static final SqlRankFunction ROW_NUMBER =
            new SqlRankFunction(SqlKind.ROW_NUMBER, ReturnTypes.RANK, false);

    // -------------------------------------------------------------
    //                   SPECIAL OPERATORS
    // -------------------------------------------------------------
    public static final SqlRowOperator ROW = new SqlRowOperator("ROW");

    /** <code>IGNORE NULLS</code> operator. */
    public static final SqlNullTreatmentOperator IGNORE_NULLS =
            new SqlNullTreatmentOperator(SqlKind.IGNORE_NULLS);

    /** <code>RESPECT NULLS</code> operator. */
    public static final SqlNullTreatmentOperator RESPECT_NULLS =
            new SqlNullTreatmentOperator(SqlKind.RESPECT_NULLS);

    /**
     * A special operator for the subtraction of two DATETIMEs. The format of DATETIME subtraction
     * is:
     *
     * <blockquote>
     *
     * <code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")"
     * &lt;interval qualifier&gt;</code>
     *
     * </blockquote>
     *
     * <p>This operator is special since it needs to hold the additional interval qualifier
     * specification.
     */
    public static final SqlDatetimeSubtractionOperator MINUS_DATE =
            new SqlDatetimeSubtractionOperator("-", ReturnTypes.ARG2_NULLABLE);

    /** The MULTISET Value Constructor. e.g. "<code>MULTISET[1,2,3]</code>". */
    public static final SqlMultisetValueConstructor MULTISET_VALUE =
            new SqlMultisetValueConstructor();

    /**
     * The MULTISET Query Constructor. e.g. "<code>SELECT dname, MULTISET(SELECT
     * FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    public static final SqlMultisetQueryConstructor MULTISET_QUERY =
            new SqlMultisetQueryConstructor();

    /**
     * The ARRAY Query Constructor. e.g. "<code>SELECT dname, ARRAY(SELECT
     * FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    public static final SqlMultisetQueryConstructor ARRAY_QUERY = new SqlArrayQueryConstructor();

    /**
     * The MAP Query Constructor. e.g. "<code>MAP(SELECT empno, deptno
     * FROM emp)</code>".
     */
    public static final SqlMultisetQueryConstructor MAP_QUERY = new SqlMapQueryConstructor();

    /**
     * The CURSOR constructor. e.g. "<code>SELECT * FROM
     * TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), 'name'))</code>".
     */
    public static final SqlCursorConstructor CURSOR = new SqlCursorConstructor();

    /**
     * The COLUMN_LIST constructor. e.g. the ROW() call in "<code>SELECT * FROM
     * TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), ROW(name, empno)))</code>".
     */
    public static final SqlColumnListConstructor COLUMN_LIST = new SqlColumnListConstructor();

    /** The <code>UNNEST</code> operator. */
    public static final SqlUnnestOperator UNNEST = new SqlUnnestOperator(false);

    /** The <code>UNNEST WITH ORDINALITY</code> operator. */
    @LibraryOperator(libraries = {}) // do not include in index
    public static final SqlUnnestOperator UNNEST_WITH_ORDINALITY = new SqlUnnestOperator(true);

    /** The <code>LATERAL</code> operator. */
    public static final SqlSpecialOperator LATERAL = new SqlLateralOperator(SqlKind.LATERAL);

    /**
     * The "table function derived table" operator, which a table-valued function into a relation,
     * e.g. "<code>SELECT * FROM
     * TABLE(ramp(5))</code>".
     *
     * <p>This operator has function syntax (with one argument), whereas {@link #EXPLICIT_TABLE} is
     * a prefix operator.
     */
    public static final SqlSpecialOperator COLLECTION_TABLE =
            new SqlCollectionTableOperator("TABLE", SqlModality.RELATION);

    public static final SqlOverlapsOperator OVERLAPS = new SqlOverlapsOperator(SqlKind.OVERLAPS);

    public static final SqlOverlapsOperator CONTAINS = new SqlOverlapsOperator(SqlKind.CONTAINS);

    public static final SqlOverlapsOperator PRECEDES = new SqlOverlapsOperator(SqlKind.PRECEDES);

    public static final SqlOverlapsOperator IMMEDIATELY_PRECEDES =
            new SqlOverlapsOperator(SqlKind.IMMEDIATELY_PRECEDES);

    public static final SqlOverlapsOperator SUCCEEDS = new SqlOverlapsOperator(SqlKind.SUCCEEDS);

    public static final SqlOverlapsOperator IMMEDIATELY_SUCCEEDS =
            new SqlOverlapsOperator(SqlKind.IMMEDIATELY_SUCCEEDS);

    public static final SqlOverlapsOperator PERIOD_EQUALS =
            new SqlOverlapsOperator(SqlKind.PERIOD_EQUALS);

    public static final SqlSpecialOperator VALUES = new SqlValuesOperator();

    public static final SqlLiteralChainOperator LITERAL_CHAIN = new SqlLiteralChainOperator();

    public static final SqlThrowOperator THROW = new SqlThrowOperator();

    public static final SqlFunction JSON_EXISTS = new SqlJsonExistsFunction();

    public static final SqlFunction JSON_VALUE = new SqlJsonValueFunction("JSON_VALUE");

    public static final SqlFunction JSON_QUERY = new SqlJsonQueryFunction();

    public static final SqlFunction JSON_OBJECT = new SqlJsonObjectFunction();

    public static final SqlJsonObjectAggAggFunction JSON_OBJECTAGG =
            new SqlJsonObjectAggAggFunction(
                    SqlKind.JSON_OBJECTAGG, SqlJsonConstructorNullClause.NULL_ON_NULL);

    public static final SqlFunction JSON_ARRAY = new SqlJsonArrayFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_TYPE = new SqlJsonTypeFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_DEPTH = new SqlJsonDepthFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_LENGTH = new SqlJsonLengthFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_KEYS = new SqlJsonKeysFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_PRETTY = new SqlJsonPrettyFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_REMOVE = new SqlJsonRemoveFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_STORAGE_SIZE = new SqlJsonStorageSizeFunction();

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_INSERT = new SqlJsonModifyFunction("JSON_INSERT");

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_REPLACE = new SqlJsonModifyFunction("JSON_REPLACE");

    @Deprecated // to be removed before 2.0
    public static final SqlFunction JSON_SET = new SqlJsonModifyFunction("JSON_SET");

    public static final SqlJsonArrayAggAggFunction JSON_ARRAYAGG =
            new SqlJsonArrayAggAggFunction(
                    SqlKind.JSON_ARRAYAGG, SqlJsonConstructorNullClause.ABSENT_ON_NULL);

    public static final SqlBetweenOperator BETWEEN =
            new SqlBetweenOperator(SqlBetweenOperator.Flag.ASYMMETRIC, false);

    public static final SqlBetweenOperator SYMMETRIC_BETWEEN =
            new SqlBetweenOperator(SqlBetweenOperator.Flag.SYMMETRIC, false);

    public static final SqlBetweenOperator NOT_BETWEEN =
            new SqlBetweenOperator(SqlBetweenOperator.Flag.ASYMMETRIC, true);

    public static final SqlBetweenOperator SYMMETRIC_NOT_BETWEEN =
            new SqlBetweenOperator(SqlBetweenOperator.Flag.SYMMETRIC, true);

    public static final SqlSpecialOperator NOT_LIKE =
            new SqlLikeOperator("NOT LIKE", SqlKind.LIKE, true, true);

    public static final SqlSpecialOperator LIKE =
            new SqlLikeOperator("LIKE", SqlKind.LIKE, false, true);

    public static final SqlSpecialOperator NOT_SIMILAR_TO =
            new SqlLikeOperator("NOT SIMILAR TO", SqlKind.SIMILAR, true, true);

    public static final SqlSpecialOperator SIMILAR_TO =
            new SqlLikeOperator("SIMILAR TO", SqlKind.SIMILAR, false, true);

    public static final SqlBinaryOperator POSIX_REGEX_CASE_SENSITIVE =
            new SqlPosixRegexOperator(
                    "POSIX REGEX CASE SENSITIVE", SqlKind.POSIX_REGEX_CASE_SENSITIVE, true, false);

    public static final SqlBinaryOperator POSIX_REGEX_CASE_INSENSITIVE =
            new SqlPosixRegexOperator(
                    "POSIX REGEX CASE INSENSITIVE",
                    SqlKind.POSIX_REGEX_CASE_INSENSITIVE,
                    false,
                    false);

    public static final SqlBinaryOperator NEGATED_POSIX_REGEX_CASE_SENSITIVE =
            new SqlPosixRegexOperator(
                    "NEGATED POSIX REGEX CASE SENSITIVE",
                    SqlKind.POSIX_REGEX_CASE_SENSITIVE,
                    true,
                    true);

    public static final SqlBinaryOperator NEGATED_POSIX_REGEX_CASE_INSENSITIVE =
            new SqlPosixRegexOperator(
                    "NEGATED POSIX REGEX CASE INSENSITIVE",
                    SqlKind.POSIX_REGEX_CASE_INSENSITIVE,
                    false,
                    true);

    /** Internal operator used to represent the ESCAPE clause of a LIKE or SIMILAR TO expression. */
    public static final SqlSpecialOperator ESCAPE =
            new SqlSpecialOperator("ESCAPE", SqlKind.ESCAPE, 0);

    public static final SqlCaseOperator CASE = SqlCaseOperator.INSTANCE;

    public static final SqlOperator PROCEDURE_CALL = new SqlProcedureCallOperator();

    public static final SqlOperator NEW = new SqlNewOperator();

    /**
     * The <code>OVER</code> operator, which applies an aggregate functions to a {@link SqlWindow
     * window}.
     *
     * <p>Operands are as follows:
     *
     * <ol>
     *   <li>name of window function ({@link org.apache.calcite.sql.SqlCall})
     *   <li>window name ({@link org.apache.calcite.sql.SqlLiteral}) or window in-line specification
     *       ({@code org.apache.calcite.sql.SqlWindow.SqlWindowOperator})
     * </ol>
     */
    public static final SqlBinaryOperator OVER = new SqlOverOperator();

    /**
     * An <code>REINTERPRET</code> operator is internal to the planner. When the physical storage of
     * two types is the same, this operator may be used to reinterpret values of one type as the
     * other. This operator is similar to a cast, except that it does not alter the data value. Like
     * a regular cast it accepts one operand and stores the target type as the return type. It
     * performs an overflow check if it has <i>any</i> second operand, whether true or not.
     */
    public static final SqlSpecialOperator REINTERPRET =
            new SqlSpecialOperator("Reinterpret", SqlKind.REINTERPRET) {
                @Override
                public SqlOperandCountRange getOperandCountRange() {
                    return SqlOperandCountRanges.between(1, 2);
                }
            };

    // -------------------------------------------------------------
    //                   FUNCTIONS
    // -------------------------------------------------------------

    /**
     * The character substring function: <code>SUBSTRING(string FROM start [FOR
     * length])</code>.
     *
     * <p>If the length parameter is a constant, the length of the result is the minimum of the
     * length of the input and that length. Otherwise it is the length of the input.
     */
    public static final SqlFunction SUBSTRING = new SqlSubstringFunction();

    /**
     * The {@code REPLACE(string, search, replace)} function. Not standard SQL, but in Oracle and
     * Postgres.
     */
    public static final SqlFunction REPLACE =
            SqlBasicFunction.create(
                    "REPLACE",
                    ReturnTypes.VARCHAR_NULLABLE,
                    OperandTypes.STRING_STRING_STRING,
                    SqlFunctionCategory.STRING);

    /**
     * The {@code CONVERT(charValue, srcCharsetName, destCharsetName)} function converts {@code
     * charValue} with {@code destCharsetName}, whose original encoding is specified by {@code
     * srcCharsetName}.
     *
     * <p>The SQL standard defines {@code CONVERT(charValue USING transcodingName)}, and MySQL
     * implements it; Calcite supports this in the following TRANSLATE function.
     *
     * <p>MySQL and Microsoft SQL Server have a {@code CONVERT(type, value)} function; Calcite does
     * not currently support this, either.
     */
    public static final SqlFunction CONVERT = new SqlConvertFunction("CONVERT");

    /**
     * The <code>TRANSLATE/CONVERT(<i>char_value</i> USING <i>transcodingName</i>)</code> function
     * alters the character set of a string value from one base character set to transcodingName.
     *
     * <p>It is defined in the SQL standard. See also the non-standard {@link
     * SqlLibraryOperators#TRANSLATE3}, which has a different purpose.
     */
    public static final SqlFunction TRANSLATE = new SqlTranslateFunction("TRANSLATE");

    public static final SqlFunction OVERLAY = new SqlOverlayFunction();

    /** The "TRIM" function. */
    public static final SqlFunction TRIM = SqlTrimFunction.INSTANCE;

    public static final SqlFunction POSITION = new SqlPositionFunction("POSITION");

    public static final SqlBasicFunction CHAR_LENGTH =
            SqlBasicFunction.create(
                    SqlKind.CHAR_LENGTH, ReturnTypes.INTEGER_NULLABLE, OperandTypes.CHARACTER);

    /** Alias for {@link #CHAR_LENGTH}. */
    public static final SqlFunction CHARACTER_LENGTH = CHAR_LENGTH.withName("CHARACTER_LENGTH");

    public static final SqlFunction OCTET_LENGTH =
            SqlBasicFunction.create(
                    "OCTET_LENGTH",
                    ReturnTypes.INTEGER_NULLABLE,
                    OperandTypes.BINARY,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction UPPER =
            SqlBasicFunction.create(
                    "UPPER",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.CHARACTER,
                    SqlFunctionCategory.STRING);

    public static final SqlFunction LOWER =
            SqlBasicFunction.create(
                    "LOWER",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.CHARACTER,
                    SqlFunctionCategory.STRING);

    public static final SqlFunction INITCAP =
            SqlBasicFunction.create(
                    "INITCAP",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.CHARACTER,
                    SqlFunctionCategory.STRING);

    public static final SqlFunction ASCII =
            SqlBasicFunction.create(
                    "ASCII",
                    ReturnTypes.INTEGER_NULLABLE,
                    OperandTypes.CHARACTER,
                    SqlFunctionCategory.STRING);

    /**
     * The {@code POWER(numeric, numeric)} function.
     *
     * <p>The return type is always {@code DOUBLE} since we don't know what the result type will be
     * by just looking at the operand types. For example {@code POWER(INTEGER, INTEGER)} can return
     * a non-INTEGER if the second operand is negative.
     */
    public static final SqlBasicFunction POWER =
            SqlBasicFunction.create(
                    "POWER",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code SQRT(numeric)} function. */
    public static final SqlFunction SQRT =
            SqlBasicFunction.create("SQRT", ReturnTypes.DOUBLE_NULLABLE, OperandTypes.NUMERIC);

    /**
     * Arithmetic remainder function {@code MOD}.
     *
     * @see #PERCENT_REMAINDER
     */
    public static final SqlFunction MOD =
            // Return type is same as divisor (2nd operand)
            // SQL2003 Part2 Section 6.27, Syntax Rules 9
            SqlBasicFunction.create(
                            SqlKind.MOD,
                            ReturnTypes.NULLABLE_MOD,
                            OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC)
                    .withFunctionType(SqlFunctionCategory.NUMERIC);

    /** The {@code LN(numeric)} function. */
    public static final SqlFunction LN =
            SqlBasicFunction.create(
                    "LN",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code LOG10(numeric)} function. */
    public static final SqlFunction LOG10 =
            SqlBasicFunction.create(
                    "LOG10",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code ABS(numeric)} function. */
    public static final SqlFunction ABS =
            SqlBasicFunction.create(
                    "ABS",
                    ReturnTypes.ARG0,
                    OperandTypes.NUMERIC_OR_INTERVAL,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code ACOS(numeric)} function. */
    public static final SqlFunction ACOS =
            SqlBasicFunction.create(
                    "ACOS",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code ASIN(numeric)} function. */
    public static final SqlFunction ASIN =
            SqlBasicFunction.create(
                    "ASIN",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code ATAN(numeric)} function. */
    public static final SqlFunction ATAN =
            SqlBasicFunction.create(
                    "ATAN",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code ATAN2(numeric, numeric)} function. */
    public static final SqlFunction ATAN2 =
            SqlBasicFunction.create(
                    "ATAN2",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code CBRT(numeric)} function. */
    public static final SqlFunction CBRT =
            SqlBasicFunction.create(
                    "CBRT",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code COS(numeric)} function. */
    public static final SqlFunction COS =
            SqlBasicFunction.create(
                    "COS",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code COT(numeric)} function. */
    public static final SqlFunction COT =
            SqlBasicFunction.create(
                    "COT",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code DEGREES(numeric)} function. */
    public static final SqlFunction DEGREES =
            SqlBasicFunction.create(
                    "DEGREES",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code EXP(numeric)} function. */
    public static final SqlFunction EXP =
            SqlBasicFunction.create(
                    "EXP",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code RADIANS(numeric)} function. */
    public static final SqlFunction RADIANS =
            SqlBasicFunction.create(
                    "RADIANS",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code ROUND(numeric [, integer])} function. */
    public static final SqlFunction ROUND =
            SqlBasicFunction.create(
                    "ROUND",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.NUMERIC.or(OperandTypes.NUMERIC_INT32),
                    SqlFunctionCategory.NUMERIC);

    /** The {@code SIGN(numeric)} function. */
    public static final SqlFunction SIGN =
            SqlBasicFunction.create(
                    "SIGN", ReturnTypes.ARG0, OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC);

    /** The {@code SIN(numeric)} function. */
    public static final SqlFunction SIN =
            SqlBasicFunction.create(
                    "SIN",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code TAN(numeric)} function. */
    public static final SqlFunction TAN =
            SqlBasicFunction.create(
                    "TAN",
                    ReturnTypes.DOUBLE_NULLABLE,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    /** The {@code TRUNCATE(numeric [, integer])} function. */
    public static final SqlBasicFunction TRUNCATE =
            SqlBasicFunction.create(
                    "TRUNCATE",
                    ReturnTypes.ARG0_NULLABLE,
                    OperandTypes.NUMERIC.or(OperandTypes.NUMERIC_INT32),
                    SqlFunctionCategory.NUMERIC);

    /** The {@code PI} function. */
    public static final SqlFunction PI =
            SqlBasicFunction.create(
                            "PI",
                            ReturnTypes.DOUBLE,
                            OperandTypes.NILADIC,
                            SqlFunctionCategory.NUMERIC)
                    .withSyntax(SqlSyntax.FUNCTION_ID);

    /** {@code FIRST} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlFunction FIRST =
            SqlBasicFunction.create(
                            SqlKind.FIRST, ReturnTypes.ARG0_NULLABLE, OperandTypes.ANY_NUMERIC)
                    .withFunctionType(SqlFunctionCategory.MATCH_RECOGNIZE);

    /** {@code LAST} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlMatchFunction LAST =
            new SqlMatchFunction(
                    "LAST",
                    SqlKind.LAST,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY_NUMERIC,
                    SqlFunctionCategory.MATCH_RECOGNIZE);

    /** {@code PREV} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlMatchFunction PREV =
            new SqlMatchFunction(
                    "PREV",
                    SqlKind.PREV,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY_NUMERIC,
                    SqlFunctionCategory.MATCH_RECOGNIZE);

    /** {@code NEXT} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlFunction NEXT =
            SqlBasicFunction.create(
                            SqlKind.NEXT, ReturnTypes.ARG0_NULLABLE, OperandTypes.ANY_NUMERIC)
                    .withFunctionType(SqlFunctionCategory.MATCH_RECOGNIZE);

    /** {@code CLASSIFIER} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlMatchFunction CLASSIFIER =
            new SqlMatchFunction(
                    "CLASSIFIER",
                    SqlKind.CLASSIFIER,
                    ReturnTypes.VARCHAR_2000,
                    null,
                    OperandTypes.NILADIC,
                    SqlFunctionCategory.MATCH_RECOGNIZE);

    /** {@code MATCH_NUMBER} function to be used within {@code MATCH_RECOGNIZE}. */
    public static final SqlFunction MATCH_NUMBER =
            SqlBasicFunction.create(
                            SqlKind.MATCH_NUMBER, ReturnTypes.BIGINT_NULLABLE, OperandTypes.NILADIC)
                    .withFunctionType(SqlFunctionCategory.MATCH_RECOGNIZE);

    public static final SqlFunction NULLIF = new SqlNullifFunction();

    /** The COALESCE builtin function. */
    public static final SqlFunction COALESCE = new SqlCoalesceFunction();

    /** The <code>FLOOR</code> function. */
    public static final SqlFunction FLOOR = new SqlFloorFunction(SqlKind.FLOOR);

    /** The <code>CEIL</code> function. */
    public static final SqlFunction CEIL = new SqlFloorFunction(SqlKind.CEIL);

    /** The <code>USER</code> function. */
    public static final SqlFunction USER = new SqlStringContextVariable("USER");

    /** The <code>CURRENT_USER</code> function. */
    public static final SqlFunction CURRENT_USER = new SqlStringContextVariable("CURRENT_USER");

    /** The <code>SESSION_USER</code> function. */
    public static final SqlFunction SESSION_USER = new SqlStringContextVariable("SESSION_USER");

    /** The <code>SYSTEM_USER</code> function. */
    public static final SqlFunction SYSTEM_USER = new SqlStringContextVariable("SYSTEM_USER");

    /** The <code>CURRENT_PATH</code> function. */
    public static final SqlFunction CURRENT_PATH = new SqlStringContextVariable("CURRENT_PATH");

    /** The <code>CURRENT_ROLE</code> function. */
    public static final SqlFunction CURRENT_ROLE = new SqlStringContextVariable("CURRENT_ROLE");

    /** The <code>CURRENT_CATALOG</code> function. */
    public static final SqlFunction CURRENT_CATALOG =
            new SqlStringContextVariable("CURRENT_CATALOG");

    /** The <code>CURRENT_SCHEMA</code> function. */
    public static final SqlFunction CURRENT_SCHEMA = new SqlStringContextVariable("CURRENT_SCHEMA");

    /** The <code>LOCALTIME [(<i>precision</i>)]</code> function. */
    public static final SqlFunction LOCALTIME =
            new SqlAbstractTimeFunction("LOCALTIME", SqlTypeName.TIME);

    /** The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function. */
    public static final SqlFunction LOCALTIMESTAMP =
            new SqlAbstractTimeFunction("LOCALTIMESTAMP", SqlTypeName.TIMESTAMP);

    /** The <code>CURRENT_TIME [(<i>precision</i>)]</code> function. */
    public static final SqlFunction CURRENT_TIME =
            new SqlAbstractTimeFunction("CURRENT_TIME", SqlTypeName.TIME);

    /** The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function. */
    public static final SqlFunction CURRENT_TIMESTAMP =
            new SqlAbstractTimeFunction("CURRENT_TIMESTAMP", SqlTypeName.TIMESTAMP);

    /** The <code>CURRENT_DATE</code> function. */
    public static final SqlFunction CURRENT_DATE = new SqlCurrentDateFunction();

    /** The <code>TIMESTAMPADD</code> function. */
    public static final SqlFunction TIMESTAMP_ADD = new SqlTimestampAddFunction("TIMESTAMPADD");

    /** The <code>TIMESTAMPDIFF</code> function. */
    public static final SqlFunction TIMESTAMP_DIFF =
            new SqlTimestampDiffFunction(
                    "TIMESTAMPDIFF",
                    OperandTypes.family(
                            SqlTypeFamily.ANY, SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME));

    /**
     * Use of the <code>IN_FENNEL</code> operator forces the argument to be evaluated in Fennel.
     * Otherwise acts as identity function.
     */
    public static final SqlFunction IN_FENNEL =
            new SqlMonotonicUnaryFunction(
                    "$IN_FENNEL",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.ANY,
                    SqlFunctionCategory.SYSTEM);

    /**
     * The SQL <code>CAST</code> operator.
     *
     * <p>The SQL syntax is
     *
     * <blockquote>
     *
     * <code>CAST(<i>expression</i> AS <i>type</i>)</code>
     *
     * </blockquote>
     *
     * <p>When the CAST operator is applies as a {@link SqlCall}, it has two arguments: the
     * expression and the type. The type must not include a constraint, so <code>
     * CAST(x AS INTEGER NOT NULL)</code>, for instance, is invalid.
     *
     * <p>When the CAST operator is applied as a <code>RexCall</code>, the target type is simply
     * stored as the return type, not an explicit operand. For example, the expression <code>
     * CAST(1 + 2 AS DOUBLE)</code> will become a call to <code>CAST</code> with the expression
     * <code>1 + 2</code> as its only operand.
     *
     * <p>The <code>RexCall</code> form can also have a type which contains a <code>NOT NULL</code>
     * constraint. When this expression is implemented, if the value is NULL, an exception will be
     * thrown.
     */
    public static final SqlFunction CAST = new SqlCastFunction();

    /**
     * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from a DATETIME or an
     * INTERVAL. E.g.<br>
     * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>
     * 23</code>
     */
    public static final SqlFunction EXTRACT = new SqlExtractFunction("EXTRACT", false);

    /**
     * The SQL <code>YEAR</code> operator. Returns the Year from a DATETIME E.g.<br>
     * <code>YEAR(date '2008-9-23')</code> returns <code>
     * 2008</code>
     */
    public static final SqlDatePartFunction YEAR = new SqlDatePartFunction("YEAR", TimeUnit.YEAR);

    /**
     * The SQL <code>QUARTER</code> operator. Returns the Quarter from a DATETIME E.g.<br>
     * <code>QUARTER(date '2008-9-23')</code> returns <code>
     * 3</code>
     */
    public static final SqlDatePartFunction QUARTER =
            new SqlDatePartFunction("QUARTER", TimeUnit.QUARTER);

    /**
     * The SQL <code>MONTH</code> operator. Returns the Month from a DATETIME E.g.<br>
     * <code>MONTH(date '2008-9-23')</code> returns <code>
     * 9</code>
     */
    public static final SqlDatePartFunction MONTH =
            new SqlDatePartFunction("MONTH", TimeUnit.MONTH);

    /**
     * The SQL <code>WEEK</code> operator. Returns the Week from a DATETIME E.g.<br>
     * <code>WEEK(date '2008-9-23')</code> returns <code>
     * 39</code>
     */
    public static final SqlDatePartFunction WEEK = new SqlDatePartFunction("WEEK", TimeUnit.WEEK);

    /**
     * The SQL <code>DAYOFYEAR</code> operator. Returns the DOY from a DATETIME E.g.<br>
     * <code>DAYOFYEAR(date '2008-9-23')</code> returns <code>
     * 267</code>
     */
    public static final SqlDatePartFunction DAYOFYEAR =
            new SqlDatePartFunction("DAYOFYEAR", TimeUnit.DOY);

    /**
     * The SQL <code>DAYOFMONTH</code> operator. Returns the Day from a DATETIME E.g.<br>
     * <code>DAYOFMONTH(date '2008-9-23')</code> returns <code>
     * 23</code>
     */
    public static final SqlDatePartFunction DAYOFMONTH =
            new SqlDatePartFunction("DAYOFMONTH", TimeUnit.DAY);

    /**
     * The SQL <code>DAYOFWEEK</code> operator. Returns the DOW from a DATETIME E.g.<br>
     * <code>DAYOFWEEK(date '2008-9-23')</code> returns <code>
     * 2</code>
     */
    public static final SqlDatePartFunction DAYOFWEEK =
            new SqlDatePartFunction("DAYOFWEEK", TimeUnit.DOW);

    /**
     * The SQL <code>HOUR</code> operator. Returns the Hour from a DATETIME E.g.<br>
     * <code>HOUR(timestamp '2008-9-23 01:23:45')</code> returns <code>
     * 1</code>
     */
    public static final SqlDatePartFunction HOUR = new SqlDatePartFunction("HOUR", TimeUnit.HOUR);

    /**
     * The SQL <code>MINUTE</code> operator. Returns the Minute from a DATETIME E.g.<br>
     * <code>MINUTE(timestamp '2008-9-23 01:23:45')</code> returns <code>
     * 23</code>
     */
    public static final SqlDatePartFunction MINUTE =
            new SqlDatePartFunction("MINUTE", TimeUnit.MINUTE);

    /**
     * The SQL <code>SECOND</code> operator. Returns the Second from a DATETIME E.g.<br>
     * <code>SECOND(timestamp '2008-9-23 01:23:45')</code> returns <code>
     * 45</code>
     */
    public static final SqlDatePartFunction SECOND =
            new SqlDatePartFunction("SECOND", TimeUnit.SECOND);

    /** The {@code LAST_DAY(date)} function. */
    public static final SqlFunction LAST_DAY =
            SqlBasicFunction.create(
                    "LAST_DAY",
                    ReturnTypes.DATE_NULLABLE,
                    OperandTypes.DATETIME,
                    SqlFunctionCategory.TIMEDATE);

    /**
     * The ELEMENT operator, used to convert a multiset with only one item to a "regular" type.
     * Example ... log(ELEMENT(MULTISET[1])) ...
     */
    public static final SqlFunction ELEMENT =
            SqlBasicFunction.create(
                    "ELEMENT",
                    ReturnTypes.MULTISET_ELEMENT_FORCE_NULLABLE,
                    OperandTypes.COLLECTION);

    /**
     * The item operator {@code [ ... ]}, used to access a given element of an array, map or struct.
     * For example, {@code myArray[3]}, {@code "myMap['foo']"}, {@code myStruct[2]} or {@code
     * myStruct['fieldName']}.
     *
     * <p>The SQL standard calls the ARRAY variant a &lt;array element reference&gt;. Index is
     * 1-based. The standard says to raise "data exception - array element error" but we currently
     * return null.
     *
     * <p>MAP is not standard SQL.
     */
    public static final SqlOperator ITEM =
            new SqlItemOperator("ITEM", OperandTypes.ARRAY_OR_MAP, 1, true);

    /** The ARRAY Value Constructor. e.g. "<code>ARRAY[1, 2, 3]</code>". */
    public static final SqlArrayValueConstructor ARRAY_VALUE_CONSTRUCTOR =
            new SqlArrayValueConstructor();

    /** The MAP Value Constructor, e.g. "<code>MAP['washington', 1, 'obama', 44]</code>". */
    public static final SqlMapValueConstructor MAP_VALUE_CONSTRUCTOR = new SqlMapValueConstructor();

    /**
     * The internal "$SLICE" operator takes a multiset of records and returns a multiset of the
     * first column of those records.
     *
     * <p>It is introduced when multisets of scalar types are created, in order to keep types
     * consistent. For example, <code>MULTISET [5]</code> has type <code>INTEGER MULTISET</code> but
     * is translated to an expression of type <code>RECORD(INTEGER EXPR$0) MULTISET</code> because
     * in our internal representation of multisets, every element must be a record. Applying the
     * "$SLICE" operator to this result converts the type back to an <code>
     * INTEGER MULTISET</code> multiset value.
     *
     * <p><code>$SLICE</code> is often translated away when the multiset type is converted back to
     * scalar values.
     */
    public static final SqlInternalOperator SLICE =
            new SqlInternalOperator(
                    "$SLICE",
                    SqlKind.OTHER,
                    0,
                    false,
                    ReturnTypes.MULTISET_PROJECT0,
                    null,
                    OperandTypes.RECORD_COLLECTION) {};

    /**
     * The internal "$ELEMENT_SLICE" operator returns the first field of the only element of a
     * multiset.
     *
     * <p>It is introduced when multisets of scalar types are created, in order to keep types
     * consistent. For example, <code>ELEMENT(MULTISET [5])</code> is translated to <code>
     * $ELEMENT_SLICE(MULTISET (VALUES ROW (5
     * EXPR$0))</code> It is translated away when the multiset type is converted back to scalar
     * values.
     *
     * <p>NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but I'm not deleting the
     * operator, because some multiset tests are disabled, and we may need this operator to get them
     * working!
     */
    public static final SqlInternalOperator ELEMENT_SLICE =
            new SqlInternalOperator(
                    "$ELEMENT_SLICE",
                    SqlKind.OTHER,
                    0,
                    false,
                    ReturnTypes.MULTISET_RECORD,
                    null,
                    OperandTypes.MULTISET) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    SqlUtil.unparseFunctionSyntax(this, writer, call, false);
                }
            };

    /**
     * The internal "$SCALAR_QUERY" operator returns a scalar value from a record type. It assumes
     * the record type only has one field, and returns that field as the output.
     */
    public static final SqlInternalOperator SCALAR_QUERY =
            new SqlInternalOperator(
                    "$SCALAR_QUERY",
                    SqlKind.SCALAR_QUERY,
                    0,
                    false,
                    ReturnTypes.RECORD_TO_SCALAR,
                    null,
                    OperandTypes.RECORD_TO_SCALAR) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    final SqlWriter.Frame frame = writer.startList("(", ")");
                    call.operand(0).unparse(writer, 0, 0);
                    writer.endList(frame);
                }

                @Override
                public boolean argumentMustBeScalar(int ordinal) {
                    // Obvious, really.
                    return false;
                }
            };

    /**
     * The internal {@code $STRUCT_ACCESS} operator is used to access a field of a record.
     *
     * <p>In contrast with {@link #DOT} operator, it never appears in an {@link SqlNode} tree and
     * allows to access fields by position and not by name.
     */
    public static final SqlInternalOperator STRUCT_ACCESS =
            new SqlInternalOperator("$STRUCT_ACCESS", SqlKind.OTHER);

    /**
     * The CARDINALITY operator, used to retrieve the number of elements in a MULTISET, ARRAY or
     * MAP.
     */
    public static final SqlFunction CARDINALITY =
            SqlBasicFunction.create(
                    "CARDINALITY", ReturnTypes.INTEGER_NULLABLE, OperandTypes.COLLECTION_OR_MAP);

    /** The COLLECT operator. Multiset aggregator function. */
    public static final SqlAggFunction COLLECT =
            SqlBasicAggFunction.create(SqlKind.COLLECT, ReturnTypes.TO_MULTISET, OperandTypes.ANY)
                    .withFunctionType(SqlFunctionCategory.SYSTEM)
                    .withGroupOrder(Optionality.OPTIONAL);

    /**
     * {@code PERCENTILE_CONT} inverse distribution aggregate function.
     *
     * <p>The argument must be a numeric literal in the range 0 to 1 inclusive (representing a
     * percentage), and the return type is the type of the {@code ORDER BY} expression.
     */
    public static final SqlAggFunction PERCENTILE_CONT =
            SqlBasicAggFunction.create(
                            SqlKind.PERCENTILE_CONT,
                            ReturnTypes.PERCENTILE_DISC_CONT,
                            OperandTypes.UNIT_INTERVAL_NUMERIC_LITERAL)
                    .withFunctionType(SqlFunctionCategory.SYSTEM)
                    .withGroupOrder(Optionality.MANDATORY)
                    .withPercentile(true)
                    .withAllowsFraming(false);

    /**
     * {@code PERCENTILE_DISC} inverse distribution aggregate function.
     *
     * <p>The argument must be a numeric literal in the range 0 to 1 inclusive (representing a
     * percentage), and the return type is the type of the {@code ORDER BY} expression.
     */
    public static final SqlAggFunction PERCENTILE_DISC =
            SqlBasicAggFunction.create(
                            SqlKind.PERCENTILE_DISC,
                            ReturnTypes.PERCENTILE_DISC_CONT,
                            OperandTypes.UNIT_INTERVAL_NUMERIC_LITERAL)
                    .withFunctionType(SqlFunctionCategory.SYSTEM)
                    .withGroupOrder(Optionality.MANDATORY)
                    .withPercentile(true)
                    .withAllowsFraming(false);

    /** The LISTAGG operator. String aggregator function. */
    public static final SqlAggFunction LISTAGG =
            new SqlListaggAggFunction(SqlKind.LISTAGG, ReturnTypes.ARG0_NULLABLE);

    /** The FUSION operator. Multiset aggregator function. */
    public static final SqlAggFunction FUSION =
            SqlBasicAggFunction.create(SqlKind.FUSION, ReturnTypes.ARG0, OperandTypes.MULTISET)
                    .withFunctionType(SqlFunctionCategory.SYSTEM);

    /** The INTERSECTION operator. Multiset aggregator function. */
    public static final SqlAggFunction INTERSECTION =
            SqlBasicAggFunction.create(
                            SqlKind.INTERSECTION, ReturnTypes.ARG0, OperandTypes.MULTISET)
                    .withFunctionType(SqlFunctionCategory.SYSTEM);

    /** The sequence next value function: <code>NEXT VALUE FOR sequence</code>. */
    public static final SqlOperator NEXT_VALUE = new SqlSequenceValueOperator(SqlKind.NEXT_VALUE);

    /**
     * The sequence current value function: <code>CURRENT VALUE FOR
     * sequence</code>.
     */
    public static final SqlOperator CURRENT_VALUE =
            new SqlSequenceValueOperator(SqlKind.CURRENT_VALUE);

    /**
     * The <code>TABLESAMPLE</code> operator.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li><code>&lt;query&gt; TABLESAMPLE SUBSTITUTE('sampleName')</code> (non-standard)
     *   <li><code>&lt;query&gt; TABLESAMPLE BERNOULLI(&lt;percent&gt;)
     * [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS yet)
     *   <li><code>&lt;query&gt; TABLESAMPLE SYSTEM(&lt;percent&gt;)
     * [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS yet)
     * </ul>
     *
     * <p>Operand #0 is a query or table; Operand #1 is a {@link SqlSampleSpec} wrapped in a {@link
     * SqlLiteral}.
     */
    public static final SqlSpecialOperator TABLESAMPLE =
            new SqlSpecialOperator(
                    "TABLESAMPLE",
                    SqlKind.TABLESAMPLE,
                    20,
                    true,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.VARIADIC) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    call.operand(0).unparse(writer, leftPrec, 0);
                    writer.keyword("TABLESAMPLE");
                    call.operand(1).unparse(writer, 0, rightPrec);
                }
            };

    /** DESCRIPTOR(column_name, ...). */
    public static final SqlOperator DESCRIPTOR = new SqlDescriptorOperator();

    /** TUMBLE as a table function. */
    public static final SqlFunction TUMBLE = new SqlTumbleTableFunction();

    /** HOP as a table function. */
    public static final SqlFunction HOP = new SqlHopTableFunction();

    /** SESSION as a table function. */
    public static final SqlFunction SESSION = new SqlSessionTableFunction();

    /**
     * The {@code TUMBLE} group function.
     *
     * <p>This operator is named "$TUMBLE" (not "TUMBLE") because it is created directly by the
     * parser, not by looking up an operator by name.
     *
     * <p>Why did we add TUMBLE to the parser? Because we plan to support TUMBLE as a table function
     * (see [CALCITE-3272]); "TUMBLE" as a name will only be used by the TUMBLE table function.
     *
     * <p>After the TUMBLE table function is introduced, we plan to deprecate this TUMBLE group
     * function, and in fact all group functions. See [CALCITE-3340] for details.
     */
    public static final SqlGroupedWindowFunction TUMBLE_OLD =
            new SqlGroupedWindowFunction(
                    "$TUMBLE",
                    SqlKind.TUMBLE,
                    null,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.DATETIME_INTERVAL.or(OperandTypes.DATETIME_INTERVAL_TIME),
                    SqlFunctionCategory.SYSTEM) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return ImmutableList.of(TUMBLE_START, TUMBLE_END);
                }
            };

    /** The {@code TUMBLE_START} auxiliary function of the {@code TUMBLE} group function. */
    public static final SqlGroupedWindowFunction TUMBLE_START =
            TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_START);

    /** The {@code TUMBLE_END} auxiliary function of the {@code TUMBLE} group function. */
    public static final SqlGroupedWindowFunction TUMBLE_END =
            TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_END);

    /** The {@code HOP} group function. */
    public static final SqlGroupedWindowFunction HOP_OLD =
            new SqlGroupedWindowFunction(
                    "$HOP",
                    SqlKind.HOP,
                    null,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.DATETIME_INTERVAL_INTERVAL.or(
                            OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME),
                    SqlFunctionCategory.SYSTEM) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return ImmutableList.of(HOP_START, HOP_END);
                }
            };

    /** The {@code HOP_START} auxiliary function of the {@code HOP} group function. */
    public static final SqlGroupedWindowFunction HOP_START = HOP_OLD.auxiliary(SqlKind.HOP_START);

    /** The {@code HOP_END} auxiliary function of the {@code HOP} group function. */
    public static final SqlGroupedWindowFunction HOP_END = HOP_OLD.auxiliary(SqlKind.HOP_END);

    /** The {@code SESSION} group function. */
    public static final SqlGroupedWindowFunction SESSION_OLD =
            new SqlGroupedWindowFunction(
                    "$SESSION",
                    SqlKind.SESSION,
                    null,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.DATETIME_INTERVAL.or(OperandTypes.DATETIME_INTERVAL_TIME),
                    SqlFunctionCategory.SYSTEM) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return ImmutableList.of(SESSION_START, SESSION_END);
                }
            };

    /** The {@code SESSION_START} auxiliary function of the {@code SESSION} group function. */
    public static final SqlGroupedWindowFunction SESSION_START =
            SESSION_OLD.auxiliary(SqlKind.SESSION_START);

    /** The {@code SESSION_END} auxiliary function of the {@code SESSION} group function. */
    public static final SqlGroupedWindowFunction SESSION_END =
            SESSION_OLD.auxiliary(SqlKind.SESSION_END);

    /**
     * {@code |} operator to create alternate patterns within {@code MATCH_RECOGNIZE}.
     *
     * <p>If {@code p1} and {@code p2} are patterns then {@code p1 | p2} is a pattern that matches
     * {@code p1} or {@code p2}.
     */
    public static final SqlBinaryOperator PATTERN_ALTER =
            new SqlBinaryOperator("|", SqlKind.PATTERN_ALTER, 70, true, null, null, null);

    /**
     * Operator to concatenate patterns within {@code MATCH_RECOGNIZE}.
     *
     * <p>If {@code p1} and {@code p2} are patterns then {@code p1 p2} is a pattern that matches
     * {@code p1} followed by {@code p2}.
     */
    public static final SqlBinaryOperator PATTERN_CONCAT =
            new SqlBinaryOperator("", SqlKind.PATTERN_CONCAT, 80, true, null, null, null);

    /**
     * Operator to quantify patterns within {@code MATCH_RECOGNIZE}.
     *
     * <p>If {@code p} is a pattern then {@code p{3, 5}} is a pattern that matches between 3 and 5
     * occurrences of {@code p}.
     */
    public static final SqlSpecialOperator PATTERN_QUANTIFIER =
            new SqlSpecialOperator("PATTERN_QUANTIFIER", SqlKind.PATTERN_QUANTIFIER, 90) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    call.operand(0).unparse(writer, this.getLeftPrec(), this.getRightPrec());
                    int startNum = ((SqlNumericLiteral) call.operand(1)).intValue(true);
                    SqlNumericLiteral endRepNum = call.operand(2);
                    boolean isReluctant = ((SqlLiteral) call.operand(3)).booleanValue();
                    int endNum = endRepNum.intValue(true);
                    if (startNum == endNum) {
                        writer.keyword("{ " + startNum + " }");
                    } else {
                        if (endNum == -1) {
                            if (startNum == 0) {
                                writer.keyword("*");
                            } else if (startNum == 1) {
                                writer.keyword("+");
                            } else {
                                writer.keyword("{ " + startNum + ", }");
                            }
                        } else {
                            if (startNum == 0 && endNum == 1) {
                                writer.keyword("?");
                            } else if (startNum == -1) {
                                writer.keyword("{ , " + endNum + " }");
                            } else {
                                writer.keyword("{ " + startNum + ", " + endNum + " }");
                            }
                        }
                        if (isReluctant) {
                            writer.keyword("?");
                        }
                    }
                }
            };

    /**
     * {@code PERMUTE} operator to combine patterns within {@code MATCH_RECOGNIZE}.
     *
     * <p>If {@code p1} and {@code p2} are patterns then {@code PERMUTE (p1, p2)} is a pattern that
     * matches all permutations of {@code p1} and {@code p2}.
     */
    public static final SqlSpecialOperator PATTERN_PERMUTE =
            new SqlSpecialOperator("PATTERN_PERMUTE", SqlKind.PATTERN_PERMUTE, 100) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    writer.keyword("PERMUTE");
                    SqlWriter.Frame frame = writer.startList("(", ")");
                    for (int i = 0; i < call.getOperandList().size(); i++) {
                        SqlNode pattern = call.getOperandList().get(i);
                        pattern.unparse(writer, 0, 0);
                        if (i != call.getOperandList().size() - 1) {
                            writer.print(",");
                        }
                    }
                    writer.endList(frame);
                }
            };

    /**
     * {@code EXCLUDE} operator within {@code MATCH_RECOGNIZE}.
     *
     * <p>If {@code p} is a pattern then {@code {- p -} }} is a pattern that excludes {@code p} from
     * the output.
     */
    public static final SqlSpecialOperator PATTERN_EXCLUDE =
            new SqlSpecialOperator("PATTERN_EXCLUDE", SqlKind.PATTERN_EXCLUDED, 100) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    SqlWriter.Frame frame = writer.startList("{-", "-}");
                    SqlNode node = call.getOperandList().get(0);
                    node.unparse(writer, 0, 0);
                    writer.endList(frame);
                }
            };

    /** SetSemanticsTable represents as an input table with set semantics. */
    public static final SqlInternalOperator SET_SEMANTICS_TABLE =
            new SqlSetSemanticsTableOperator();

    // ~ Methods ----------------------------------------------------------------

    /** Returns the standard operator table, creating it if necessary. */
    public static SqlStdOperatorTable instance() {
        return INSTANCE.get();
    }

    @Override
    protected void lookUpOperators(
            String name, boolean caseSensitive, Consumer<SqlOperator> consumer) {
        // Only UDFs are looked up using case-sensitive search.
        // Always look up built-in operators case-insensitively. Even in sessions
        // with unquotedCasing=UNCHANGED and caseSensitive=true.
        super.lookUpOperators(name, false, consumer);
    }

    /**
     * Returns the group function for which a given kind is an auxiliary function, or null if it is
     * not an auxiliary function.
     */
    public static @Nullable SqlGroupedWindowFunction auxiliaryToGroup(SqlKind kind) {
        switch (kind) {
            case TUMBLE_START:
            case TUMBLE_END:
                return TUMBLE_OLD;
            case HOP_START:
            case HOP_END:
                return HOP_OLD;
            case SESSION_START:
            case SESSION_END:
                return SESSION_OLD;
            default:
                return null;
        }
    }

    /**
     * Converts a call to a grouped auxiliary function to a call to the grouped window function. For
     * other calls returns null.
     *
     * <p>For example, converts {@code TUMBLE_START(rowtime, INTERVAL '1' HOUR))} to {@code
     * TUMBLE(rowtime, INTERVAL '1' HOUR))}.
     */
    public static @Nullable SqlCall convertAuxiliaryToGroupCall(SqlCall call) {
        final SqlOperator op = call.getOperator();
        if (op instanceof SqlGroupedWindowFunction && op.isGroupAuxiliary()) {
            final SqlGroupedWindowFunction fun = (SqlGroupedWindowFunction) op;
            return copy(call, requireNonNull(fun.groupFunction, "groupFunction"));
        }
        return null;
    }

    /**
     * Converts a call to a grouped window function to a call to its auxiliary window function(s).
     */
    @Deprecated // to be removed before 2.0
    public static List<Pair<SqlNode, AuxiliaryConverter>> convertGroupToAuxiliaryCalls(
            SqlCall call) {
        ImmutableList.Builder<Pair<SqlNode, AuxiliaryConverter>> builder = ImmutableList.builder();
        convertGroupToAuxiliaryCalls(call, (k, v) -> builder.add(Pair.of(k, v)));
        return builder.build();
    }

    /**
     * Converts a call to a grouped window function to a call to its auxiliary window function(s).
     *
     * <p>For example, converts {@code TUMBLE_START(rowtime, INTERVAL '1' HOUR))} to {@code
     * TUMBLE(rowtime, INTERVAL '1' HOUR))}.
     */
    public static void convertGroupToAuxiliaryCalls(
            SqlCall call, BiConsumer<SqlNode, AuxiliaryConverter> consumer) {
        final SqlOperator op = call.getOperator();
        if (op instanceof SqlGroupedWindowFunction && op.isGroup()) {
            final SqlGroupedWindowFunction fun = (SqlGroupedWindowFunction) op;
            fun.getAuxiliaryFunctions()
                    .forEach(f -> consumer.accept(copy(call, f), new AuxiliaryConverter.Impl(f)));
        }
    }

    /** Creates a copy of a call with a new operator. */
    private static SqlCall copy(SqlCall call, SqlOperator operator) {
        return new SqlBasicCall(operator, call.getOperandList(), call.getParserPosition());
    }

    /** Returns the operator for {@code SOME comparisonKind}. */
    public static SqlQuantifyOperator some(SqlKind comparisonKind) {
        switch (comparisonKind) {
            case EQUALS:
                return SOME_EQ;
            case NOT_EQUALS:
                return SOME_NE;
            case LESS_THAN:
                return SOME_LT;
            case LESS_THAN_OR_EQUAL:
                return SOME_LE;
            case GREATER_THAN:
                return SOME_GT;
            case GREATER_THAN_OR_EQUAL:
                return SOME_GE;
            default:
                throw new AssertionError(comparisonKind);
        }
    }

    /** Returns the operator for {@code ALL comparisonKind}. */
    public static SqlQuantifyOperator all(SqlKind comparisonKind) {
        switch (comparisonKind) {
            case EQUALS:
                return ALL_EQ;
            case NOT_EQUALS:
                return ALL_NE;
            case LESS_THAN:
                return ALL_LT;
            case LESS_THAN_OR_EQUAL:
                return ALL_LE;
            case GREATER_THAN:
                return ALL_GT;
            case GREATER_THAN_OR_EQUAL:
                return ALL_GE;
            default:
                throw new AssertionError(comparisonKind);
        }
    }

    /**
     * Returns the binary operator that corresponds to this operator but in the opposite direction.
     * Or returns this, if its kind is not reversible.
     *
     * <p>For example, {@code reverse(GREATER_THAN)} returns {@link #LESS_THAN}.
     *
     * @deprecated Use {@link SqlOperator#reverse()}, but beware that it has slightly different
     *     semantics
     */
    @Deprecated // to be removed before 2.0
    public static SqlOperator reverse(SqlOperator operator) {
        switch (operator.getKind()) {
            case GREATER_THAN:
                return LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            default:
                return operator;
        }
    }

    /** Returns the operator for {@code LIKE} with given case-sensitivity, optionally negated. */
    public static SqlOperator like(boolean negated, boolean caseSensitive) {
        if (negated) {
            if (caseSensitive) {
                return NOT_LIKE;
            } else {
                return SqlLibraryOperators.NOT_ILIKE;
            }
        } else {
            if (caseSensitive) {
                return LIKE;
            } else {
                return SqlLibraryOperators.ILIKE;
            }
        }
    }

    /**
     * Returns the operator for {@code FLOOR} and {@code CEIL} with given floor flag and library.
     */
    public static SqlOperator floorCeil(boolean floor, SqlConformance conformance) {
        if (SqlConformanceEnum.BIG_QUERY == conformance) {
            return floor ? SqlLibraryOperators.FLOOR_BIG_QUERY : SqlLibraryOperators.CEIL_BIG_QUERY;
        } else {
            return floor ? SqlStdOperatorTable.FLOOR : SqlStdOperatorTable.CEIL;
        }
    }
}
