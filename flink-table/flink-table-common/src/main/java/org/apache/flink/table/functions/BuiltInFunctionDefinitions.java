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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparision;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.OTHER;
import static org.apache.flink.table.functions.FunctionKind.SCALAR;
import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.COMMON_ARG_NULLABLE;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.NO_ARGS;
import static org.apache.flink.table.types.inference.InputTypeStrategies.OUTPUT_IF_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.SPECIFIC_FOR_CAST;
import static org.apache.flink.table.types.inference.InputTypeStrategies.TWO_EQUALS_COMPARABLE;
import static org.apache.flink.table.types.inference.InputTypeStrategies.TWO_FULLY_COMPARABLE;
import static org.apache.flink.table.types.inference.InputTypeStrategies.and;
import static org.apache.flink.table.types.inference.InputTypeStrategies.commonType;
import static org.apache.flink.table.types.inference.InputTypeStrategies.comparable;
import static org.apache.flink.table.types.inference.InputTypeStrategies.compositeSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.wildcardWithCount;
import static org.apache.flink.table.types.inference.TypeStrategies.COMMON;
import static org.apache.flink.table.types.inference.TypeStrategies.DECIMAL_DIVIDE;
import static org.apache.flink.table.types.inference.TypeStrategies.DECIMAL_MOD;
import static org.apache.flink.table.types.inference.TypeStrategies.DECIMAL_PLUS;
import static org.apache.flink.table.types.inference.TypeStrategies.DECIMAL_SCALE0;
import static org.apache.flink.table.types.inference.TypeStrategies.DECIMAL_TIMES;
import static org.apache.flink.table.types.inference.TypeStrategies.STRING_CONCAT;
import static org.apache.flink.table.types.inference.TypeStrategies.argument;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.table.types.inference.TypeStrategies.first;
import static org.apache.flink.table.types.inference.TypeStrategies.matchFamily;
import static org.apache.flink.table.types.inference.TypeStrategies.nullable;
import static org.apache.flink.table.types.inference.TypeStrategies.varyingString;

/** Dictionary of function definitions for all built-in functions. */
@PublicEvolving
public final class BuiltInFunctionDefinitions {

    // --------------------------------------------------------------------------------------------
    // New stack built-in functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition TYPE_OF =
            BuiltInFunctionDefinition.newBuilder()
                    .name("TYPEOF")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            new String[] {"input"},
                                            new ArgumentTypeStrategy[] {ANY}),
                                    sequence(
                                            new String[] {"input", "force_serializable"},
                                            new ArgumentTypeStrategy[] {
                                                ANY, and(logical(LogicalTypeRoot.BOOLEAN), LITERAL)
                                            })))
                    .outputTypeStrategy(explicit(DataTypes.STRING()))
                    .runtimeClass("org.apache.flink.table.runtime.functions.scalar.TypeOfFunction")
                    .build();

    public static final BuiltInFunctionDefinition IF_NULL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("IFNULL")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    new String[] {"input", "null_replacement"},
                                    new ArgumentTypeStrategy[] {
                                        COMMON_ARG_NULLABLE, COMMON_ARG_NULLABLE
                                    }))
                    .outputTypeStrategy(TypeStrategies.IF_NULL)
                    .runtimeClass("org.apache.flink.table.runtime.functions.scalar.IfNullFunction")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Logic functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition AND =
            BuiltInFunctionDefinition.newBuilder()
                    .name("and")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            varyingSequence(
                                    logical(LogicalTypeRoot.BOOLEAN),
                                    logical(LogicalTypeRoot.BOOLEAN),
                                    logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition OR =
            BuiltInFunctionDefinition.newBuilder()
                    .name("or")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            varyingSequence(
                                    logical(LogicalTypeRoot.BOOLEAN),
                                    logical(LogicalTypeRoot.BOOLEAN),
                                    logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition NOT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("not")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition IF =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ifThenElse")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            compositeSequence()
                                    .argument(logical(LogicalTypeRoot.BOOLEAN))
                                    .subsequence(commonType(2))
                                    .finish())
                    .outputTypeStrategy(argument(1))
                    .build();

    // --------------------------------------------------------------------------------------------
    // Comparison functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition EQUALS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("equals")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_EQUALS_COMPARABLE)
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition GREATER_THAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("greaterThan")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition GREATER_THAN_OR_EQUAL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("greaterThanOrEqual")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition LESS_THAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lessThan")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition LESS_THAN_OR_EQUAL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lessThanOrEqual")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition NOT_EQUALS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("notEquals")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_EQUALS_COMPARABLE)
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition IS_NULL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("isNull")
                    .kind(SCALAR)
                    .inputTypeStrategy(wildcardWithCount(ConstantArgumentCount.of(1)))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition IS_NOT_NULL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("isNotNull")
                    .kind(SCALAR)
                    .inputTypeStrategy(wildcardWithCount(ConstantArgumentCount.of(1)))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition IS_TRUE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("isTrue")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition IS_FALSE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("isFalse")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition IS_NOT_TRUE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("isNotTrue")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition IS_NOT_FALSE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("isNotFalse")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition BETWEEN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("between")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            comparable(ConstantArgumentCount.of(3), StructuredComparision.FULL))
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition NOT_BETWEEN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("notBetween")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            comparable(ConstantArgumentCount.of(3), StructuredComparision.FULL))
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    // --------------------------------------------------------------------------------------------
    // Aggregate functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition AVG =
            BuiltInFunctionDefinition.newBuilder()
                    .name("avg")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition COUNT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("count")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition MAX =
            BuiltInFunctionDefinition.newBuilder()
                    .name("max")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition MIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("min")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition SUM =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sum")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition SUM0 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sum0")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition STDDEV_POP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("stddevPop")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition STDDEV_SAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("stddevSamp")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition VAR_POP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("varPop")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition VAR_SAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("varSamp")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition COLLECT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("collect")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition DISTINCT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("distinct")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    // --------------------------------------------------------------------------------------------
    // String functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition CHAR_LENGTH =
            BuiltInFunctionDefinition.newBuilder()
                    .name("charLength")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.INT())))
                    .build();

    public static final BuiltInFunctionDefinition INIT_CAP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("initCap")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition LIKE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("like")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition LOWER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lower")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(argument(0)))
                    .build();

    // we need LOWERCASE here to maintain compatibility for the string-based expression DSL
    // which exposes LOWER as lowerCase()
    public static final BuiltInFunctionDefinition LOWERCASE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lowerCase")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition SIMILAR =
            BuiltInFunctionDefinition.newBuilder()
                    .name("similar")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition SUBSTRING =
            BuiltInFunctionDefinition.newBuilder()
                    .name("substring")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeRoot.INTEGER)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeRoot.INTEGER),
                                            logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(nullable(varyingString(argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition REPLACE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("replace")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition TRIM =
            BuiltInFunctionDefinition.newBuilder()
                    .name("trim")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeRoot.BOOLEAN),
                                    logical(LogicalTypeRoot.BOOLEAN),
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(varyingString(argument(3))))
                    .build();

    public static final BuiltInFunctionDefinition UPPER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("upper")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(argument(0)))
                    .build();

    // we need UPPERCASE here to maintain compatibility for the string-based expression DSL
    // which exposes UPPER as upperCase()
    public static final BuiltInFunctionDefinition UPPERCASE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("upperCase")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition POSITION =
            BuiltInFunctionDefinition.newBuilder()
                    .name("position")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.INT())))
                    .build();

    public static final BuiltInFunctionDefinition OVERLAY =
            BuiltInFunctionDefinition.newBuilder()
                    .name("overlay")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeRoot.INTEGER)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeRoot.INTEGER),
                                            logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(nullable(STRING_CONCAT))
                    .build();

    public static final BuiltInFunctionDefinition CONCAT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("concat")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    varyingSequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    varyingSequence(
                                            logical(LogicalTypeFamily.BINARY_STRING),
                                            logical(LogicalTypeFamily.BINARY_STRING))))
                    .outputTypeStrategy(nullable(STRING_CONCAT))
                    .build();

    public static final BuiltInFunctionDefinition CONCAT_WS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("concat_ws")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            varyingSequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition LPAD =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lpad")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeRoot.INTEGER),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition RPAD =
            BuiltInFunctionDefinition.newBuilder()
                    .name("rpad")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeRoot.INTEGER),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition REGEXP_EXTRACT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("regexpExtract")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition FROM_BASE64 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("fromBase64")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition TO_BASE64 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("toBase64")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition UUID =
            BuiltInFunctionDefinition.newBuilder()
                    .name("uuid")
                    .kind(SCALAR)
                    .notDeterministic()
                    .inputTypeStrategy(NO_ARGS)
                    .outputTypeStrategy(explicit(DataTypes.CHAR(36).notNull()))
                    .build();

    public static final BuiltInFunctionDefinition LTRIM =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ltrim")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullable(varyingString(argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition RTRIM =
            BuiltInFunctionDefinition.newBuilder()
                    .name("rtrim")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullable(varyingString(argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition REPEAT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("repeat")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeRoot.INTEGER)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition REGEXP_REPLACE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("regexpReplace")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    // --------------------------------------------------------------------------------------------
    // Math functions
    // --------------------------------------------------------------------------------------------

    /**
     * Combines numeric addition, "datetime + interval"/"interval + interval" arithmetic, and string
     * concatenation.
     */
    public static final BuiltInFunctionDefinition PLUS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("plus")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeRoot.INTERVAL_DAY_TIME),
                                            logical(LogicalTypeRoot.INTERVAL_DAY_TIME)),
                                    sequence(
                                            logical(LogicalTypeRoot.INTERVAL_YEAR_MONTH),
                                            logical(LogicalTypeRoot.INTERVAL_YEAR_MONTH)),
                                    sequence(
                                            logical(LogicalTypeFamily.DATETIME),
                                            logical(LogicalTypeFamily.INTERVAL)),
                                    sequence(
                                            logical(LogicalTypeFamily.INTERVAL),
                                            logical(LogicalTypeFamily.DATETIME)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.PREDEFINED))))
                    .outputTypeStrategy(
                            nullable(first(DECIMAL_PLUS, COMMON, explicit(DataTypes.STRING()))))
                    .build();

    /** Combines numeric subtraction and "datetime - interval" arithmetic. */
    public static final BuiltInFunctionDefinition MINUS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("minus")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeRoot.INTERVAL_DAY_TIME),
                                            logical(LogicalTypeRoot.INTERVAL_DAY_TIME)),
                                    sequence(
                                            logical(LogicalTypeRoot.INTERVAL_YEAR_MONTH),
                                            logical(LogicalTypeRoot.INTERVAL_YEAR_MONTH)),
                                    sequence(
                                            logical(LogicalTypeFamily.DATETIME),
                                            logical(LogicalTypeFamily.INTERVAL))))
                    .outputTypeStrategy(nullable(first(DECIMAL_PLUS, COMMON)))
                    .build();

    public static final BuiltInFunctionDefinition DIVIDE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("divide")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeFamily.INTERVAL),
                                            logical(LogicalTypeFamily.NUMERIC))))
                    .outputTypeStrategy(
                            nullable(
                                    first(
                                            DECIMAL_DIVIDE,
                                            matchFamily(0, LogicalTypeFamily.INTERVAL),
                                            COMMON)))
                    .build();

    public static final BuiltInFunctionDefinition TIMES =
            BuiltInFunctionDefinition.newBuilder()
                    .name("times")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeFamily.INTERVAL),
                                            logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeFamily.INTERVAL))))
                    .outputTypeStrategy(
                            nullable(
                                    first(
                                            DECIMAL_TIMES,
                                            matchFamily(0, LogicalTypeFamily.INTERVAL),
                                            COMMON)))
                    .build();

    public static final BuiltInFunctionDefinition ABS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("abs")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(logical(LogicalTypeFamily.INTERVAL))))
                    .outputTypeStrategy(argument(0))
                    .build();

    public static final BuiltInFunctionDefinition EXP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("exp")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition FLOOR =
            BuiltInFunctionDefinition.newBuilder()
                    .name("floor")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(logical(LogicalTypeFamily.INTERVAL)),
                                    sequence(
                                            logical(LogicalTypeFamily.DATETIME),
                                            logical(LogicalTypeRoot.SYMBOL))))
                    .outputTypeStrategy(nullable(first(DECIMAL_SCALE0, argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition CEIL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ceil")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(logical(LogicalTypeFamily.INTERVAL)),
                                    sequence(
                                            logical(LogicalTypeFamily.DATETIME),
                                            logical(LogicalTypeRoot.SYMBOL))))
                    .outputTypeStrategy(nullable(first(DECIMAL_SCALE0, argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition LOG10 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("log10")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition LOG2 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("log2")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition LN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ln")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition LOG =
            BuiltInFunctionDefinition.newBuilder()
                    .name("log")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeFamily.NUMERIC))))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition POWER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("power")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.NUMERIC),
                                    logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition MOD =
            BuiltInFunctionDefinition.newBuilder()
                    .name("mod")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.EXACT_NUMERIC),
                                    logical(LogicalTypeFamily.EXACT_NUMERIC)))
                    .outputTypeStrategy(nullable(first(DECIMAL_MOD, argument(1))))
                    .build();

    public static final BuiltInFunctionDefinition SQRT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sqrt")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition MINUS_PREFIX =
            BuiltInFunctionDefinition.newBuilder()
                    .name("minusPrefix")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(OUTPUT_IF_NULL),
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(logical(LogicalTypeFamily.INTERVAL))))
                    .outputTypeStrategy(nullable(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition SIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sin")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition COS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cos")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition SINH =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sinh")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition TAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("tan")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition TANH =
            BuiltInFunctionDefinition.newBuilder()
                    .name("tanh")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition COT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cot")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ASIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("asin")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ACOS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("acos")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ATAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("atan")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ATAN2 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("atan2")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.NUMERIC),
                                    logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition COSH =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cosh")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition DEGREES =
            BuiltInFunctionDefinition.newBuilder()
                    .name("degrees")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition RADIANS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("radians")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition SIGN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sign")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(argument(0))
                    .build();

    public static final BuiltInFunctionDefinition ROUND =
            BuiltInFunctionDefinition.newBuilder()
                    .name("round")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(nullable(TypeStrategies.ROUND))
                    .build();

    public static final BuiltInFunctionDefinition PI =
            BuiltInFunctionDefinition.newBuilder()
                    .name("pi")
                    .kind(SCALAR)
                    .inputTypeStrategy(NO_ARGS)
                    .outputTypeStrategy(explicit(DataTypes.DOUBLE().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition E =
            BuiltInFunctionDefinition.newBuilder()
                    .name("e")
                    .kind(SCALAR)
                    .inputTypeStrategy(NO_ARGS)
                    .outputTypeStrategy(explicit(DataTypes.DOUBLE().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition RAND =
            BuiltInFunctionDefinition.newBuilder()
                    .name("rand")
                    .kind(SCALAR)
                    .notDeterministic()
                    .inputTypeStrategy(or(NO_ARGS, sequence(logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(explicit(DataTypes.DOUBLE().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition RAND_INTEGER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("randInteger")
                    .kind(SCALAR)
                    .notDeterministic()
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeRoot.INTEGER)),
                                    sequence(
                                            logical(LogicalTypeRoot.INTEGER),
                                            logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(explicit(DataTypes.INT().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition BIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("bin")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.INTEGER_NUMERIC)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition HEX =
            BuiltInFunctionDefinition.newBuilder()
                    .name("hex")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.INTEGER_NUMERIC)),
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullable(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition TRUNCATE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("truncate")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(nullable(argument(0)))
                    .build();

    // --------------------------------------------------------------------------------------------
    // Time functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition EXTRACT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("extract")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_DATE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentDate")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_TIME =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentTime")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentTimestamp")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition LOCAL_TIME =
            BuiltInFunctionDefinition.newBuilder()
                    .name("localTime")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition LOCAL_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("localTimestamp")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition TEMPORAL_OVERLAPS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("temporalOverlaps")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition DATE_FORMAT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("dateFormat")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition TIMESTAMP_DIFF =
            BuiltInFunctionDefinition.newBuilder()
                    .name("timestampDiff")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Collection functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition AT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("at")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition CARDINALITY =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cardinality")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition ARRAY =
            BuiltInFunctionDefinition.newBuilder()
                    .name("array")
                    .kind(SCALAR)
                    .inputTypeStrategy(InputTypeStrategies.SPECIFIC_FOR_ARRAY)
                    .outputTypeStrategy(TypeStrategies.ARRAY)
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_ELEMENT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("element")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition MAP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("map")
                    .kind(SCALAR)
                    .inputTypeStrategy(InputTypeStrategies.SPECIFIC_FOR_MAP)
                    .outputTypeStrategy(TypeStrategies.MAP)
                    .build();

    public static final BuiltInFunctionDefinition ROW =
            BuiltInFunctionDefinition.newBuilder()
                    .name("row")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            InputTypeStrategies.wildcardWithCount(ConstantArgumentCount.from(1)))
                    .outputTypeStrategy(TypeStrategies.ROW)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Composite type functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition FLATTEN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("flatten")
                    .kind(OTHER)
                    .inputTypeStrategy(sequence(InputTypeStrategies.COMPOSITE))
                    .outputTypeStrategy(
                            callContext -> {
                                throw new UnsupportedOperationException(
                                        "FLATTEN should be resolved to GET expressions");
                            })
                    .build();

    public static final BuiltInFunctionDefinition GET =
            BuiltInFunctionDefinition.newBuilder()
                    .name("get")
                    .kind(OTHER)
                    .inputTypeStrategy(
                            sequence(
                                    InputTypeStrategies.COMPOSITE,
                                    and(
                                            InputTypeStrategies.LITERAL,
                                            or(
                                                    logical(LogicalTypeRoot.INTEGER),
                                                    logical(LogicalTypeFamily.CHARACTER_STRING)))))
                    .outputTypeStrategy(TypeStrategies.GET)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Crypto hash functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition MD5 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("md5")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.CHAR(32))))
                    .build();

    public static final BuiltInFunctionDefinition SHA1 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha1")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.CHAR(40))))
                    .build();

    public static final BuiltInFunctionDefinition SHA224 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha224")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.CHAR(56))))
                    .build();

    public static final BuiltInFunctionDefinition SHA256 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha256")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.CHAR(64))))
                    .build();

    public static final BuiltInFunctionDefinition SHA384 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha384")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.CHAR(96))))
                    .build();

    public static final BuiltInFunctionDefinition SHA512 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha512")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.CHAR(128))))
                    .build();

    public static final BuiltInFunctionDefinition SHA2 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha2")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeRoot.INTEGER)))
                    .outputTypeStrategy(nullable(explicit(DataTypes.VARCHAR(128))))
                    .build();

    // --------------------------------------------------------------------------------------------
    // Window properties
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition WINDOW_START =
            BuiltInFunctionDefinition.newBuilder()
                    .name("start")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition WINDOW_END =
            BuiltInFunctionDefinition.newBuilder()
                    .name("end")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Ordering
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition ORDER_ASC =
            BuiltInFunctionDefinition.newBuilder()
                    .name("asc")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition ORDER_DESC =
            BuiltInFunctionDefinition.newBuilder()
                    .name("desc")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Time attributes
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition PROCTIME =
            BuiltInFunctionDefinition.newBuilder()
                    .name("proctime")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition ROWTIME =
            BuiltInFunctionDefinition.newBuilder()
                    .name("rowtime")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Over window
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition OVER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("over")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition UNBOUNDED_RANGE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("unboundedRange")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition UNBOUNDED_ROW =
            BuiltInFunctionDefinition.newBuilder()
                    .name("unboundedRow")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_RANGE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentRange")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_ROW =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentRow")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Column functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition WITH_COLUMNS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("withColumns")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition WITHOUT_COLUMNS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("withoutColumns")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Other functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition IN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("in")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition CAST =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cast")
                    .kind(SCALAR)
                    .inputTypeStrategy(SPECIFIC_FOR_CAST)
                    .outputTypeStrategy(
                            nullable(ConstantArgumentCount.to(0), TypeStrategies.argument(1)))
                    .build();

    public static final BuiltInFunctionDefinition REINTERPRET_CAST =
            BuiltInFunctionDefinition.newBuilder()
                    .name("reinterpretCast")
                    .kind(SCALAR)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition AS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("as")
                    .kind(OTHER)
                    .inputTypeStrategy(
                            varyingSequence(
                                    or(OUTPUT_IF_NULL, InputTypeStrategies.ANY),
                                    and(
                                            InputTypeStrategies.LITERAL,
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    and(
                                            InputTypeStrategies.LITERAL,
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(TypeStrategies.argument(0))
                    .build();

    public static final BuiltInFunctionDefinition STREAM_RECORD_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("streamRecordTimestamp")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final BuiltInFunctionDefinition RANGE_TO =
            BuiltInFunctionDefinition.newBuilder()
                    .name("rangeTo")
                    .kind(OTHER)
                    .outputTypeStrategy(TypeStrategies.MISSING)
                    .build();

    public static final Set<FunctionDefinition> WINDOW_PROPERTIES =
            new HashSet<>(Arrays.asList(WINDOW_START, WINDOW_END, PROCTIME, ROWTIME));

    public static final Set<FunctionDefinition> TIME_ATTRIBUTES =
            new HashSet<>(Arrays.asList(PROCTIME, ROWTIME));

    public static final List<FunctionDefinition> ORDERING = Arrays.asList(ORDER_ASC, ORDER_DESC);

    public static List<BuiltInFunctionDefinition> getDefinitions() {
        final Field[] fields = BuiltInFunctionDefinitions.class.getFields();
        final List<BuiltInFunctionDefinition> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (FunctionDefinition.class.isAssignableFrom(field.getType())) {
                try {
                    final BuiltInFunctionDefinition funcDef =
                            (BuiltInFunctionDefinition) field.get(BuiltInFunctionDefinitions.class);
                    list.add(Preconditions.checkNotNull(funcDef));
                } catch (IllegalAccessException e) {
                    throw new TableException(
                            "The function definition for field "
                                    + field.getName()
                                    + " is not accessible.",
                            e);
                }
            }
        }
        return list;
    }
}
