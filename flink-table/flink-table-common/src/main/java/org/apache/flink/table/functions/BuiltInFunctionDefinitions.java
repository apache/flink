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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonType;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.strategies.ArrayOfStringArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies;
import org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.OTHER;
import static org.apache.flink.table.functions.FunctionKind.SCALAR;
import static org.apache.flink.table.functions.FunctionKind.TABLE;
import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.COMMON_ARG_NULLABLE;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.NO_ARGS;
import static org.apache.flink.table.types.inference.InputTypeStrategies.OUTPUT_IF_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.TYPE_LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.and;
import static org.apache.flink.table.types.inference.InputTypeStrategies.arrayFullyComparableElementType;
import static org.apache.flink.table.types.inference.InputTypeStrategies.commonArrayType;
import static org.apache.flink.table.types.inference.InputTypeStrategies.commonMultipleArrayType;
import static org.apache.flink.table.types.inference.InputTypeStrategies.commonType;
import static org.apache.flink.table.types.inference.InputTypeStrategies.comparable;
import static org.apache.flink.table.types.inference.InputTypeStrategies.compositeSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.symbol;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.wildcardWithCount;
import static org.apache.flink.table.types.inference.TypeStrategies.COMMON;
import static org.apache.flink.table.types.inference.TypeStrategies.argument;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.table.types.inference.TypeStrategies.first;
import static org.apache.flink.table.types.inference.TypeStrategies.forceNullable;
import static org.apache.flink.table.types.inference.TypeStrategies.matchFamily;
import static org.apache.flink.table.types.inference.TypeStrategies.nullableIfAllArgs;
import static org.apache.flink.table.types.inference.TypeStrategies.nullableIfArgs;
import static org.apache.flink.table.types.inference.TypeStrategies.varyingString;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.ARRAY_ELEMENT_ARG;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.JSON_ARGUMENT;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.TWO_EQUALS_COMPARABLE;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.TWO_FULLY_COMPARABLE;

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
                                    Arrays.asList("input", "null_replacement"),
                                    Arrays.asList(COMMON_ARG_NULLABLE, COMMON_ARG_NULLABLE)))
                    .outputTypeStrategy(SpecificTypeStrategies.IF_NULL)
                    .runtimeClass("org.apache.flink.table.runtime.functions.scalar.IfNullFunction")
                    .build();

    public static final BuiltInFunctionDefinition MAP_KEYS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("MAP_KEYS")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    new String[] {"input"},
                                    new ArgumentTypeStrategy[] {logical(LogicalTypeRoot.MAP)}))
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.MAP_KEYS))
                    .runtimeClass("org.apache.flink.table.runtime.functions.scalar.MapKeysFunction")
                    .build();

    public static final BuiltInFunctionDefinition MAP_VALUES =
            BuiltInFunctionDefinition.newBuilder()
                    .name("MAP_VALUES")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    new String[] {"input"},
                                    new ArgumentTypeStrategy[] {logical(LogicalTypeRoot.MAP)}))
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.MAP_VALUES))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.MapValuesFunction")
                    .build();

    public static final BuiltInFunctionDefinition MAP_ENTRIES =
            BuiltInFunctionDefinition.newBuilder()
                    .name("MAP_ENTRIES")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    new String[] {"input"},
                                    new ArgumentTypeStrategy[] {logical(LogicalTypeRoot.MAP)}))
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.MAP_ENTRIES))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.MapEntriesFunction")
                    .build();

    public static final BuiltInFunctionDefinition MAP_FROM_ARRAYS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("MAP_FROM_ARRAYS")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    new String[] {"keysArray", "valuesArray"},
                                    new ArgumentTypeStrategy[] {
                                        logical(LogicalTypeRoot.ARRAY),
                                        logical(LogicalTypeRoot.ARRAY)
                                    }))
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.MAP_FROM_ARRAYS))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.MapFromArraysFunction")
                    .build();

    public static final BuiltInFunctionDefinition SOURCE_WATERMARK =
            BuiltInFunctionDefinition.newBuilder()
                    .name("SOURCE_WATERMARK")
                    .kind(SCALAR)
                    .inputTypeStrategy(NO_ARGS)
                    .outputTypeStrategy(SpecificTypeStrategies.SOURCE_WATERMARK)
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.SourceWatermarkFunction")
                    .build();

    public static final BuiltInFunctionDefinition COALESCE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("COALESCE")
                    .kind(SCALAR)
                    .inputTypeStrategy(varyingSequence(COMMON_ARG_NULLABLE, COMMON_ARG_NULLABLE))
                    .outputTypeStrategy(nullableIfAllArgs(COMMON))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.CoalesceFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_CONTAINS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_CONTAINS")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    Arrays.asList("haystack", "needle"),
                                    Arrays.asList(
                                            logical(LogicalTypeRoot.ARRAY), ARRAY_ELEMENT_ARG)))
                    .outputTypeStrategy(
                            nullableIfArgs(
                                    ConstantArgumentCount.of(0), explicit(DataTypes.BOOLEAN())))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayContainsFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_DISTINCT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_DISTINCT")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    Collections.singletonList("haystack"),
                                    Collections.singletonList(logical(LogicalTypeRoot.ARRAY))))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayDistinctFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_POSITION =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_POSITION")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    Arrays.asList("haystack", "needle"),
                                    Arrays.asList(
                                            logical(LogicalTypeRoot.ARRAY), ARRAY_ELEMENT_ARG)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.INT())))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayPositionFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_REMOVE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_REMOVE")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    Arrays.asList("haystack", "needle"),
                                    Arrays.asList(
                                            logical(LogicalTypeRoot.ARRAY), ARRAY_ELEMENT_ARG)))
                    .outputTypeStrategy(nullableIfArgs(ConstantArgumentCount.of(0), argument(0)))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayRemoveFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_REVERSE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_REVERSE")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    Collections.singletonList("haystack"),
                                    Collections.singletonList(logical(LogicalTypeRoot.ARRAY))))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayReverseFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_SLICE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_SLICE")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeRoot.ARRAY),
                                            logical(LogicalTypeRoot.INTEGER),
                                            logical(LogicalTypeRoot.INTEGER)),
                                    sequence(
                                            logical(LogicalTypeRoot.ARRAY),
                                            logical(LogicalTypeRoot.INTEGER))))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArraySliceFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_UNION =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_UNION")
                    .kind(SCALAR)
                    .inputTypeStrategy(commonArrayType(2))
                    .outputTypeStrategy(nullableIfArgs(COMMON))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayUnionFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_CONCAT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_CONCAT")
                    .kind(SCALAR)
                    .inputTypeStrategy(commonMultipleArrayType(1))
                    .outputTypeStrategy(nullableIfArgs(COMMON))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayConcatFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_MAX =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_MAX")
                    .kind(SCALAR)
                    .inputTypeStrategy(arrayFullyComparableElementType())
                    .outputTypeStrategy(forceNullable(SpecificTypeStrategies.ARRAY_ELEMENT))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayMaxFunction")
                    .build();

    public static final BuiltInFunctionDefinition ARRAY_JOIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ARRAY_JOIN")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            new ArrayOfStringArgumentTypeStrategy(),
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            new ArrayOfStringArgumentTypeStrategy(),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING().nullable())))
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.scalar.ArrayJoinFunction")
                    .build();

    public static final BuiltInFunctionDefinition INTERNAL_REPLICATE_ROWS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("$REPLICATE_ROWS$1")
                    .kind(TABLE)
                    .outputTypeStrategy(SpecificTypeStrategies.INTERNAL_REPLICATE_ROWS)
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.table.ReplicateRowsFunction")
                    .internal()
                    .build();

    public static final BuiltInFunctionDefinition INTERNAL_UNNEST_ROWS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("$UNNEST_ROWS$1")
                    .kind(TABLE)
                    .outputTypeStrategy(SpecificTypeStrategies.UNUSED)
                    .runtimeClass(
                            "org.apache.flink.table.runtime.functions.table.UnnestRowsFunction")
                    .internal()
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition NOT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("not")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeRoot.BOOLEAN)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition GREATER_THAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("greaterThan")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition GREATER_THAN_OR_EQUAL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("greaterThanOrEqual")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition LESS_THAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lessThan")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition LESS_THAN_OR_EQUAL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lessThanOrEqual")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_FULLY_COMPARABLE)
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition NOT_EQUALS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("notEquals")
                    .kind(SCALAR)
                    .inputTypeStrategy(TWO_EQUALS_COMPARABLE)
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
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
                            comparable(ConstantArgumentCount.of(3), StructuredComparison.FULL))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition NOT_BETWEEN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("notBetween")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            comparable(ConstantArgumentCount.of(3), StructuredComparison.FULL))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition GREATEST =
            BuiltInFunctionDefinition.newBuilder()
                    .name("GREATEST")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            comparable(ConstantArgumentCount.from(1), StructuredComparison.FULL))
                    .outputTypeStrategy(nullableIfArgs(TypeStrategies.COMMON))
                    .runtimeProvided()
                    .build();

    public static final BuiltInFunctionDefinition LEAST =
            BuiltInFunctionDefinition.newBuilder()
                    .name("LEAST")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            comparable(ConstantArgumentCount.from(1), StructuredComparison.FULL))
                    .outputTypeStrategy(nullableIfArgs(TypeStrategies.COMMON))
                    .runtimeProvided()
                    .build();

    // --------------------------------------------------------------------------------------------
    // Aggregate functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition AVG =
            BuiltInFunctionDefinition.newBuilder()
                    .name("avg")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(
                            TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                    .build();

    public static final BuiltInFunctionDefinition COUNT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("count")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(ANY)) // COUNT(*) is not supported yet
                    .outputTypeStrategy(explicit(BIGINT().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition MAX =
            BuiltInFunctionDefinition.newBuilder()
                    .name("max")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(
                            comparable(ConstantArgumentCount.of(1), StructuredComparison.FULL))
                    .outputTypeStrategy(TypeStrategies.aggArg0(t -> t, true))
                    .build();

    public static final BuiltInFunctionDefinition MIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("min")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(
                            comparable(ConstantArgumentCount.of(1), StructuredComparison.FULL))
                    .outputTypeStrategy(TypeStrategies.aggArg0(t -> t, true))
                    .build();

    public static final BuiltInFunctionDefinition FIRST_VALUE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("first_value")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.aggArg0(t -> t, true))
                    .build();

    public static final BuiltInFunctionDefinition LAST_VALUE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("last_value")
                    .kind(AGGREGATE)
                    .outputTypeStrategy(TypeStrategies.aggArg0(t -> t, true))
                    .build();

    public static final BuiltInFunctionDefinition LISTAGG =
            BuiltInFunctionDefinition.newBuilder()
                    .name("listAgg")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(ANY, logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(explicit(STRING().nullable()))
                    .build();

    public static final BuiltInFunctionDefinition SUM =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sum")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(
                            TypeStrategies.aggArg0(LogicalTypeMerging::findSumAggType, true))
                    .build();

    public static final BuiltInFunctionDefinition SUM0 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sum0")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(
                            TypeStrategies.aggArg0(LogicalTypeMerging::findSumAggType, false))
                    .build();

    public static final BuiltInFunctionDefinition STDDEV_POP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("stddevPop")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(
                            TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                    .build();

    public static final BuiltInFunctionDefinition STDDEV_SAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("stddevSamp")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(
                            TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                    .build();

    public static final BuiltInFunctionDefinition VAR_POP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("varPop")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(
                            TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                    .build();

    public static final BuiltInFunctionDefinition VAR_SAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("varSamp")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(
                            TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(INT())))
                    .build();

    public static final BuiltInFunctionDefinition INIT_CAP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("initCap")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition LIKE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("like")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition LOWER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lower")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .build();

    // we need LOWERCASE here to maintain compatibility for the string-based expression DSL
    // which exposes LOWER as lowerCase()
    public static final BuiltInFunctionDefinition LOWERCASE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("lowerCase")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition SIMILAR =
            BuiltInFunctionDefinition.newBuilder()
                    .name("similar")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
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
                    .outputTypeStrategy(nullableIfArgs(varyingString(argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition SUBSTR =
            BuiltInFunctionDefinition.newBuilder()
                    .name("substr")
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
                    .outputTypeStrategy(nullableIfArgs(varyingString(argument(0))))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
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
                    .outputTypeStrategy(nullableIfArgs(varyingString(argument(3))))
                    .build();

    public static final BuiltInFunctionDefinition UPPER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("upper")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .build();

    // we need UPPERCASE here to maintain compatibility for the string-based expression DSL
    // which exposes UPPER as upperCase()
    public static final BuiltInFunctionDefinition UPPERCASE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("upperCase")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition POSITION =
            BuiltInFunctionDefinition.newBuilder()
                    .name("position")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(INT())))
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
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.STRING_CONCAT))
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
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.STRING_CONCAT))
                    .build();

    public static final BuiltInFunctionDefinition CONCAT_WS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("concat_ws")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            varyingSequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
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
                    .outputTypeStrategy(explicit(DataTypes.STRING().nullable()))
                    .build();

    public static final BuiltInFunctionDefinition FROM_BASE64 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("fromBase64")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition TO_BASE64 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("toBase64")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition ASCII =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ascii")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.TINYINT())))
                    .build();

    public static final BuiltInFunctionDefinition CHR =
            BuiltInFunctionDefinition.newBuilder()
                    .name("chr")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.INTEGER_NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.CHAR(1))))
                    .build();

    public static final BuiltInFunctionDefinition DECODE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("decode")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.BINARY_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition ENCODE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("encode")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BYTES())))
                    .build();

    public static final BuiltInFunctionDefinition LEFT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("left")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.INTEGER_NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(varyingString(argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition RIGHT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("right")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.INTEGER_NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(varyingString(argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition INSTR =
            BuiltInFunctionDefinition.newBuilder()
                    .name("instr")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(INT())))
                    .build();

    public static final BuiltInFunctionDefinition LOCATE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("locate")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.INTEGER_NUMERIC))))
                    .outputTypeStrategy(nullableIfArgs(explicit(INT())))
                    .build();

    public static final BuiltInFunctionDefinition PARSE_URL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("parseUrl")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
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
                    .outputTypeStrategy(nullableIfArgs(varyingString(argument(0))))
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
                    .outputTypeStrategy(nullableIfArgs(varyingString(argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition REPEAT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("repeat")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeRoot.INTEGER)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition REGEXP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("regexp")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.BOOLEAN())))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition REVERSE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("reverse")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition SPLIT_INDEX =
            BuiltInFunctionDefinition.newBuilder()
                    .name("splitIndex")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeRoot.INTEGER)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition STR_TO_MAP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("strToMap")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(
                            nullableIfArgs(
                                    explicit(
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))))
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
                            nullableIfArgs(
                                    first(
                                            SpecificTypeStrategies.DECIMAL_PLUS,
                                            COMMON,
                                            explicit(DataTypes.STRING()))))
                    .build();

    /**
     * Special "+" operator used internally for implementing SUM/AVG aggregations (with and without
     * retractions) on a Decimal type. Uses the {@link
     * LogicalTypeMerging#findSumAggType(LogicalType)} to prevent the normal {@link #PLUS} from
     * overriding the special calculation for precision and scale needed by the aggregate function.
     */
    public static final BuiltInFunctionDefinition AGG_DECIMAL_PLUS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("AGG_DECIMAL_PLUS")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeRoot.DECIMAL),
                                    logical(LogicalTypeRoot.DECIMAL)))
                    .outputTypeStrategy(SpecificTypeStrategies.AGG_DECIMAL_PLUS)
                    .runtimeProvided()
                    .build();

    /**
     * Special "+" operator used internally for implementing native hive SUM/AVG aggregations on a
     * Decimal type. Here is used to prevent the normal {@link #PLUS} from overriding the special
     * calculation for precision and scale needed by the aggregate function. {@link
     * LogicalTypeMerging#findAdditionDecimalType} will add 1 to the precision of the plus result
     * type, but for hive we just keep the precision as input type.
     */
    public static final BuiltInFunctionDefinition HIVE_AGG_DECIMAL_PLUS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("HIVE_AGG_DECIMAL_PLUS")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeRoot.DECIMAL),
                                    logical(LogicalTypeRoot.DECIMAL)))
                    .outputTypeStrategy(SpecificTypeStrategies.HIVE_AGG_DECIMAL_PLUS)
                    .runtimeProvided()
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
                    .outputTypeStrategy(
                            nullableIfArgs(first(SpecificTypeStrategies.DECIMAL_PLUS, COMMON)))
                    .build();

    /**
     * Special "-" operator used internally for implementing SUM/AVG aggregations (with and without
     * retractions) on a Decimal type. Uses the {@link
     * LogicalTypeMerging#findSumAggType(LogicalType)} to prevent the normal {@link #MINUS} from
     * overriding the special calculation for precision and scale needed by the aggregate function.
     */
    public static final BuiltInFunctionDefinition AGG_DECIMAL_MINUS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("AGG_DECIMAL_MINUS")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeRoot.DECIMAL),
                                    logical(LogicalTypeRoot.DECIMAL)))
                    .outputTypeStrategy(SpecificTypeStrategies.AGG_DECIMAL_PLUS)
                    .runtimeProvided()
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
                            nullableIfArgs(
                                    first(
                                            SpecificTypeStrategies.DECIMAL_DIVIDE,
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
                            nullableIfArgs(
                                    first(
                                            SpecificTypeStrategies.DECIMAL_TIMES,
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
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
                    .outputTypeStrategy(
                            nullableIfArgs(
                                    first(SpecificTypeStrategies.DECIMAL_SCALE_0, argument(0))))
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
                    .outputTypeStrategy(
                            nullableIfArgs(
                                    first(SpecificTypeStrategies.DECIMAL_SCALE_0, argument(0))))
                    .build();

    public static final BuiltInFunctionDefinition LOG10 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("log10")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition LOG2 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("log2")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition LN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("ln")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
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
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition POWER =
            BuiltInFunctionDefinition.newBuilder()
                    .name("power")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.NUMERIC),
                                    logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition MOD =
            BuiltInFunctionDefinition.newBuilder()
                    .name("mod")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.EXACT_NUMERIC),
                                    logical(LogicalTypeFamily.EXACT_NUMERIC)))
                    .outputTypeStrategy(
                            nullableIfArgs(first(SpecificTypeStrategies.DECIMAL_MOD, argument(1))))
                    .build();

    public static final BuiltInFunctionDefinition SQRT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sqrt")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
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
                    .outputTypeStrategy(nullableIfArgs(argument(0)))
                    .build();

    public static final BuiltInFunctionDefinition SIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sin")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition COS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cos")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition SINH =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sinh")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition TAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("tan")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition TANH =
            BuiltInFunctionDefinition.newBuilder()
                    .name("tanh")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition COT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cot")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ASIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("asin")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ACOS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("acos")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ATAN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("atan")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition ATAN2 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("atan2")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.NUMERIC),
                                    logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition COSH =
            BuiltInFunctionDefinition.newBuilder()
                    .name("cosh")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition DEGREES =
            BuiltInFunctionDefinition.newBuilder()
                    .name("degrees")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
                    .build();

    public static final BuiltInFunctionDefinition RADIANS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("radians")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.DOUBLE())))
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
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.ROUND))
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
                    .outputTypeStrategy(explicit(INT().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition BIN =
            BuiltInFunctionDefinition.newBuilder()
                    .name("bin")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.INTEGER_NUMERIC)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .build();

    public static final BuiltInFunctionDefinition HEX =
            BuiltInFunctionDefinition.newBuilder()
                    .name("hex")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.INTEGER_NUMERIC)),
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
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
                    .outputTypeStrategy(nullableIfArgs(SpecificTypeStrategies.ROUND))
                    .build();

    // --------------------------------------------------------------------------------------------
    // Catalog functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition CURRENT_DATABASE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentDatabase")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(STRING().notNull()))
                    .notDeterministic()
                    .build();

    // --------------------------------------------------------------------------------------------
    // Time functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition EXTRACT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("extract")
                    .kind(SCALAR)
                    .inputTypeStrategy(SpecificInputTypeStrategies.EXTRACT)
                    .outputTypeStrategy(nullableIfArgs(explicit(BIGINT())))
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_DATE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentDate")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(DATE().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_TIME =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentTime")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(TIME().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition LOCAL_TIME =
            BuiltInFunctionDefinition.newBuilder()
                    .name("localTime")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(TIME().notNull()))
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentTimestamp")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(TIMESTAMP_LTZ(3).notNull()))
                    .build();

    public static final BuiltInFunctionDefinition NOW =
            BuiltInFunctionDefinition.newBuilder()
                    .name("now")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(TIMESTAMP_LTZ(3).notNull()))
                    .build();

    public static final BuiltInFunctionDefinition CURRENT_ROW_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("currentRowTimestamp")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(TIMESTAMP_LTZ(3).notNull()))
                    .notDeterministic()
                    .build();

    public static final BuiltInFunctionDefinition LOCAL_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("localTimestamp")
                    .kind(SCALAR)
                    .outputTypeStrategy(explicit(TIMESTAMP(3).notNull()))
                    .build();

    public static final BuiltInFunctionDefinition TEMPORAL_OVERLAPS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("temporalOverlaps")
                    .kind(SCALAR)
                    .inputTypeStrategy(SpecificInputTypeStrategies.TEMPORAL_OVERLAPS)
                    .outputTypeStrategy(nullableIfArgs(explicit(BOOLEAN())))
                    .build();

    public static final BuiltInFunctionDefinition DATE_FORMAT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("dateFormat")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.TIMESTAMP),
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(STRING())))
                    .build();

    public static final BuiltInFunctionDefinition TIMESTAMP_DIFF =
            BuiltInFunctionDefinition.newBuilder()
                    .name("timestampDiff")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    symbol(
                                            TimePointUnit.YEAR,
                                            TimePointUnit.QUARTER,
                                            TimePointUnit.MONTH,
                                            TimePointUnit.WEEK,
                                            TimePointUnit.DAY,
                                            TimePointUnit.HOUR,
                                            TimePointUnit.MINUTE,
                                            TimePointUnit.SECOND),
                                    logical(LogicalTypeFamily.DATETIME),
                                    logical(LogicalTypeFamily.DATETIME)))
                    .outputTypeStrategy(nullableIfArgs(explicit(INT())))
                    .build();

    public static final BuiltInFunctionDefinition CONVERT_TZ =
            BuiltInFunctionDefinition.newBuilder()
                    .name("convertTz")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(explicit(STRING().nullable()))
                    .build();

    public static final BuiltInFunctionDefinition FROM_UNIXTIME =
            BuiltInFunctionDefinition.newBuilder()
                    .name("fromUnixtime")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.NUMERIC)),
                                    sequence(
                                            logical(LogicalTypeFamily.NUMERIC),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(STRING())))
                    .build();

    public static final BuiltInFunctionDefinition UNIX_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("unixTimestamp")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    NO_ARGS,
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(explicit(BIGINT()))
                    .build();

    public static final BuiltInFunctionDefinition TO_DATE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("toDate")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(DATE())))
                    .build();

    public static final BuiltInFunctionDefinition TO_TIMESTAMP_LTZ =
            BuiltInFunctionDefinition.newBuilder()
                    .name("toTimestampLtz")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.NUMERIC),
                                    logical(LogicalTypeFamily.INTEGER_NUMERIC, false)))
                    .outputTypeStrategy(SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                    .build();

    public static final BuiltInFunctionDefinition TO_TIMESTAMP =
            BuiltInFunctionDefinition.newBuilder()
                    .name("toTimestamp")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(TIMESTAMP(3))))
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
                    .inputTypeStrategy(SpecificInputTypeStrategies.ARRAY)
                    .outputTypeStrategy(SpecificTypeStrategies.ARRAY)
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
                    .inputTypeStrategy(SpecificInputTypeStrategies.MAP)
                    .outputTypeStrategy(SpecificTypeStrategies.MAP)
                    .build();

    public static final BuiltInFunctionDefinition ROW =
            BuiltInFunctionDefinition.newBuilder()
                    .name("row")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            InputTypeStrategies.wildcardWithCount(ConstantArgumentCount.from(1)))
                    .outputTypeStrategy(SpecificTypeStrategies.ROW)
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
                    .outputTypeStrategy(SpecificTypeStrategies.GET)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Crypto hash functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition MD5 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("md5")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.CHAR(32))))
                    .build();

    public static final BuiltInFunctionDefinition SHA1 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha1")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.CHAR(40))))
                    .build();

    public static final BuiltInFunctionDefinition SHA224 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha224")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.CHAR(56))))
                    .build();

    public static final BuiltInFunctionDefinition SHA256 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha256")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.CHAR(64))))
                    .build();

    public static final BuiltInFunctionDefinition SHA384 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha384")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.CHAR(96))))
                    .build();

    public static final BuiltInFunctionDefinition SHA512 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha512")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.CHAR(128))))
                    .build();

    public static final BuiltInFunctionDefinition SHA2 =
            BuiltInFunctionDefinition.newBuilder()
                    .name("sha2")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    logical(LogicalTypeRoot.INTEGER)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.VARCHAR(128))))
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

    public static final BuiltInFunctionDefinition CURRENT_WATERMARK =
            BuiltInFunctionDefinition.newBuilder()
                    .name("CURRENT_WATERMARK")
                    .kind(SCALAR)
                    .inputTypeStrategy(SpecificInputTypeStrategies.CURRENT_WATERMARK)
                    .outputTypeStrategy(SpecificTypeStrategies.CURRENT_WATERMARK)
                    .notDeterministic()
                    .runtimeProvided()
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
    // JSON functions
    // --------------------------------------------------------------------------------------------

    public static final BuiltInFunctionDefinition IS_JSON =
            BuiltInFunctionDefinition.newBuilder()
                    .name("IS_JSON")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            symbol(JsonType.class))))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().notNull()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_EXISTS =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_EXISTS")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            and(
                                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                                    LITERAL)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            and(
                                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                                    LITERAL),
                                            symbol(JsonExistsOnError.class))))
                    .outputTypeStrategy(explicit(DataTypes.BOOLEAN().nullable()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_VALUE =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_VALUE")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    and(logical(LogicalTypeFamily.CHARACTER_STRING), LITERAL),
                                    TYPE_LITERAL,
                                    symbol(JsonValueOnEmptyOrError.class),
                                    ANY,
                                    symbol(JsonValueOnEmptyOrError.class),
                                    ANY))
                    .outputTypeStrategy(forceNullable(argument(2)))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_QUERY =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_QUERY")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    and(logical(LogicalTypeFamily.CHARACTER_STRING), LITERAL),
                                    symbol(JsonQueryWrapper.class),
                                    symbol(JsonQueryOnEmptyOrError.class),
                                    symbol(JsonQueryOnEmptyOrError.class)))
                    .outputTypeStrategy(explicit(DataTypes.STRING().nullable()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_STRING =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_STRING")
                    .kind(SCALAR)
                    .inputTypeStrategy(sequence(JSON_ARGUMENT))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .runtimeProvided()
                    .build();

    public static final BuiltInFunctionDefinition JSON_OBJECT =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_OBJECT")
                    .kind(SCALAR)
                    .inputTypeStrategy(SpecificInputTypeStrategies.JSON_OBJECT)
                    .outputTypeStrategy(explicit(DataTypes.STRING().notNull()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_OBJECTAGG_NULL_ON_NULL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_OBJECTAGG_NULL_ON_NULL")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(
                            sequence(logical(LogicalTypeFamily.CHARACTER_STRING), JSON_ARGUMENT))
                    .outputTypeStrategy(explicit(DataTypes.STRING().notNull()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_OBJECTAGG_ABSENT_ON_NULL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_OBJECTAGG_ABSENT_ON_NULL")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(
                            sequence(logical(LogicalTypeFamily.CHARACTER_STRING), JSON_ARGUMENT))
                    .outputTypeStrategy(explicit(DataTypes.STRING().notNull()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_ARRAY =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_ARRAY")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            InputTypeStrategies.varyingSequence(
                                    symbol(JsonOnNull.class),
                                    SpecificInputTypeStrategies.JSON_ARGUMENT))
                    .outputTypeStrategy(explicit(DataTypes.STRING().notNull()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_ARRAYAGG_NULL_ON_NULL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_ARRAYAGG_NULL_ON_NULL")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(JSON_ARGUMENT))
                    .outputTypeStrategy(explicit(DataTypes.STRING().notNull()))
                    .runtimeDeferred()
                    .build();

    public static final BuiltInFunctionDefinition JSON_ARRAYAGG_ABSENT_ON_NULL =
            BuiltInFunctionDefinition.newBuilder()
                    .name("JSON_ARRAYAGG_ABSENT_ON_NULL")
                    .kind(AGGREGATE)
                    .inputTypeStrategy(sequence(JSON_ARGUMENT))
                    .outputTypeStrategy(explicit(DataTypes.STRING().notNull()))
                    .runtimeDeferred()
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
                    .inputTypeStrategy(SpecificInputTypeStrategies.CAST)
                    .outputTypeStrategy(
                            nullableIfArgs(ConstantArgumentCount.to(0), TypeStrategies.argument(1)))
                    .build();

    public static final BuiltInFunctionDefinition TRY_CAST =
            BuiltInFunctionDefinition.newBuilder()
                    .name("TRY_CAST")
                    .kind(SCALAR)
                    .inputTypeStrategy(SpecificInputTypeStrategies.CAST)
                    .outputTypeStrategy(forceNullable(TypeStrategies.argument(1)))
                    .build();

    public static final BuiltInFunctionDefinition REINTERPRET_CAST =
            BuiltInFunctionDefinition.newBuilder()
                    .name("reinterpretCast")
                    .kind(SCALAR)
                    .inputTypeStrategy(SpecificInputTypeStrategies.REINTERPRET_CAST)
                    .outputTypeStrategy(TypeStrategies.argument(1))
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

    @Internal
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
