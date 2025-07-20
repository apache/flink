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

package org.apache.flink.table.types.extraction;

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.types.Row;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TypeInferenceExtractor}. */
@SuppressWarnings("unused")
class TypeInferenceExtractorTest {

    private static Stream<TestSpec> testData() {
        return Stream.concat(functionSpecs(), procedureSpecs());
    }

    private static Stream<TestSpec> functionSpecs() {
        return Stream.of(
                // function hint defines everything
                TestSpec.forScalarFunction(FullFunctionHint.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectStaticArgument(StaticArgument.scalar("s", DataTypes.STRING(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.BOOLEAN())),
                // ---
                // function hint defines everything with overloading
                TestSpec.forScalarFunction(FullFunctionHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // ---
                // global output hint with local input overloading
                TestSpec.forScalarFunction(GlobalOutputFunctionHint.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.STRING()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // unsupported output overloading
                TestSpec.forScalarFunction(InvalidSingleOutputFunctionHint.class)
                        .expectErrorMessage(
                                "Function hints that lead to ambiguous results are not allowed."),
                // ---
                // global and local overloading
                TestSpec.forScalarFunction(SplitFullFunctionHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // ---
                // global and local overloading with unsupported output overloading
                TestSpec.forScalarFunction(InvalidFullOutputFunctionHint.class)
                        .expectErrorMessage(
                                "Function hints with same input definition but different result types are not allowed."),
                // ---
                // ignore argument names during overloading
                TestSpec.forScalarFunction(InvalidFullOutputFunctionWithArgNamesHint.class)
                        .expectErrorMessage(
                                "Function hints with same input definition but different result types are not allowed."),
                // ---
                // invalid data type hint
                TestSpec.forScalarFunction(IncompleteFunctionHint.class)
                        .expectErrorMessage("Data type missing for scalar argument at position 1."),
                // ---
                // varargs and ANY input group
                TestSpec.forScalarFunction(ComplexFunctionHint.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"myInt", "myAny"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.ARRAY(DataTypes.INT())),
                                            InputTypeStrategies.ANY
                                        }),
                                TypeStrategies.explicit(DataTypes.BOOLEAN())),
                // ---
                // global input hints and local output hints
                TestSpec.forScalarFunction(GlobalInputFunctionHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // no arguments
                TestSpec.forScalarFunction(ZeroArgFunction.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // no arguments async
                TestSpec.forAsyncScalarFunction(ZeroArgFunctionAsync.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // no arguments async table
                TestSpec.forAsyncTableFunction(ZeroArgFunctionAsyncTable.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test primitive arguments extraction
                TestSpec.forScalarFunction(MixedArgFunction.class)
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "i", DataTypes.INT().notNull().bridgedTo(int.class), false))
                        .expectStaticArgument(StaticArgument.scalar("d", DataTypes.DOUBLE(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test primitive arguments extraction async
                TestSpec.forAsyncScalarFunction(MixedArgFunctionAsync.class)
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "i", DataTypes.INT().notNull().bridgedTo(int.class), false))
                        .expectStaticArgument(StaticArgument.scalar("d", DataTypes.DOUBLE(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test primitive arguments extraction async table
                TestSpec.forAsyncTableFunction(MixedArgFunctionAsyncTable.class)
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "i", DataTypes.INT().notNull().bridgedTo(int.class), false))
                        .expectStaticArgument(StaticArgument.scalar("d", DataTypes.DOUBLE(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test overloaded arguments extraction
                TestSpec.forScalarFunction(OverloadedFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "d"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(DataTypes.DOUBLE())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"s"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.BIGINT().notNull().bridgedTo(long.class))),
                // ---
                // test overloaded arguments extraction async
                TestSpec.forAsyncScalarFunction(OverloadedFunctionAsync.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "d"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(DataTypes.DOUBLE())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"s"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING())
                                        }),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // ---
                // test overloaded arguments extraction async
                TestSpec.forAsyncTableFunction(OverloadedFunctionAsyncTable.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "d"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(DataTypes.DOUBLE())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"s"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test varying arguments extraction
                TestSpec.forScalarFunction(VarArgFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"i", "more"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // test varying arguments extraction async
                TestSpec.forAsyncScalarFunction(VarArgFunctionAsync.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"i", "more"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // test varying arguments extraction async table
                TestSpec.forAsyncTableFunction(VarArgFunctionAsyncTable.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"i", "more"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // test varying arguments extraction with byte
                TestSpec.forScalarFunction(VarArgWithByteFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"bytes"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.TINYINT()
                                                            .notNull()
                                                            .bridgedTo(byte.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // test varying arguments extraction with byte async
                TestSpec.forAsyncScalarFunction(VarArgWithByteFunctionAsync.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"bytes"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.TINYINT()
                                                            .notNull()
                                                            .bridgedTo(byte.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // test varying arguments extraction with byte async
                TestSpec.forAsyncTableFunction(VarArgWithByteFunctionAsyncTable.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"bytes"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.TINYINT()
                                                            .notNull()
                                                            .bridgedTo(byte.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // output hint with input extraction
                TestSpec.forScalarFunction(ExtractWithOutputHintFunction.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // output hint with input extraction
                TestSpec.forAsyncScalarFunction(ExtractWithOutputHintFunctionAsync.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // output hint with input extraction
                TestSpec.forAsyncTableFunction(ExtractWithOutputHintFunctionAsyncTable.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // output extraction with input hints
                TestSpec.forScalarFunction(ExtractWithInputHintFunction.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectStaticArgument(
                                StaticArgument.scalar("b", DataTypes.BOOLEAN(), false))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.DOUBLE().notNull().bridgedTo(double.class))),
                // ---
                // non-scalar args
                TestSpec.forScalarFunction(TableArgScalarFunction.class)
                        .expectErrorMessage(
                                "Only scalar arguments are supported at this location. "
                                        + "But argument 't' declared the following traits: [ROW_SEMANTIC_TABLE]"),
                // ---
                // different accumulator depending on input
                TestSpec.forAggregateFunction(InputDependentAccumulatorFunction.class)
                        .expectAccumulator(
                                TypeStrategies.mapping(
                                        Map.of(
                                                InputTypeStrategies.sequence(
                                                        List.of("arg0"),
                                                        List.of(
                                                                InputTypeStrategies.explicit(
                                                                        DataTypes.BIGINT()))),
                                                TypeStrategies.explicit(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "f", DataTypes.BIGINT()))),
                                                InputTypeStrategies.sequence(
                                                        List.of("arg0"),
                                                        List.of(
                                                                InputTypeStrategies.explicit(
                                                                        DataTypes.STRING()))),
                                                TypeStrategies.explicit(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "f",
                                                                        DataTypes.STRING()))))))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.STRING()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.STRING()))),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // input, accumulator, and output are spread across the function
                TestSpec.forAggregateFunction(AggregateFunctionWithManyAnnotations.class)
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "r",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("i", DataTypes.INT()),
                                                DataTypes.FIELD("b", DataTypes.BOOLEAN())),
                                        false))
                        .expectAccumulator(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("b", DataTypes.BOOLEAN()))))
                        .expectOutput(TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // accumulator with state hint
                TestSpec.forAggregateFunction(StateHintAggregateFunction.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectState("myAcc", TypeStrategies.explicit(MyState.TYPE))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // accumulator with state hint in function hint
                TestSpec.forAggregateFunction(StateHintInFunctionHintAggregateFunction.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectState("myAcc", TypeStrategies.explicit(MyState.TYPE))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test for table functions
                TestSpec.forTableFunction(OutputHintTableFunction.class)
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "i", DataTypes.INT().notNull().bridgedTo(int.class), false))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("i", DataTypes.INT()),
                                                DataTypes.FIELD("b", DataTypes.BOOLEAN())))),
                // ---
                // mismatch between hints and implementation regarding return type
                TestSpec.forScalarFunction(InvalidMethodScalarFunction.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "java.lang.String eval(int[])"),
                // ---
                // mismatch between hints and implementation regarding return type
                TestSpec.forAsyncScalarFunction(InvalidMethodScalarFunctionAsync.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "eval(java.util.concurrent.CompletableFuture, int[])"),
                // ---
                // mismatch between hints and implementation regarding return type
                TestSpec.forAsyncTableFunction(InvalidMethodTableFunctionAsync.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "eval(java.util.concurrent.CompletableFuture, int[])"),
                // ---
                TestSpec.forAsyncTableFunction(InvalidMethodTableFunctionMissingCollection.class)
                        .expectErrorMessage(
                                "The method 'eval' expects nested generic type CompletableFuture<Collection> for the 0 arg."),
                // ---
                TestSpec.forAsyncTableFunction(InvalidMethodTableFunctionWrongGeneric.class)
                        .expectErrorMessage(
                                "The method 'eval' expects nested generic type CompletableFuture<Collection> for the 0 arg."),
                // ---
                TestSpec.forAsyncTableFunction(ConflictingReturnTypesAsyncTable.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "eval(java.util.concurrent.CompletableFuture, int)"),
                // ---
                // mismatch between hints and implementation regarding accumulator
                TestSpec.forAggregateFunction(InvalidMethodAggregateFunction.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "accumulate(java.lang.Integer, int, boolean)\n"
                                        + "Pattern: (<accumulator> [, <argument>]*)"),
                // ---
                // no implementation
                TestSpec.forTableFunction(MissingMethodTableFunction.class)
                        .expectErrorMessage(
                                "Could not find a publicly accessible method named 'eval'."),
                // ---
                // named arguments with overloaded function
                // expected no named argument for overloaded function
                TestSpec.forScalarFunction(NamedArgumentsScalarFunction.class),
                // ---
                // scalar function that takes any input
                TestSpec.forScalarFunction(InputGroupScalarFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // scalar function that takes any input as vararg
                TestSpec.forScalarFunction(VarArgInputGroupScalarFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with implicit overloading order",
                                OrderedScalarFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"l"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.BIGINT())
                                        }),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with explicit overloading order by class annotations",
                                OrderedScalarFunction2.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with explicit overloading order by method annotations",
                                OrderedScalarFunction3.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forTableFunction(
                                "A data type hint on the class is used instead of a function output hint",
                                DataTypeHintOnTableFunctionClass.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT())))),
                // ---
                TestSpec.forTableFunction(
                                "A data type hint on the method is used instead of a function output hint",
                                DataTypeHintOnTableFunctionMethod.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT())))),
                // ---
                TestSpec.forTableFunction(
                                "Invalid data type hint on top of method and class",
                                InvalidDataTypeHintOnTableFunction.class)
                        .expectErrorMessage(
                                "More than one data type hint found for output of function. "
                                        + "Please use a function hint instead."),
                // ---
                TestSpec.forScalarFunction(
                                "A data type hint on the method is used for enriching (not a function output hint)",
                                DataTypeHintOnScalarFunction.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()))
                                                .bridgedTo(RowData.class))),
                // ---
                TestSpec.forAsyncScalarFunction(
                                "A data type hint on the method is used for enriching (not a function output hint)",
                                DataTypeHintOnScalarFunctionAsync.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()))
                                                .bridgedTo(RowData.class))),
                // ---
                TestSpec.forAsyncTableFunction(
                                "A data type hint on the method is used for enriching (not a function output hint)",
                                DataTypeHintOnTableFunctionAsync.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()))
                                                .bridgedTo(RowData.class))),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints",
                                ArgumentHintScalarFunction.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("f1", DataTypes.STRING(), false))
                        .expectStaticArgument(StaticArgument.scalar("f2", DataTypes.INT(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints missing type",
                                ArgumentHintMissingTypeScalarFunction.class)
                        .expectErrorMessage("Data type missing for scalar argument at position 0."),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints all missing name",
                                ArgumentHintMissingNameScalarFunction.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("arg0", DataTypes.STRING(), false))
                        .expectStaticArgument(StaticArgument.scalar("arg1", DataTypes.INT(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints all missing partial name",
                                ArgumentHintMissingPartialNameScalarFunction.class)
                        .expectErrorMessage(
                                "Argument names in function hint must be either fully set or not set at all."),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints name conflict",
                                ArgumentHintNameConflictScalarFunction.class)
                        .expectErrorMessage(
                                "Argument name conflict, there are at least two argument names that are the same."),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints on method parameter",
                                ArgumentHintOnParameterScalarFunction.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("in1", DataTypes.STRING(), false))
                        .expectStaticArgument(StaticArgument.scalar("in2", DataTypes.INT(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints and inputs hints both defined",
                                ArgumentsAndInputsScalarFunction.class)
                        .expectErrorMessage(
                                "Argument and input hints cannot be declared in the same function hint."),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with argument hint and data type hint declared in the same parameter",
                                ArgumentsHintAndDataTypeHintScalarFunction.class)
                        .expectErrorMessage(
                                "Argument and data type hints cannot be declared at the same time at position 0."),
                // ---
                TestSpec.forScalarFunction(
                                "An invalid scalar function that declare FunctionHint for both class and method in the same class.",
                                InvalidFunctionHintOnClassAndMethod.class)
                        .expectErrorMessage(
                                "Argument and input hints cannot be declared in the same function hint."),
                // ---
                TestSpec.forScalarFunction(
                                "A valid scalar class that declare FunctionHint for both class and method in the same class.",
                                ValidFunctionHintOnClassAndMethod.class)
                        .expectStaticArgument(StaticArgument.scalar("f1", DataTypes.STRING(), true))
                        .expectStaticArgument(StaticArgument.scalar("f2", DataTypes.INT(), true)),
                // ---
                TestSpec.forScalarFunction(
                                "The FunctionHint of the function conflicts with the method.",
                                ScalarFunctionWithFunctionHintConflictMethod.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature"),
                // ---
                // For function with overloaded function, argument name will be empty
                TestSpec.forScalarFunction(
                        "Scalar function with overloaded functions and arguments hint declared.",
                        ArgumentsHintScalarFunctionWithOverloadedFunction.class),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with argument type not null but optional.",
                                ArgumentHintNotNullTypeWithOptionalsScalarFunction.class)
                        .expectErrorMessage(
                                "Argument at position 0 is optional but its type doesn't accept null value."),
                // ---
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hint and variable length args",
                                ArgumentHintVariableLengthScalarFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"f1", "f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                TestSpec.forProcessTableFunction(StatelessProcessTableFunction.class)
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "i", DataTypes.INT().notNull().bridgedTo(int.class), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcessTableFunction(StateProcessTableFunction.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectState("s", TypeStrategies.explicit(MyState.TYPE))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcessTableFunction(NamedStateProcessTableFunction.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("myArg", DataTypes.INT(), false))
                        .expectState("myState", TypeStrategies.explicit(MyState.TYPE))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcessTableFunction(MultiStateProcessTableFunction.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectState(
                                "s1",
                                TypeStrategies.explicit(MyFirstState.TYPE),
                                Duration.ofDays(2))
                        .expectState(
                                "s2", TypeStrategies.explicit(MySecondState.TYPE), Duration.ZERO)
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcessTableFunction(UntypedTableArgProcessTableFunction.class)
                        .expectStaticArgument(
                                StaticArgument.table(
                                        "t",
                                        Row.class,
                                        false,
                                        EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcessTableFunction(TypedTableArgProcessTableFunction.class)
                        .expectStaticArgument(
                                StaticArgument.table(
                                        "t",
                                        TypedTableArgProcessTableFunction.Customer.TYPE,
                                        false,
                                        EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcessTableFunction(ComplexProcessTableFunction.class)
                        .expectStaticArgument(
                                StaticArgument.table(
                                        "setTable",
                                        RowData.class,
                                        false,
                                        EnumSet.of(
                                                StaticArgumentTrait.SET_SEMANTIC_TABLE,
                                                StaticArgumentTrait.OPTIONAL_PARTITION_BY)))
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectStaticArgument(
                                StaticArgument.table(
                                        "rowTable",
                                        Row.class,
                                        false,
                                        EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)))
                        .expectStaticArgument(StaticArgument.scalar("s", DataTypes.STRING(), true))
                        .expectState("s1", TypeStrategies.explicit(MyFirstState.TYPE))
                        .expectState(
                                "other",
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("f", DataTypes.FLOAT()))))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("b", DataTypes.BOOLEAN())))),
                // ---
                TestSpec.forProcessTableFunction(ComplexProcessTableFunctionWithFunctionHint.class)
                        .expectStaticArgument(
                                StaticArgument.table(
                                        "setTable",
                                        RowData.class,
                                        false,
                                        EnumSet.of(
                                                StaticArgumentTrait.SET_SEMANTIC_TABLE,
                                                StaticArgumentTrait.OPTIONAL_PARTITION_BY)))
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectStaticArgument(
                                StaticArgument.table(
                                        "rowTable",
                                        Row.class,
                                        false,
                                        EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)))
                        .expectStaticArgument(StaticArgument.scalar("s", DataTypes.STRING(), true))
                        .expectState("s1", TypeStrategies.explicit(MyFirstState.TYPE))
                        .expectState(
                                "other",
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("f", DataTypes.FLOAT()))))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("b", DataTypes.BOOLEAN())))),
                // ---
                TestSpec.forProcessTableFunction(WrongStateOrderProcessTableFunction.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "eval(org.apache.flink.table.types.extraction.TypeInferenceExtractorTest.MyFirstState, int)\n"
                                        + "Pattern: (<context>? [, <state>]* [, <argument>]*)"),
                // ---
                TestSpec.forProcessTableFunction(MissingStateTypeProcessTableFunction.class)
                        .expectErrorMessage(
                                "Could not extract a data type from 'class java.lang.Object' in parameter 0 of method 'eval'"),
                // ---
                TestSpec.forProcessTableFunction(EnrichedExtractionStateProcessTableFunction.class)
                        .expectState(
                                "u",
                                TypeStrategies.explicit(
                                        EnrichedExtractionStateProcessTableFunction.User.TYPE))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcessTableFunction(
                                MissingDefaultConstructorStateProcessTableFunction.class)
                        .expectErrorMessage(
                                "State entries must provide an argument-less constructor so that all fields are mutable."),
                // ---
                TestSpec.forProcessTableFunction(NonCompositeStateProcessTableFunction.class)
                        .expectErrorMessage(
                                "State entries must use a mutable, composite data type. But was: INT"),
                // ---
                TestSpec.forProcessTableFunction(WrongTypedTableProcessTableFunction.class)
                        .expectErrorMessage(
                                "Invalid data type 'INT' for table argument 'i'. "
                                        + "Typed table arguments must use a composite type (i.e. row or structured type)."),
                // ---
                TestSpec.forProcessTableFunction(WrongArgumentTraitsProcessTableFunction.class)
                        .expectErrorMessage(
                                "Invalid argument traits for argument 'r'. "
                                        + "Trait OPTIONAL_PARTITION_BY requires SET_SEMANTIC_TABLE."),
                // ---
                TestSpec.forProcessTableFunction(
                                MixingStaticAndInputGroupProcessTableFunction.class)
                        .expectErrorMessage(
                                "Process table functions require a non-overloaded, non-vararg, and static signature."),
                // ---
                TestSpec.forProcessTableFunction(MultiEvalProcessTableFunction.class)
                        .expectErrorMessage(
                                "Process table functions require a non-overloaded, non-vararg, and static signature."));
    }

    private static Stream<TestSpec> procedureSpecs() {
        return Stream.of(
                // procedure hint defines everything
                TestSpec.forProcedure(FullProcedureHint.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectStaticArgument(StaticArgument.scalar("s", DataTypes.STRING(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.BOOLEAN())),
                // ---
                // procedure hints define everything
                TestSpec.forProcedure(FullProcedureHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // ---
                // global output hint with local input overloading
                TestSpec.forProcedure(GlobalOutputProcedureHint.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.STRING()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // global and local overloading
                TestSpec.forProcedure(SplitFullProcedureHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // ---
                // varargs and ANY input group
                TestSpec.forProcedure(ComplexProcedureHint.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"myInt", "myAny"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.ARRAY(DataTypes.INT())),
                                            InputTypeStrategies.ANY
                                        }),
                                TypeStrategies.explicit(DataTypes.BOOLEAN())),
                // ---
                // global input hints and local output hints
                TestSpec.forProcedure(GlobalInputProcedureHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // no arguments
                TestSpec.forProcedure(ZeroArgProcedure.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test primitive arguments extraction
                TestSpec.forProcedure(MixedArgProcedure.class)
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "i", DataTypes.INT().notNull().bridgedTo(int.class), false))
                        .expectStaticArgument(StaticArgument.scalar("d", DataTypes.DOUBLE(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // test overloaded arguments extraction
                TestSpec.forProcedure(OverloadedProcedure.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "d"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(DataTypes.DOUBLE())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"s"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.BIGINT().notNull().bridgedTo(long.class))),
                // ---
                // test varying arguments extraction
                TestSpec.forProcedure(VarArgProcedure.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"i", "more"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // test varying arguments extraction with byte
                TestSpec.forProcedure(VarArgWithByteProcedure.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"bytes"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.TINYINT()
                                                            .notNull()
                                                            .bridgedTo(byte.class))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // output hint with input extraction
                TestSpec.forProcedure(ExtractWithOutputHintProcedure.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectOutput(TypeStrategies.explicit(DataTypes.INT())),
                // ---
                // output extraction with input hints
                TestSpec.forProcedure(ExtractWithInputHintProcedure.class)
                        .expectStaticArgument(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .expectStaticArgument(
                                StaticArgument.scalar("b", DataTypes.BOOLEAN(), false))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.DOUBLE().notNull().bridgedTo(double.class))),
                // ---
                // named arguments with overloaded function
                // expected no named argument for overloaded function
                TestSpec.forProcedure(NamedArgumentsProcedure.class),
                // ---
                // procedure function that takes any input
                TestSpec.forProcedure(InputGroupProcedure.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                // procedure function that takes any input as vararg
                TestSpec.forProcedure(VarArgInputGroupProcedure.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),
                // ---
                TestSpec.forProcedure(
                                "Procedure with implicit overloading order", OrderedProcedure.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"l"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.BIGINT())
                                        }),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // ---
                TestSpec.forProcedure(
                                "Procedure with explicit overloading order by class annotations",
                                OrderedProcedure2.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcedure(
                                "Procedure with explicit overloading order by method annotations",
                                OrderedProcedure3.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.BIGINT()))),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        List.of("arg0"),
                                        List.of(InputTypeStrategies.explicit(DataTypes.INT()))),
                                TypeStrategies.explicit(DataTypes.INT())),
                // ---
                TestSpec.forProcedure(
                                "A data type hint on the method is used for enriching (not a function output hint)",
                                DataTypeHintOnProcedure.class)
                        .expectEmptyStaticArguments()
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()))
                                                .bridgedTo(RowData.class))),
                // ---
                // unsupported output overloading
                TestSpec.forProcedure(InvalidSingleOutputProcedureHint.class)
                        .expectErrorMessage(
                                "Procedure hints that lead to ambiguous results are not allowed."),
                // ---
                // global and local overloading with unsupported output overloading
                TestSpec.forProcedure(InvalidFullOutputProcedureHint.class)
                        .expectErrorMessage(
                                "Procedure hints with same input definition but different result types are not allowed."),
                // ---
                // ignore argument names during overloading
                TestSpec.forProcedure(InvalidFullOutputProcedureWithArgNamesHint.class)
                        .expectErrorMessage(
                                "Procedure hints with same input definition but different result types are not allowed."),
                // ---
                // invalid data type hint
                TestSpec.forProcedure(IncompleteProcedureHint.class)
                        .expectErrorMessage("Data type missing for scalar argument at position 1."),
                // ---
                // mismatch between hints and implementation regarding return type
                TestSpec.forProcedure(InvalidMethodProcedure.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "java.lang.String[] call(_, int[])\n"
                                        + "Pattern: (<context> [, <argument>]*)"),
                // ---
                // no implementation
                TestSpec.forProcedure(MissingMethodProcedure.class)
                        .expectErrorMessage(
                                "Could not find a publicly accessible method named 'call'."),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on method",
                                ArgumentHintOnMethodProcedure.class)
                        .expectStaticArgument(StaticArgument.scalar("f1", DataTypes.STRING(), true))
                        .expectStaticArgument(StaticArgument.scalar("f2", DataTypes.INT(), true))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on class",
                                ArgumentHintOnClassProcedure.class)
                        .expectStaticArgument(StaticArgument.scalar("f1", DataTypes.STRING(), true))
                        .expectStaticArgument(StaticArgument.scalar("f2", DataTypes.INT(), true))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on parameter",
                                ArgumentHintOnParameterProcedure.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("parameter_f1", DataTypes.STRING(), true))
                        .expectStaticArgument(
                                StaticArgument.scalar(
                                        "parameter_f2",
                                        DataTypes.INT().bridgedTo(int.class),
                                        false))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on method and parameter",
                                ArgumentHintOnMethodAndParameterProcedure.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("local_f1", DataTypes.STRING(), true))
                        .expectStaticArgument(
                                StaticArgument.scalar("local_f2", DataTypes.INT(), true))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on class and method",
                                ArgumentHintOnClassAndMethodProcedure.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("global_f1", DataTypes.STRING(), false))
                        .expectStaticArgument(
                                StaticArgument.scalar("global_f2", DataTypes.INT(), false))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on class and method and parameter",
                                ArgumentHintOnClassAndMethodAndParameterProcedure.class)
                        .expectStaticArgument(
                                StaticArgument.scalar("global_f1", DataTypes.STRING(), false))
                        .expectStaticArgument(
                                StaticArgument.scalar("global_f2", DataTypes.INT(), false))
                        .expectOutput(
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint type not null but optional",
                                ArgumentHintNotNullWithOptionalProcedure.class)
                        .expectErrorMessage(
                                "Argument at position 1 is optional but its type doesn't accept null value."),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with argument name conflict",
                                ArgumentHintNameConflictProcedure.class)
                        .expectErrorMessage(
                                "Argument name conflict, there are at least two argument names that are the same."),
                // ---
                TestSpec.forProcedure(
                                "Named arguments procedure with optional type on primitive type",
                                ArgumentHintOptionalOnPrimitiveParameterConflictProcedure.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "int[] call(_, java.lang.String, java.lang.Integer)"));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testStaticArguments(TestSpec testSpec) {
        if (testSpec.expectedStaticArguments != null) {
            final Optional<List<StaticArgument>> staticArguments =
                    testSpec.typeInferenceExtraction.get().getStaticArguments();
            assertThat(staticArguments).isPresent();
            assertThat(staticArguments.get())
                    .containsExactlyElementsOf(testSpec.expectedStaticArguments);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testInputTypeStrategy(TestSpec testSpec) {
        if (!testSpec.expectedOutputStrategies.isEmpty()) {
            assertThat(testSpec.typeInferenceExtraction.get().getInputTypeStrategy())
                    .isEqualTo(
                            testSpec.expectedOutputStrategies.keySet().stream()
                                    .reduce(InputTypeStrategies::or)
                                    .orElseThrow(AssertionError::new));
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testStateTypeStrategies(TestSpec testSpec) {
        if (!testSpec.expectedStateStrategies.isEmpty()) {
            assertThat(testSpec.typeInferenceExtraction.get().getStateTypeStrategies())
                    .isNotEmpty();
            assertThat(testSpec.typeInferenceExtraction.get().getStateTypeStrategies())
                    .isEqualTo(testSpec.expectedStateStrategies);
        } else if (testSpec.expectedErrorMessage == null) {
            assertThat(testSpec.typeInferenceExtraction.get().getStateTypeStrategies()).isEmpty();
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testOutputTypeStrategy(TestSpec testSpec) {
        if (!testSpec.expectedOutputStrategies.isEmpty()) {
            if (testSpec.expectedOutputStrategies.size() == 1) {
                assertThat(testSpec.typeInferenceExtraction.get().getOutputTypeStrategy())
                        .isEqualTo(testSpec.expectedOutputStrategies.values().iterator().next());
            } else {
                assertThat(testSpec.typeInferenceExtraction.get().getOutputTypeStrategy())
                        .isEqualTo(TypeStrategies.mapping(testSpec.expectedOutputStrategies));
            }
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testErrorMessage(TestSpec testSpec) {
        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(testSpec.typeInferenceExtraction::get)
                    .isInstanceOf(ValidationException.class)
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(
                                    ValidationException.class, testSpec.expectedErrorMessage));
        } else {
            testSpec.typeInferenceExtraction.get();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    /** Test specification shared with the Scala tests. */
    static class TestSpec {

        private final String description;

        final Supplier<TypeInference> typeInferenceExtraction;

        @Nullable List<StaticArgument> expectedStaticArguments;

        LinkedHashMap<String, StateTypeStrategy> expectedStateStrategies;

        Map<InputTypeStrategy, TypeStrategy> expectedOutputStrategies;

        @Nullable String expectedErrorMessage;

        private TestSpec(String description, Supplier<TypeInference> typeInferenceExtraction) {
            this.description = description;
            this.typeInferenceExtraction = typeInferenceExtraction;
            this.expectedStateStrategies = new LinkedHashMap<>();
            this.expectedOutputStrategies = new LinkedHashMap<>();
        }

        static TestSpec forScalarFunction(Class<? extends ScalarFunction> function) {
            return forScalarFunction(null, function);
        }

        static TestSpec forScalarFunction(
                String description, Class<? extends ScalarFunction> function) {
            return new TestSpec(
                    description == null ? function.getSimpleName() : description,
                    () ->
                            TypeInferenceExtractor.forScalarFunction(
                                    new DataTypeFactoryMock(), function));
        }

        static TestSpec forAsyncScalarFunction(Class<? extends AsyncScalarFunction> function) {
            return forAsyncScalarFunction(null, function);
        }

        static TestSpec forAsyncScalarFunction(
                String description, Class<? extends AsyncScalarFunction> function) {
            return new TestSpec(
                    description == null ? function.getSimpleName() : description,
                    () ->
                            TypeInferenceExtractor.forAsyncScalarFunction(
                                    new DataTypeFactoryMock(), function));
        }

        @SuppressWarnings("rawtypes")
        static TestSpec forAsyncTableFunction(Class<? extends AsyncTableFunction<?>> function) {
            return forAsyncTableFunction(null, function);
        }

        @SuppressWarnings("rawtypes")
        static TestSpec forAsyncTableFunction(
                String description, Class<? extends AsyncTableFunction<?>> function) {
            return new TestSpec(
                    description == null ? function.getSimpleName() : description,
                    () ->
                            TypeInferenceExtractor.forAsyncTableFunction(
                                    new DataTypeFactoryMock(), function));
        }

        static TestSpec forAggregateFunction(Class<? extends AggregateFunction<?, ?>> function) {
            return new TestSpec(
                    function.getSimpleName(),
                    () ->
                            TypeInferenceExtractor.forAggregateFunction(
                                    new DataTypeFactoryMock(), function));
        }

        static TestSpec forTableFunction(Class<? extends TableFunction<?>> function) {
            return forTableFunction(null, function);
        }

        static TestSpec forTableFunction(
                String description, Class<? extends TableFunction<?>> function) {
            return new TestSpec(
                    description == null ? function.getSimpleName() : description,
                    () ->
                            TypeInferenceExtractor.forTableFunction(
                                    new DataTypeFactoryMock(), function));
        }

        static TestSpec forTableAggregateFunction(
                Class<? extends TableAggregateFunction<?, ?>> function) {
            return new TestSpec(
                    function.getSimpleName(),
                    () ->
                            TypeInferenceExtractor.forTableAggregateFunction(
                                    new DataTypeFactoryMock(), function));
        }

        static TestSpec forProcessTableFunction(Class<? extends ProcessTableFunction<?>> function) {
            return new TestSpec(
                    function.getSimpleName(),
                    () ->
                            TypeInferenceExtractor.forProcessTableFunction(
                                    new DataTypeFactoryMock(), function));
        }

        static TestSpec forProcedure(Class<? extends Procedure> procedure) {
            return forProcedure(null, procedure);
        }

        static TestSpec forProcedure(
                @Nullable String description, Class<? extends Procedure> procedure) {
            return new TestSpec(
                    description == null ? procedure.getSimpleName() : description,
                    () ->
                            TypeInferenceExtractor.forProcedure(
                                    new DataTypeFactoryMock(), procedure));
        }

        TestSpec expectEmptyStaticArguments() {
            this.expectedStaticArguments = new ArrayList<>();
            return this;
        }

        TestSpec expectStaticArgument(StaticArgument argument) {
            if (this.expectedStaticArguments == null) {
                this.expectedStaticArguments = new ArrayList<>();
            }
            this.expectedStaticArguments.add(argument);
            return this;
        }

        TestSpec expectAccumulator(TypeStrategy typeStrategy) {
            expectState("acc", typeStrategy);
            return this;
        }

        TestSpec expectState(String name, TypeStrategy typeStrategy) {
            return expectState(name, typeStrategy, null);
        }

        TestSpec expectState(String name, TypeStrategy typeStrategy, @Nullable Duration ttl) {
            this.expectedStateStrategies.put(name, StateTypeStrategy.of(typeStrategy, ttl));
            return this;
        }

        TestSpec expectOutputMapping(InputTypeStrategy validator, TypeStrategy outputStrategy) {
            this.expectedOutputStrategies.put(validator, outputStrategy);
            return this;
        }

        TestSpec expectOutput(TypeStrategy outputStrategy) {
            this.expectedOutputStrategies.put(InputTypeStrategies.WILDCARD, outputStrategy);
            return this;
        }

        TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test classes for extraction
    // --------------------------------------------------------------------------------------------

    @FunctionHint(
            input = {@DataTypeHint("INT"), @DataTypeHint("STRING")},
            argumentNames = {"i", "s"},
            output = @DataTypeHint("BOOLEAN"))
    private static class FullFunctionHint extends ScalarFunction {
        public Boolean eval(Integer i, String s) {
            return null;
        }
    }

    private static class ComplexFunctionHint extends ScalarFunction {
        @FunctionHint(
                input = {@DataTypeHint("ARRAY<INT>"), @DataTypeHint(inputGroup = InputGroup.ANY)},
                argumentNames = {"myInt", "myAny"},
                output = @DataTypeHint("BOOLEAN"),
                isVarArgs = true)
        public Boolean eval(Object... o) {
            return null;
        }
    }

    @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
    private static class FullFunctionHints extends ScalarFunction {
        public Number eval(Number n) {
            return null;
        }
    }

    @FunctionHint(output = @DataTypeHint("INT"))
    private static class GlobalOutputFunctionHint extends ScalarFunction {
        @FunctionHint(input = @DataTypeHint("INT"))
        public Integer eval(Integer n) {
            return null;
        }

        @FunctionHint(input = @DataTypeHint("STRING"))
        public Integer eval(String n) {
            return null;
        }
    }

    @FunctionHint(output = @DataTypeHint("INT"))
    private static class InvalidSingleOutputFunctionHint extends ScalarFunction {
        @FunctionHint(output = @DataTypeHint("TINYINT"))
        public Integer eval(Number n) {
            return null;
        }
    }

    @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    private static class SplitFullFunctionHints extends ScalarFunction {
        @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
        public Number eval(Number n) {
            return null;
        }
    }

    @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    private static class InvalidFullOutputFunctionHint extends ScalarFunction {
        @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("BIGINT"))
        public Number eval(Integer i) {
            return null;
        }
    }

    @FunctionHint(input = @DataTypeHint("INT"), argumentNames = "a", output = @DataTypeHint("INT"))
    private static class InvalidFullOutputFunctionWithArgNamesHint extends ScalarFunction {
        @FunctionHint(
                input = @DataTypeHint("INT"),
                argumentNames = "b",
                output = @DataTypeHint("BIGINT"))
        public Number eval(Integer i) {
            return null;
        }
    }

    @FunctionHint(input = @DataTypeHint("INT"))
    private static class InvalidLocalOutputFunctionHint extends ScalarFunction {
        @FunctionHint(output = @DataTypeHint("INT"))
        public Integer eval(Integer n) {
            return null;
        }

        @FunctionHint(output = @DataTypeHint("STRING"))
        public Integer eval(String n) {
            return null;
        }
    }

    @FunctionHint(
            input = {@DataTypeHint("INT"), @DataTypeHint()},
            output = @DataTypeHint("BOOLEAN"))
    private static class IncompleteFunctionHint extends ScalarFunction {
        public Boolean eval(Integer i1, Integer i2) {
            return null;
        }
    }

    @FunctionHint(input = @DataTypeHint("INT"))
    @FunctionHint(input = @DataTypeHint("BIGINT"))
    private static class GlobalInputFunctionHints extends ScalarFunction {
        @FunctionHint(output = @DataTypeHint("INT"))
        public Integer eval(Number n) {
            return null;
        }
    }

    private static class ZeroArgFunction extends ScalarFunction {
        public Integer eval() {
            return null;
        }
    }

    private static class MixedArgFunction extends ScalarFunction {
        public Integer eval(int i, Double d) {
            return null;
        }
    }

    private static class OverloadedFunction extends ScalarFunction {
        public Integer eval(int i, Double d) {
            return null;
        }

        public long eval(String s) {
            return 0L;
        }
    }

    private static class VarArgFunction extends ScalarFunction {
        public String eval(int i, int... more) {
            return null;
        }
    }

    private static class VarArgWithByteFunction extends ScalarFunction {
        public String eval(byte... bytes) {
            return null;
        }
    }

    @FunctionHint(output = @DataTypeHint("INT"))
    private static class ExtractWithOutputHintFunction extends ScalarFunction {
        public Object eval(Integer i) {
            return null;
        }
    }

    @FunctionHint(
            input = {@DataTypeHint("INT"), @DataTypeHint("BOOLEAN")},
            argumentNames = {"i", "b"})
    private static class ExtractWithInputHintFunction extends ScalarFunction {
        public double eval(Object... o) {
            return 0.0;
        }
    }

    @FunctionHint(input = @DataTypeHint("BIGINT"), accumulator = @DataTypeHint("ROW<f BIGINT>"))
    @FunctionHint(input = @DataTypeHint("STRING"), accumulator = @DataTypeHint("ROW<f STRING>"))
    private static class InputDependentAccumulatorFunction extends AggregateFunction<String, Row> {

        public void accumulate(Row accumulator, Object o) {
            // nothing to do
        }

        @Override
        public String getValue(Row accumulator) {
            return null;
        }

        @Override
        public Row createAccumulator() {
            return null;
        }
    }

    @FunctionHint(output = @DataTypeHint("STRING"))
    private static class AggregateFunctionWithManyAnnotations
            extends AggregateFunction<String, Row> {
        @FunctionHint(accumulator = @DataTypeHint("ROW<b BOOLEAN>"))
        public void accumulate(Row accumulator, @DataTypeHint("ROW<i INT, b BOOLEAN>") Row r) {
            // nothing to do
        }

        @Override
        public String getValue(Row accumulator) {
            return null;
        }

        @Override
        public Row createAccumulator() {
            return null;
        }
    }

    private static class StateHintAggregateFunction extends AggregateFunction<Integer, MyState> {
        public void accumulate(
                @StateHint(name = "myAcc") MyState acc, @ArgumentHint(name = "i") Integer i) {}

        @Override
        public Integer getValue(MyState accumulator) {
            return null;
        }

        @Override
        public MyState createAccumulator() {
            return new MyState();
        }
    }

    @FunctionHint(
            state = {@StateHint(name = "myAcc", type = @DataTypeHint(bridgedTo = MyState.class))},
            arguments = {@ArgumentHint(name = "i", type = @DataTypeHint("INT"))})
    private static class StateHintInFunctionHintAggregateFunction
            extends AggregateFunction<Integer, Object> {
        public void accumulate(Object acc, Integer i) {}

        @Override
        public Integer getValue(Object accumulator) {
            return null;
        }

        @Override
        public Object createAccumulator() {
            return new Object();
        }
    }

    @FunctionHint(output = @DataTypeHint("ROW<i INT, b BOOLEAN>"))
    private static class OutputHintTableFunction extends TableFunction<Row> {
        public void eval(int i) {
            // nothing to do
        }
    }

    @FunctionHint(output = @DataTypeHint("STRING"))
    private static class InvalidMethodScalarFunction extends ScalarFunction {
        public Long eval(int[] i) {
            return null;
        }
    }

    @FunctionHint(accumulator = @DataTypeHint("INT"))
    private static class InvalidMethodAggregateFunction extends AggregateFunction<String, Boolean> {

        public void accumulate(Boolean acc, int a, boolean b) {
            // nothing to do
        }

        @Override
        public String getValue(Boolean accumulator) {
            return null;
        }

        @Override
        public Boolean createAccumulator() {
            return null;
        }
    }

    private static class MissingMethodTableFunction extends TableFunction<String> {
        // nothing to do
    }

    private static class NamedArgumentsScalarFunction extends ScalarFunction {
        public Integer eval(int n) {
            return null;
        }

        public Integer eval(long n) {
            return null;
        }

        public Integer eval(@DataTypeHint("DECIMAL(10, 2)") Object n) {
            return null;
        }
    }

    private static class InputGroupScalarFunction extends ScalarFunction {
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.toString();
        }
    }

    private static class VarArgInputGroupScalarFunction extends ScalarFunction {
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... o) {
            return Arrays.toString(o);
        }
    }

    // extracted order is f(INT) || f(BIGINT) due to method signature sorting
    private static class OrderedScalarFunction extends ScalarFunction {
        public Long eval(Long l) {
            return l;
        }

        public Integer eval(Integer i) {
            return i;
        }
    }

    // extracted order is f(BIGINT) || f(INT)
    @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
    @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    private static class OrderedScalarFunction2 extends ScalarFunction {
        public Number eval(Number n) {
            return n;
        }
    }

    // extracted order is f(BIGINT) || f(INT)
    private static class OrderedScalarFunction3 extends ScalarFunction {
        @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
        @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
        public Number eval(Number n) {
            return n;
        }
    }

    @DataTypeHint("ROW<i INT>")
    private static class DataTypeHintOnTableFunctionClass extends TableFunction<Row> {
        public void eval() {
            // nothing to do
        }
    }

    private static class DataTypeHintOnTableFunctionMethod extends TableFunction<Row> {
        @DataTypeHint("ROW<i INT>")
        public void eval(Integer i) {
            // nothing to do
        }
    }

    @DataTypeHint("ROW<i BOOLEAN>")
    private static class InvalidDataTypeHintOnTableFunction extends TableFunction<Row> {
        @DataTypeHint("ROW<i INT>")
        public void eval(Integer i) {
            // nothing to do
        }
    }

    private static class DataTypeHintOnScalarFunction extends ScalarFunction {
        public @DataTypeHint("ROW<i INT>") RowData eval() {
            return null;
        }
    }

    @ProcedureHint(
            input = {@DataTypeHint("INT"), @DataTypeHint("STRING")},
            argumentNames = {"i", "s"},
            output = @DataTypeHint("BOOLEAN"))
    private static class FullProcedureHint implements Procedure {
        public Boolean[] call(Object procedureContext, Integer i, String s) {
            return null;
        }
    }

    private static class ComplexProcedureHint implements Procedure {
        @ProcedureHint(
                input = {@DataTypeHint("ARRAY<INT>"), @DataTypeHint(inputGroup = InputGroup.ANY)},
                argumentNames = {"myInt", "myAny"},
                output = @DataTypeHint("BOOLEAN"),
                isVarArgs = true)
        public Boolean[] call(Object procedureContext, Object... o) {
            return null;
        }
    }

    @ProcedureHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    @ProcedureHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
    private static class FullProcedureHints implements Procedure {
        public Number[] call(Object procedureContext, Number n) {
            return null;
        }
    }

    @ProcedureHint(output = @DataTypeHint("INT"))
    private static class GlobalOutputProcedureHint implements Procedure {
        @ProcedureHint(input = @DataTypeHint("INT"))
        public Integer[] call(Object procedureContext, Integer n) {
            return null;
        }

        @ProcedureHint(input = @DataTypeHint("STRING"))
        public Integer[] call(Object procedureContext, String n) {
            return null;
        }
    }

    @ProcedureHint(output = @DataTypeHint("INT"))
    private static class InvalidSingleOutputProcedureHint implements Procedure {
        @ProcedureHint(output = @DataTypeHint("TINYINT"))
        public Integer call(Object procedureContext, Number n) {
            return null;
        }
    }

    @ProcedureHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    private static class SplitFullProcedureHints implements Procedure {
        @ProcedureHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
        public Number[] call(Object procedureContext, Number n) {
            return null;
        }
    }

    @ProcedureHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    private static class InvalidFullOutputProcedureHint implements Procedure {
        @ProcedureHint(input = @DataTypeHint("INT"), output = @DataTypeHint("BIGINT"))
        public Number[] call(Object procedureContext, Integer i) {
            return null;
        }
    }

    @ProcedureHint(input = @DataTypeHint("INT"), argumentNames = "a", output = @DataTypeHint("INT"))
    private static class InvalidFullOutputProcedureWithArgNamesHint implements Procedure {
        @ProcedureHint(
                input = @DataTypeHint("INT"),
                argumentNames = "b",
                output = @DataTypeHint("BIGINT"))
        public Number[] call(Object procedureContext, Integer i) {
            return null;
        }
    }

    @ProcedureHint(input = @DataTypeHint("INT"))
    private static class InvalidLocalOutputProcedureHint implements Procedure {
        @ProcedureHint(output = @DataTypeHint("INT"))
        public Integer[] call(Object procedureContext, Integer n) {
            return null;
        }

        @ProcedureHint(output = @DataTypeHint("STRING"))
        public Integer[] call(Object procedureContext, String n) {
            return null;
        }
    }

    @ProcedureHint(
            input = {@DataTypeHint("INT"), @DataTypeHint()},
            output = @DataTypeHint("BOOLEAN"))
    private static class IncompleteProcedureHint implements Procedure {
        public Boolean[] call(Object procedureContext, Integer i1, Integer i2) {
            return null;
        }
    }

    @ProcedureHint(input = @DataTypeHint("INT"))
    @ProcedureHint(input = @DataTypeHint("BIGINT"))
    private static class GlobalInputProcedureHints implements Procedure {
        @ProcedureHint(output = @DataTypeHint("INT"))
        public Integer[] call(Object procedureContext, Number n) {
            return null;
        }
    }

    private static class ZeroArgProcedure implements Procedure {
        public Integer[] call(Object procedureContext) {
            return null;
        }
    }

    private static class MixedArgProcedure implements Procedure {
        public Integer[] call(Object procedureContext, int i, Double d) {
            return null;
        }
    }

    private static class OverloadedProcedure implements Procedure {
        public Integer[] call(Object procedureContext, int i, Double d) {
            return null;
        }

        public long[] call(Object procedureContext, String s) {
            return null;
        }
    }

    private static class VarArgProcedure implements Procedure {
        public String[] call(Object procedureContext, int i, int... more) {
            return null;
        }
    }

    private static class VarArgWithByteProcedure implements Procedure {
        public String[] call(Object procedureContext, byte... bytes) {
            return null;
        }
    }

    @ProcedureHint(output = @DataTypeHint("INT"))
    private static class ExtractWithOutputHintProcedure implements Procedure {
        public Object[] call(Object procedureContext, Integer i) {
            return null;
        }
    }

    @ProcedureHint(
            input = {@DataTypeHint("INT"), @DataTypeHint("BOOLEAN")},
            argumentNames = {"i", "b"})
    private static class ExtractWithInputHintProcedure implements Procedure {
        public double[] call(Object procedureContext, Object... o) {
            return new double[] {0.0};
        }
    }

    @ProcedureHint(output = @DataTypeHint("STRING"))
    private static class InvalidMethodProcedure implements Procedure {
        public Long[] call(Object procedureContext, int[] i) {
            return null;
        }
    }

    private static class NamedArgumentsProcedure implements Procedure {
        public Integer[] call(Object procedureContext, int n) {
            return null;
        }

        public Integer[] call(Object procedureContext, long n) {
            return null;
        }

        public Integer[] call(Object procedureContext, @DataTypeHint("DECIMAL(10, 2)") Object n) {
            return null;
        }
    }

    private static class InputGroupProcedure implements Procedure {
        public String[] call(
                Object procedureContext, @DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return null;
        }
    }

    private static class VarArgInputGroupProcedure implements Procedure {
        public String[] call(
                Object procedureContext, @DataTypeHint(inputGroup = InputGroup.ANY) Object... o) {
            return null;
        }
    }

    // extracted order is f(INT) || f(BIGINT) due to method signature sorting
    private static class OrderedProcedure implements Procedure {
        public Long[] call(Object procedureContext, Long l) {
            return null;
        }

        public Integer[] call(Object procedureContext, Integer i) {
            return null;
        }
    }

    // extracted order is f(BIGINT) || f(INT)
    @ProcedureHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
    @ProcedureHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    private static class OrderedProcedure2 implements Procedure {
        public Number[] call(Object procedureContext, Number n) {
            return null;
        }
    }

    // extracted order is f(BIGINT) || f(INT)
    private static class OrderedProcedure3 implements Procedure {
        @ProcedureHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
        @ProcedureHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
        public Number[] call(Object procedureContext, Number n) {
            return null;
        }
    }

    private static class DataTypeHintOnProcedure implements Procedure {
        public @DataTypeHint("ROW<i INT>") RowData[] call(Object procedureContext) {
            return null;
        }
    }

    private static class MissingMethodProcedure implements Procedure {
        public int[] call1(Object procedureContext) {
            return null;
        }
    }

    private static class ArgumentHintOnMethodProcedure implements Procedure {
        @ProcedureHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2", isOptional = true)
                })
        public int[] call(Object procedureContext, String f1, Integer f2) {
            return null;
        }
    }

    @ProcedureHint(
            arguments = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2", isOptional = true)
            })
    private static class ArgumentHintOnClassProcedure implements Procedure {
        public int[] call(Object procedureContext, String f1, Integer f2) {
            return null;
        }
    }

    private static class ArgumentHintOnParameterProcedure implements Procedure {
        public int[] call(
                Object procedureContext,
                @ArgumentHint(
                                type = @DataTypeHint("STRING"),
                                name = "parameter_f1",
                                isOptional = true)
                        String f1,
                @ArgumentHint(type = @DataTypeHint("INT"), name = "parameter_f2") int f2) {
            return null;
        }
    }

    @ProcedureHint(
            arguments = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "global_f1"),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "global_f2")
            })
    private static class ArgumentHintOnClassAndMethodProcedure implements Procedure {
        @ProcedureHint(
                arguments = {
                    @ArgumentHint(
                            type = @DataTypeHint("STRING"),
                            name = "local_f1",
                            isOptional = true),
                    @ArgumentHint(
                            type = @DataTypeHint("INTEGER"),
                            name = "local_f2",
                            isOptional = true)
                })
        public int[] call(Object procedureContext, String f1, Integer f2) {
            return null;
        }
    }

    private static class ArgumentHintOnMethodAndParameterProcedure implements Procedure {
        @ProcedureHint(
                arguments = {
                    @ArgumentHint(
                            type = @DataTypeHint("STRING"),
                            name = "local_f1",
                            isOptional = true),
                    @ArgumentHint(
                            type = @DataTypeHint("INTEGER"),
                            name = "local_f2",
                            isOptional = true)
                })
        public int[] call(
                Object procedureContext,
                @ArgumentHint(
                                type = @DataTypeHint("INTEGER"),
                                name = "parameter_f1",
                                isOptional = true)
                        String f1,
                @ArgumentHint(
                                type = @DataTypeHint("INTEGER"),
                                name = "parameter_f2",
                                isOptional = false)
                        Integer f2) {
            return null;
        }
    }

    @ProcedureHint(
            arguments = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "global_f1"),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "global_f2")
            })
    private static class ArgumentHintOnClassAndMethodAndParameterProcedure implements Procedure {
        @ProcedureHint(
                arguments = {
                    @ArgumentHint(
                            type = @DataTypeHint("STRING"),
                            name = "local_f1",
                            isOptional = true),
                    @ArgumentHint(
                            type = @DataTypeHint("INTEGER"),
                            name = "local_f2",
                            isOptional = true)
                })
        public int[] call(
                Object procedureContext,
                @ArgumentHint(
                                type = @DataTypeHint("STRING"),
                                name = "parameter_f1",
                                isOptional = false)
                        String f1,
                Integer f2) {
            return null;
        }
    }

    private static class ArgumentHintNotNullWithOptionalProcedure implements Procedure {
        @ProcedureHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                    @ArgumentHint(
                            type = @DataTypeHint("INTEGER NOT NULL"),
                            name = "f2",
                            isOptional = true)
                })
        public int[] call(Object procedureContext, String f1, Integer f2) {
            return null;
        }
    }

    private static class ArgumentHintNameConflictProcedure implements Procedure {
        @ProcedureHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                    @ArgumentHint(
                            type = @DataTypeHint("INTEGER NOT NULL"),
                            name = "f1",
                            isOptional = true)
                })
        public int[] call(Object procedureContext, String f1, Integer f2) {
            return null;
        }
    }

    private static class ArgumentHintOptionalOnPrimitiveParameterConflictProcedure
            implements Procedure {
        @ProcedureHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2", isOptional = true)
                })
        public int[] call(Object procedureContext, String f1, int f2) {
            return null;
        }
    }

    private static class ZeroArgFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> f) {}
    }

    private static class ZeroArgFunctionAsyncTable extends AsyncTableFunction<Integer> {
        public void eval(CompletableFuture<Collection<Integer>> f) {}
    }

    private static class MixedArgFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> f, int i, Double d) {}
    }

    private static class MixedArgFunctionAsyncTable extends AsyncTableFunction<Integer> {
        public void eval(CompletableFuture<Collection<Integer>> f, int i, Double d) {}
    }

    private static class OverloadedFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> f, int i, Double d) {}

        public void eval(CompletableFuture<Long> f, String s) {}
    }

    private static class OverloadedFunctionAsyncTable extends AsyncTableFunction<Integer> {
        public void eval(CompletableFuture<Collection<Integer>> f, int i, Double d) {}

        public void eval(CompletableFuture<Collection<Integer>> f, String s) {}
    }

    private static class ConflictingReturnTypesAsyncTable extends AsyncTableFunction<Integer> {
        public void eval(CompletableFuture<Collection<Long>> f, int i) {}
    }

    private static class VarArgFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, int i, int... more) {}
    }

    private static class VarArgFunctionAsyncTable extends AsyncTableFunction<String> {
        public void eval(CompletableFuture<Collection<String>> f, int i, int... more) {}
    }

    private static class VarArgWithByteFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, byte... bytes) {}
    }

    private static class VarArgWithByteFunctionAsyncTable extends AsyncTableFunction<String> {
        public void eval(CompletableFuture<Collection<String>> f, byte... bytes) {}
    }

    @FunctionHint(output = @DataTypeHint("INT"))
    private static class ExtractWithOutputHintFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Object> f, Integer i) {}
    }

    @FunctionHint(output = @DataTypeHint("INT"))
    private static class ExtractWithOutputHintFunctionAsyncTable
            extends AsyncTableFunction<Object> {
        public void eval(CompletableFuture<Collection<Object>> f, Integer i) {}
    }

    @FunctionHint(output = @DataTypeHint("STRING"))
    private static class InvalidMethodScalarFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Long> f, int[] i) {}
    }

    @FunctionHint(output = @DataTypeHint("STRING"))
    private static class InvalidMethodTableFunctionAsync extends AsyncTableFunction<Long> {
        public void eval(CompletableFuture<Collection<Long>> f, int[] i) {}
    }

    private static class InvalidMethodTableFunctionMissingCollection
            extends AsyncTableFunction<Long> {
        public void eval(CompletableFuture<Long> f, int[] i) {}
    }

    private static class InvalidMethodTableFunctionWrongGeneric extends AsyncTableFunction<Long> {
        public void eval(CompletableFuture<Optional<Long>> f, int[] i) {}
    }

    private static class DataTypeHintOnScalarFunctionAsync extends AsyncScalarFunction {
        public void eval(@DataTypeHint("ROW<i INT>") CompletableFuture<RowData> f) {}
    }

    private static class DataTypeHintOnTableFunctionAsync extends AsyncTableFunction<String> {
        @DataTypeHint(value = "ROW<i INT>", bridgedTo = RowData.class)
        public void eval(CompletableFuture<Collection<RowData>> f) {}
    }

    private static class ArgumentHintScalarFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintMissingTypeScalarFunction extends ScalarFunction {
        @FunctionHint(arguments = {@ArgumentHint(name = "f1"), @ArgumentHint(name = "f2")})
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintMissingNameScalarFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING")),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"))
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintMissingPartialNameScalarFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "in1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"))
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintNameConflictScalarFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(name = "in1", type = @DataTypeHint("STRING")),
                    @ArgumentHint(name = "in1", type = @DataTypeHint("INTEGER"))
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintOnParameterScalarFunction extends ScalarFunction {
        public String eval(
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "in1") String f1,
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "in2") Integer f2) {
            return "";
        }
    }

    private static class ArgumentsAndInputsScalarFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                },
                input = {@DataTypeHint("STRING"), @DataTypeHint("INTEGER")})
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentsHintAndDataTypeHintScalarFunction extends ScalarFunction {

        public String eval(
                @DataTypeHint("STRING") @ArgumentHint(name = "f1", type = @DataTypeHint("STRING"))
                        String f1,
                @ArgumentHint(name = "f2", type = @DataTypeHint("INTEGER")) Integer f2) {
            return "";
        }
    }

    @FunctionHint(
            arguments = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
            })
    private static class InvalidFunctionHintOnClassAndMethod extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                },
                input = {@DataTypeHint("STRING"), @DataTypeHint("INTEGER")})
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    @FunctionHint(
            arguments = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2", isOptional = true)
            })
    private static class ValidFunctionHintOnClassAndMethod extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    @FunctionHint(
            arguments = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
            })
    @FunctionHint(
            arguments = {
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f1"),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
            })
    private static class ScalarFunctionWithFunctionHintConflictMethod extends ScalarFunction {
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentsHintScalarFunctionWithOverloadedFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                })
        public String eval(String f1, Integer f2) {
            return "";
        }

        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f2")
                })
        public String eval(String f1, String f2) {
            return "";
        }
    }

    private static class ArgumentHintNotNullTypeWithOptionalsScalarFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(
                            type = @DataTypeHint("STRING NOT NULL"),
                            name = "f1",
                            isOptional = true),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2", isOptional = true)
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintVariableLengthScalarFunction extends ScalarFunction {
        @FunctionHint(
                arguments = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                },
                isVarArgs = true)
        public String eval(String f1, Integer... f2) {
            return "";
        }
    }

    private static class StatelessProcessTableFunction extends ProcessTableFunction<Integer> {
        public void eval(int i) {}
    }

    public static class MyState {
        static final DataType TYPE =
                DataTypes.STRUCTURED(
                        MyState.class,
                        DataTypes.FIELD("d", DataTypes.DOUBLE().notNull().bridgedTo(double.class)));
        public double d;
    }

    public static class MyFirstState {
        static final DataType TYPE =
                DataTypes.STRUCTURED(MyFirstState.class, DataTypes.FIELD("d", DataTypes.DOUBLE()));
        public Double d;
    }

    public static class MySecondState {
        static final DataType TYPE =
                DataTypes.STRUCTURED(MySecondState.class, DataTypes.FIELD("i", DataTypes.INT()));
        public Integer i;
    }

    private static class StateProcessTableFunction extends ProcessTableFunction<Integer> {
        public void eval(@StateHint MyState s, Integer i) {}
    }

    private static class NamedStateProcessTableFunction extends ProcessTableFunction<Integer> {
        public void eval(
                @StateHint(name = "myState") MyState s, @ArgumentHint(name = "myArg") Integer i) {}
    }

    private static class MultiStateProcessTableFunction extends ProcessTableFunction<Integer> {
        public void eval(
                @StateHint(ttl = "2 days") MyFirstState s1,
                @StateHint(ttl = "0") MySecondState s2,
                Integer i) {}
    }

    private static class UntypedTableArgProcessTableFunction extends ProcessTableFunction<Integer> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t) {}
    }

    private static class TypedTableArgProcessTableFunction extends ProcessTableFunction<Integer> {
        public static class Customer {
            static final DataType TYPE =
                    DataTypes.STRUCTURED(
                            Customer.class,
                            DataTypes.FIELD("age", DataTypes.INT()),
                            DataTypes.FIELD("name", DataTypes.STRING()));
            public String name;
            public Integer age;
        }

        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Customer t) {}
    }

    @DataTypeHint("ROW<b BOOLEAN>")
    private static class ComplexProcessTableFunction extends ProcessTableFunction<Row> {
        public void eval(
                Context context,
                @StateHint(name = "s1") MyFirstState s1,
                @StateHint(name = "other", type = @DataTypeHint("ROW<f FLOAT>")) Row s2,
                @ArgumentHint(
                                value = {
                                    ArgumentTrait.SET_SEMANTIC_TABLE,
                                    ArgumentTrait.OPTIONAL_PARTITION_BY
                                },
                                name = "setTable")
                        RowData t1,
                @ArgumentHint(name = "i") Integer i,
                @ArgumentHint(
                                value = {ArgumentTrait.ROW_SEMANTIC_TABLE},
                                name = "rowTable")
                        Row t2,
                @ArgumentHint(isOptional = true, name = "s") String s) {}
    }

    @FunctionHint(
            state = {
                @StateHint(name = "s1", type = @DataTypeHint(bridgedTo = MyFirstState.class)),
                @StateHint(name = "other", type = @DataTypeHint("ROW<f FLOAT>"))
            },
            arguments = {
                @ArgumentHint(
                        name = "setTable",
                        value = {
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        },
                        type = @DataTypeHint(bridgedTo = RowData.class)),
                @ArgumentHint(name = "i", type = @DataTypeHint("INT")),
                @ArgumentHint(
                        name = "rowTable",
                        value = {ArgumentTrait.ROW_SEMANTIC_TABLE}),
                @ArgumentHint(name = "s", isOptional = true, type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint("ROW<b BOOLEAN>"))
    private static class ComplexProcessTableFunctionWithFunctionHint
            extends ProcessTableFunction<Row> {

        public void eval(
                Context context,
                MyFirstState arg0,
                Row arg1,
                RowData arg2,
                Integer arg3,
                Row arg4,
                String arg5) {}
    }

    private static class WrongStateOrderProcessTableFunction extends ProcessTableFunction<Integer> {

        public void eval(int i, @StateHint MyFirstState state) {}
    }

    private static class MissingStateTypeProcessTableFunction
            extends ProcessTableFunction<Integer> {

        public void eval(@StateHint Object state) {}
    }

    private static class EnrichedExtractionStateProcessTableFunction
            extends ProcessTableFunction<Integer> {

        public static class User {
            static final DataType TYPE =
                    DataTypes.STRUCTURED(
                            EnrichedExtractionStateProcessTableFunction.User.class,
                            DataTypes.FIELD("score", DataTypes.DECIMAL(3, 2)));
            public BigDecimal score;
        }

        public void eval(
                @StateHint(
                                type =
                                        @DataTypeHint(
                                                defaultDecimalPrecision = 3,
                                                defaultDecimalScale = 2))
                        User u) {}
    }

    private static class WrongTypedTableProcessTableFunction extends ProcessTableFunction<Integer> {
        public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Integer i) {}
    }

    private static class WrongArgumentTraitsProcessTableFunction
            extends ProcessTableFunction<Integer> {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.ROW_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row r) {}
    }

    private static class MixingStaticAndInputGroupProcessTableFunction
            extends ProcessTableFunction<Integer> {
        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row r,
                @DataTypeHint(inputGroup = InputGroup.ANY) Object o) {}
    }

    private static class InvalidInputGroupTableArgProcessTableFunction
            extends ProcessTableFunction<Integer> {
        public void eval(
                @ArgumentHint(
                                value = ArgumentTrait.ROW_SEMANTIC_TABLE,
                                type = @DataTypeHint(inputGroup = InputGroup.ANY))
                        Row r) {}
    }

    private static class MultiEvalProcessTableFunction extends ProcessTableFunction<Integer> {
        public void eval(int i) {}

        public void eval(String i) {}
    }

    private static class MissingDefaultConstructorStateProcessTableFunction
            extends ProcessTableFunction<Integer> {

        public static class Score {

            public final int value;

            public Score(int value) {
                this.value = value;
            }
        }

        public int eval(@StateHint Score s, @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t) {
            return 0;
        }
    }

    private static class NonCompositeStateProcessTableFunction
            extends ProcessTableFunction<Integer> {

        public int eval(
                @StateHint Integer i, @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t) {
            return 0;
        }
    }

    private static class TableArgScalarFunction extends ScalarFunction {
        public int eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t) {
            return 0;
        }
    }
}
