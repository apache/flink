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
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.types.Row;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
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
                        .expectNamedArguments("i", "s")
                        .expectTypedArguments(DataTypes.INT(), DataTypes.STRING())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "s"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT()),
                                            InputTypeStrategies.explicit(DataTypes.STRING())
                                        }),
                                TypeStrategies.explicit(DataTypes.BOOLEAN())),

                // function hint defines everything with overloading
                TestSpec.forScalarFunction(FullFunctionHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT())),

                // global output hint with local input overloading
                TestSpec.forScalarFunction(GlobalOutputFunctionHint.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.STRING())),
                                TypeStrategies.explicit(DataTypes.INT())),

                // unsupported output overloading
                TestSpec.forScalarFunction(InvalidSingleOutputFunctionHint.class)
                        .expectErrorMessage(
                                "Function hints that lead to ambiguous results are not allowed."),

                // global and local overloading
                TestSpec.forScalarFunction(SplitFullFunctionHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT())),

                // global and local overloading with unsupported output overloading
                TestSpec.forScalarFunction(InvalidFullOutputFunctionHint.class)
                        .expectErrorMessage(
                                "Function hints with same input definition but different result types are not allowed."),

                // ignore argument names during overloading
                TestSpec.forScalarFunction(InvalidFullOutputFunctionWithArgNamesHint.class)
                        .expectErrorMessage(
                                "Function hints with same input definition but different result types are not allowed."),

                // invalid data type hint
                TestSpec.forScalarFunction(IncompleteFunctionHint.class)
                        .expectErrorMessage(
                                "Data type hint does neither specify a data type nor input group for use as function argument."),

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

                // global input hints and local output hints
                TestSpec.forScalarFunction(GlobalInputFunctionHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.INT())),

                // no arguments
                TestSpec.forScalarFunction(ZeroArgFunction.class)
                        .expectNamedArguments()
                        .expectTypedArguments()
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[0], new ArgumentTypeStrategy[0]),
                                TypeStrategies.explicit(DataTypes.INT())),

                // no arguments async
                TestSpec.forAsyncScalarFunction(ZeroArgFunctionAsync.class)
                        .expectNamedArguments()
                        .expectTypedArguments()
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[0], new ArgumentTypeStrategy[0]),
                                TypeStrategies.explicit(DataTypes.INT())),

                // test primitive arguments extraction
                TestSpec.forScalarFunction(MixedArgFunction.class)
                        .expectNamedArguments("i", "d")
                        .expectTypedArguments(
                                DataTypes.INT().notNull().bridgedTo(int.class), DataTypes.DOUBLE())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "d"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(DataTypes.DOUBLE())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT())),

                // test primitive arguments extraction async
                TestSpec.forAsyncScalarFunction(MixedArgFunctionAsync.class)
                        .expectNamedArguments("i", "d")
                        .expectTypedArguments(
                                DataTypes.INT().notNull().bridgedTo(int.class), DataTypes.DOUBLE())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "d"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(DataTypes.DOUBLE())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT())),

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

                // output hint with input extraction
                TestSpec.forScalarFunction(ExtractWithOutputHintFunction.class)
                        .expectNamedArguments("i")
                        .expectTypedArguments(DataTypes.INT())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT())),

                // output hint with input extraction
                TestSpec.forAsyncScalarFunction(ExtractWithOutputHintFunctionAsync.class)
                        .expectNamedArguments("i")
                        .expectTypedArguments(DataTypes.INT())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT())),

                // output extraction with input hints
                TestSpec.forScalarFunction(ExtractWithInputHintFunction.class)
                        .expectNamedArguments("i", "b")
                        .expectTypedArguments(DataTypes.INT(), DataTypes.BOOLEAN())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "b"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT()),
                                            InputTypeStrategies.explicit(DataTypes.BOOLEAN())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.DOUBLE().notNull().bridgedTo(double.class))),

                // different accumulator depending on input
                TestSpec.forAggregateFunction(InputDependentAccumulatorFunction.class)
                        .expectAccumulatorMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("f", DataTypes.BIGINT()))))
                        .expectAccumulatorMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.STRING())),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("f", DataTypes.STRING()))))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.STRING()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.STRING())),
                                TypeStrategies.explicit(DataTypes.STRING())),

                // input, accumulator, and output are spread across the function
                TestSpec.forAggregateFunction(AggregateFunctionWithManyAnnotations.class)
                        .expectNamedArguments("r")
                        .expectTypedArguments(
                                DataTypes.ROW(
                                        DataTypes.FIELD("i", DataTypes.INT()),
                                        DataTypes.FIELD("b", DataTypes.BOOLEAN())))
                        .expectAccumulatorMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"r"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD("i", DataTypes.INT()),
                                                            DataTypes.FIELD(
                                                                    "b", DataTypes.BOOLEAN())))
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("b", DataTypes.BOOLEAN()))))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"r"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD("i", DataTypes.INT()),
                                                            DataTypes.FIELD(
                                                                    "b", DataTypes.BOOLEAN())))
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),

                // test for table functions
                TestSpec.forTableFunction(OutputHintTableFunction.class)
                        .expectNamedArguments("i")
                        .expectTypedArguments(DataTypes.INT().notNull().bridgedTo(int.class))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class))
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("i", DataTypes.INT()),
                                                DataTypes.FIELD("b", DataTypes.BOOLEAN())))),

                // mismatch between hints and implementation regarding return type
                TestSpec.forScalarFunction(InvalidMethodScalarFunction.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "java.lang.String eval(int[])"),

                // mismatch between hints and implementation regarding return type
                TestSpec.forAsyncScalarFunction(InvalidMethodScalarFunctionAsync.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "eval(java.util.concurrent.CompletableFuture, int[])"),

                // mismatch between hints and implementation regarding accumulator
                TestSpec.forAggregateFunction(InvalidMethodAggregateFunction.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "accumulate(java.lang.Integer, int, boolean)"),

                // no implementation
                TestSpec.forTableFunction(MissingMethodTableFunction.class)
                        .expectErrorMessage(
                                "Could not find a publicly accessible method named 'eval'."),

                // named arguments with overloaded function
                // expected no named argument for overloaded function
                TestSpec.forScalarFunction(NamedArgumentsScalarFunction.class),

                // scalar function that takes any input
                TestSpec.forScalarFunction(InputGroupScalarFunction.class)
                        .expectNamedArguments("o")
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),

                // scalar function that takes any input as vararg
                TestSpec.forScalarFunction(VarArgInputGroupScalarFunction.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),
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
                TestSpec.forScalarFunction(
                                "Scalar function with explicit overloading order by class annotations",
                                OrderedScalarFunction2.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT())),
                TestSpec.forScalarFunction(
                                "Scalar function with explicit overloading order by method annotations",
                                OrderedScalarFunction3.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT())),
                TestSpec.forTableFunction(
                                "A data type hint on the class is used instead of a function output hint",
                                DataTypeHintOnTableFunctionClass.class)
                        .expectNamedArguments()
                        .expectTypedArguments()
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {}, new ArgumentTypeStrategy[] {}),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT())))),
                TestSpec.forTableFunction(
                                "A data type hint on the method is used instead of a function output hint",
                                DataTypeHintOnTableFunctionMethod.class)
                        .expectNamedArguments("i")
                        .expectTypedArguments(DataTypes.INT())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT())))),
                TestSpec.forTableFunction(
                                "Invalid data type hint on top of method and class",
                                InvalidDataTypeHintOnTableFunction.class)
                        .expectErrorMessage(
                                "More than one data type hint found for output of function. "
                                        + "Please use a function hint instead."),
                TestSpec.forScalarFunction(
                                "A data type hint on the method is used for enriching (not a function output hint)",
                                DataTypeHintOnScalarFunction.class)
                        .expectNamedArguments()
                        .expectTypedArguments()
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {}, new ArgumentTypeStrategy[] {}),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()))
                                                .bridgedTo(RowData.class))),
                TestSpec.forAsyncScalarFunction(
                                "A data type hint on the method is used for enriching (not a function output hint)",
                                DataTypeHintOnScalarFunctionAsync.class)
                        .expectNamedArguments()
                        .expectTypedArguments()
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {}, new ArgumentTypeStrategy[] {}),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()))
                                                .bridgedTo(RowData.class))),
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints",
                                ArgumentHintScalarFunction.class)
                        .expectNamedArguments("f1", "f2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"f1", "f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints missing type",
                                ArgumentHintMissingTypeScalarFunction.class)
                        .expectErrorMessage("The type of the argument at position 0 is not set."),
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints all missing name",
                                ArgumentHintMissingNameScalarFunction.class)
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT()),
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints all missing partial name",
                                ArgumentHintMissingPartialNameScalarFunction.class)
                        .expectErrorMessage(
                                "The argument name in function hint must be either fully set or not set at all."),
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints name conflict",
                                ArgumentHintNameConflictScalarFunction.class)
                        .expectErrorMessage(
                                "Argument name conflict, there are at least two argument names that are the same."),
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints on method parameter",
                                ArgumentHintOnParameterScalarFunction.class)
                        .expectNamedArguments("in1", "in2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOptionalArguments(false, false)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"in1", "in2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.STRING())),
                TestSpec.forScalarFunction(
                                "Scalar function with arguments hints and inputs hints both defined",
                                ArgumentsAndInputsScalarFunction.class)
                        .expectErrorMessage(
                                "Argument and input hints cannot be declared in the same function hint."),
                TestSpec.forScalarFunction(
                                "Scalar function with argument hint and dataType hint declared in the same parameter",
                                ArgumentsHintAndDataTypeHintScalarFunction.class)
                        .expectErrorMessage(
                                "Argument and dataType hints cannot be declared in the same parameter at position 0."),
                TestSpec.forScalarFunction(
                                "An invalid scalar function that declare FunctionHint for both class and method in the same class.",
                                InvalidFunctionHintOnClassAndMethod.class)
                        .expectErrorMessage(
                                "Argument and input hints cannot be declared in the same function hint."),
                TestSpec.forScalarFunction(
                                "A valid scalar class that declare FunctionHint for both class and method in the same class.",
                                ValidFunctionHintOnClassAndMethod.class)
                        .expectNamedArguments("f1", "f2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOptionalArguments(true, true),
                TestSpec.forScalarFunction(
                                "The FunctionHint of the function conflicts with the method.",
                                ScalarFunctionWithFunctionHintConflictMethod.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature"),
                // For function with overloaded function, argument name will be empty
                TestSpec.forScalarFunction(
                        "Scalar function with overloaded functions and arguments hint declared.",
                        ArgumentsHintScalarFunctionWithOverloadedFunction.class),
                TestSpec.forScalarFunction(
                                "Scalar function with argument type not null but optional.",
                                ArgumentHintNotNullTypeWithOptionalsScalarFunction.class)
                        .expectErrorMessage(
                                "Argument at position 0 is optional but its type doesn't accept null value."),
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
                                TypeStrategies.explicit(DataTypes.STRING())));
    }

    private static Stream<TestSpec> procedureSpecs() {
        return Stream.of(
                // procedure hint defines everything
                TestSpec.forProcedure(FullProcedureHint.class)
                        .expectNamedArguments("i", "s")
                        .expectTypedArguments(DataTypes.INT(), DataTypes.STRING())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "s"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT()),
                                            InputTypeStrategies.explicit(DataTypes.STRING())
                                        }),
                                TypeStrategies.explicit(DataTypes.BOOLEAN())),
                // procedure hint defines everything
                TestSpec.forProcedure(FullProcedureHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
                // global output hint with local input overloading
                TestSpec.forProcedure(GlobalOutputProcedureHint.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.STRING())),
                                TypeStrategies.explicit(DataTypes.INT())),
                // global and local overloading
                TestSpec.forProcedure(SplitFullProcedureHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT())),
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
                // global input hints and local output hints
                TestSpec.forProcedure(GlobalInputProcedureHints.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.INT())),

                // no arguments
                TestSpec.forProcedure(ZeroArgProcedure.class)
                        .expectNamedArguments()
                        .expectTypedArguments()
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[0], new ArgumentTypeStrategy[0]),
                                TypeStrategies.explicit(DataTypes.INT())),

                // test primitive arguments extraction
                TestSpec.forProcedure(MixedArgProcedure.class)
                        .expectNamedArguments("i", "d")
                        .expectTypedArguments(
                                DataTypes.INT().notNull().bridgedTo(int.class), DataTypes.DOUBLE())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "d"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().notNull().bridgedTo(int.class)),
                                            InputTypeStrategies.explicit(DataTypes.DOUBLE())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT())),

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

                // output hint with input extraction
                TestSpec.forProcedure(ExtractWithOutputHintProcedure.class)
                        .expectNamedArguments("i")
                        .expectTypedArguments(DataTypes.INT())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(DataTypes.INT())),

                // output extraction with input hints
                TestSpec.forProcedure(ExtractWithInputHintProcedure.class)
                        .expectNamedArguments("i", "b")
                        .expectTypedArguments(DataTypes.INT(), DataTypes.BOOLEAN())
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"i", "b"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.INT()),
                                            InputTypeStrategies.explicit(DataTypes.BOOLEAN())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.DOUBLE().notNull().bridgedTo(double.class))),
                // named arguments with overloaded function
                // expected no named argument for overloaded function
                TestSpec.forProcedure(NamedArgumentsProcedure.class),

                // scalar function that takes any input
                TestSpec.forProcedure(InputGroupProcedure.class)
                        .expectNamedArguments("o")
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),

                // scalar function that takes any input as vararg
                TestSpec.forProcedure(VarArgInputGroupProcedure.class)
                        .expectOutputMapping(
                                InputTypeStrategies.varyingSequence(
                                        new String[] {"o"},
                                        new ArgumentTypeStrategy[] {InputTypeStrategies.ANY}),
                                TypeStrategies.explicit(DataTypes.STRING())),
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
                TestSpec.forProcedure(
                                "Procedure with explicit overloading order by class annotations",
                                OrderedProcedure2.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT())),
                TestSpec.forProcedure(
                                "Procedure with explicit overloading order by method annotations",
                                OrderedProcedure3.class)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.BIGINT())),
                                TypeStrategies.explicit(DataTypes.BIGINT()))
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.INT())),
                                TypeStrategies.explicit(DataTypes.INT())),
                TestSpec.forProcedure(
                                "A data type hint on the method is used for enriching (not a function output hint)",
                                DataTypeHintOnProcedure.class)
                        .expectNamedArguments()
                        .expectTypedArguments()
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {}, new ArgumentTypeStrategy[] {}),
                                TypeStrategies.explicit(
                                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()))
                                                .bridgedTo(RowData.class))),
                // unsupported output overloading
                TestSpec.forProcedure(InvalidSingleOutputProcedureHint.class)
                        .expectErrorMessage(
                                "Procedure hints that lead to ambiguous results are not allowed."),
                // global and local overloading with unsupported output overloading
                TestSpec.forProcedure(InvalidFullOutputProcedureHint.class)
                        .expectErrorMessage(
                                "Procedure hints with same input definition but different result types are not allowed."),
                // ignore argument names during overloading
                TestSpec.forProcedure(InvalidFullOutputProcedureWithArgNamesHint.class)
                        .expectErrorMessage(
                                "Procedure hints with same input definition but different result types are not allowed."),
                // invalid data type hint
                TestSpec.forProcedure(IncompleteProcedureHint.class)
                        .expectErrorMessage(
                                "Data type hint does neither specify a data type nor input group for use as function argument."),
                // mismatch between hints and implementation regarding return type
                TestSpec.forProcedure(InvalidMethodProcedure.class)
                        .expectErrorMessage(
                                "Considering all hints, the method should comply with the signature:\n"
                                        + "java.lang.String[] call(_, int[])"),
                // no implementation
                TestSpec.forProcedure(MissingMethodProcedure.class)
                        .expectErrorMessage(
                                "Could not find a publicly accessible method named 'call'."),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on method",
                                ArgumentHintOnMethodProcedure.class)
                        .expectNamedArguments("f1", "f2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOptionalArguments(true, true)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"f1", "f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on class",
                                ArgumentHintOnClassProcedure.class)
                        .expectNamedArguments("f1", "f2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOptionalArguments(true, true)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"f1", "f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on parameter",
                                ArgumentHintOnParameterProcedure.class)
                        .expectNamedArguments("parameter_f1", "parameter_f2")
                        .expectTypedArguments(
                                DataTypes.STRING(), DataTypes.INT().bridgedTo(int.class))
                        .expectOptionalArguments(true, false)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"parameter_f1", "parameter_f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(
                                                    DataTypes.INT().bridgedTo(int.class))
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on method and parameter",
                                ArgumentHintOnMethodAndParameterProcedure.class)
                        .expectNamedArguments("local_f1", "local_f2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOptionalArguments(true, true)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"local_f1", "local_f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on class and method",
                                ArgumentHintOnClassAndMethodProcedure.class)
                        .expectNamedArguments("global_f1", "global_f2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOptionalArguments(false, false)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"global_f1", "global_f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint on class and method and parameter",
                                ArgumentHintOnClassAndMethodAndParameterProcedure.class)
                        .expectNamedArguments("global_f1", "global_f2")
                        .expectTypedArguments(DataTypes.STRING(), DataTypes.INT())
                        .expectOptionalArguments(false, false)
                        .expectOutputMapping(
                                InputTypeStrategies.sequence(
                                        new String[] {"global_f1", "global_f2"},
                                        new ArgumentTypeStrategy[] {
                                            InputTypeStrategies.explicit(DataTypes.STRING()),
                                            InputTypeStrategies.explicit(DataTypes.INT())
                                        }),
                                TypeStrategies.explicit(
                                        DataTypes.INT().notNull().bridgedTo(int.class))),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument hint type not null but optional",
                                ArgumentHintNotNullWithOptionalProcedure.class)
                        .expectErrorMessage(
                                "Argument at position 1 is optional but its type doesn't accept null value."),
                TestSpec.forProcedure(
                                "Named arguments procedure with argument name conflict",
                                ArgumentHintNameConflictProcedure.class)
                        .expectErrorMessage(
                                "Argument name conflict, there are at least two argument names that are the same."),
                TestSpec.forProcedure(
                                "Named arguments procedure with optional type on primitive type",
                                ArgumentHintOptionalOnPrimitiveParameterConflictProcedure.class)
                        .expectErrorMessage(
                                "Argument at position 1 is optional but a primitive type doesn't accept null value."));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testArgumentNames(TestSpec testSpec) {
        if (testSpec.expectedArgumentNames != null) {
            assertThat(testSpec.typeInferenceExtraction.get().getNamedArguments())
                    .isEqualTo(Optional.of(testSpec.expectedArgumentNames));
        } else if (testSpec.expectedErrorMessage == null) {
            assertThat(testSpec.typeInferenceExtraction.get().getNamedArguments())
                    .isEqualTo(Optional.empty());
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testArgumentOptionals(TestSpec testSpec) {
        if (testSpec.expectedArgumentOptionals != null) {
            assertThat(testSpec.typeInferenceExtraction.get().getOptionalArguments())
                    .isEqualTo(Optional.of(testSpec.expectedArgumentOptionals));
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testArgumentTypes(TestSpec testSpec) {
        if (testSpec.expectedArgumentTypes != null) {
            assertThat(testSpec.typeInferenceExtraction.get().getTypedArguments())
                    .isEqualTo(Optional.of(testSpec.expectedArgumentTypes));
        } else if (testSpec.expectedErrorMessage == null) {
            assertThat(testSpec.typeInferenceExtraction.get().getTypedArguments())
                    .isEqualTo(Optional.empty());
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
    void testAccumulatorTypeStrategy(TestSpec testSpec) {
        if (!testSpec.expectedAccumulatorStrategies.isEmpty()) {
            assertThat(
                            testSpec.typeInferenceExtraction
                                    .get()
                                    .getAccumulatorTypeStrategy()
                                    .isPresent())
                    .isEqualTo(true);
            assertThat(testSpec.typeInferenceExtraction.get().getAccumulatorTypeStrategy().get())
                    .isEqualTo(TypeStrategies.mapping(testSpec.expectedAccumulatorStrategies));
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testOutputTypeStrategy(TestSpec testSpec) {
        if (!testSpec.expectedOutputStrategies.isEmpty()) {
            assertThat(testSpec.typeInferenceExtraction.get().getOutputTypeStrategy())
                    .isEqualTo(TypeStrategies.mapping(testSpec.expectedOutputStrategies));
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

        @Nullable List<String> expectedArgumentNames;

        @Nullable List<Boolean> expectedArgumentOptionals;

        @Nullable List<DataType> expectedArgumentTypes;

        Map<InputTypeStrategy, TypeStrategy> expectedAccumulatorStrategies;

        Map<InputTypeStrategy, TypeStrategy> expectedOutputStrategies;

        @Nullable String expectedErrorMessage;

        private TestSpec(String description, Supplier<TypeInference> typeInferenceExtraction) {
            this.description = description;
            this.typeInferenceExtraction = typeInferenceExtraction;
            this.expectedAccumulatorStrategies = new LinkedHashMap<>();
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

        TestSpec expectNamedArguments(String... expectedArgumentNames) {
            this.expectedArgumentNames = Arrays.asList(expectedArgumentNames);
            return this;
        }

        TestSpec expectOptionalArguments(Boolean... expectedArgumentOptionals) {
            this.expectedArgumentOptionals = Arrays.asList(expectedArgumentOptionals);
            return this;
        }

        TestSpec expectTypedArguments(DataType... expectedArgumentTypes) {
            this.expectedArgumentTypes = Arrays.asList(expectedArgumentTypes);
            return this;
        }

        TestSpec expectAccumulatorMapping(
                InputTypeStrategy validator, TypeStrategy accumulatorStrategy) {
            this.expectedAccumulatorStrategies.put(validator, accumulatorStrategy);
            return this;
        }

        TestSpec expectOutputMapping(InputTypeStrategy validator, TypeStrategy outputStrategy) {
            this.expectedOutputStrategies.put(validator, outputStrategy);
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
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2", isOptional = true)
                })
        public int[] call(Object procedureContext, String f1, Integer f2) {
            return null;
        }
    }

    @ProcedureHint(
            argument = {
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
                @ArgumentHint(
                                type = @DataTypeHint("INT"),
                                name = "parameter_f2",
                                isOptional = false)
                        int f2) {
            return null;
        }
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(
                        type = @DataTypeHint("STRING"),
                        name = "global_f1",
                        isOptional = false),
                @ArgumentHint(
                        type = @DataTypeHint("INTEGER"),
                        name = "global_f2",
                        isOptional = false)
            })
    private static class ArgumentHintOnClassAndMethodProcedure implements Procedure {
        @ProcedureHint(
                argument = {
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
                argument = {
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
            argument = {
                @ArgumentHint(
                        type = @DataTypeHint("STRING"),
                        name = "global_f1",
                        isOptional = false),
                @ArgumentHint(
                        type = @DataTypeHint("INTEGER"),
                        name = "global_f2",
                        isOptional = false)
            })
    private static class ArgumentHintOnClassAndMethodAndParameterProcedure implements Procedure {
        @ProcedureHint(
                argument = {
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
                argument = {
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
                argument = {
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
                argument = {
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

    private static class MixedArgFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> f, int i, Double d) {}
    }

    private static class OverloadedFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> f, int i, Double d) {}

        public void eval(CompletableFuture<Long> f, String s) {}
    }

    private static class VarArgFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, int i, int... more) {}
    }

    private static class VarArgWithByteFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, byte... bytes) {}
    }

    @FunctionHint(output = @DataTypeHint("INT"))
    private static class ExtractWithOutputHintFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Object> f, Integer i) {}
    }

    @FunctionHint(output = @DataTypeHint("STRING"))
    private static class InvalidMethodScalarFunctionAsync extends AsyncScalarFunction {
        public void eval(CompletableFuture<Long> f, int[] i) {}
    }

    private static class DataTypeHintOnScalarFunctionAsync extends AsyncScalarFunction {
        public void eval(@DataTypeHint("ROW<i INT>") CompletableFuture<RowData> f) {}
    }

    private static class ArgumentHintScalarFunction extends ScalarFunction {
        @FunctionHint(
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintMissingTypeScalarFunction extends ScalarFunction {
        @FunctionHint(argument = {@ArgumentHint(name = "f1"), @ArgumentHint(name = "f2")})
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintMissingNameScalarFunction extends ScalarFunction {
        @FunctionHint(
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING")),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"))
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintMissingPartialNameScalarFunction extends ScalarFunction {
        @FunctionHint(
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "in1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"))
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    private static class ArgumentHintNameConflictScalarFunction extends ScalarFunction {
        @FunctionHint(
                argument = {
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
                argument = {
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
            argument = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
            })
    private static class InvalidFunctionHintOnClassAndMethod extends ScalarFunction {
        @FunctionHint(
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                },
                input = {@DataTypeHint("STRING"), @DataTypeHint("INTEGER")})
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    @FunctionHint(
            argument = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1", isOptional = true),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2", isOptional = true)
            })
    private static class ValidFunctionHintOnClassAndMethod extends ScalarFunction {
        @FunctionHint(
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                })
        public String eval(String f1, Integer f2) {
            return "";
        }
    }

    @FunctionHint(
            argument = {
                @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
            })
    @FunctionHint(
            argument = {
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
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                })
        public String eval(String f1, Integer f2) {
            return "";
        }

        @FunctionHint(
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f2")
                })
        public String eval(String f1, String f2) {
            return "";
        }
    }

    private static class ArgumentHintNotNullTypeWithOptionalsScalarFunction extends ScalarFunction {
        @FunctionHint(
                argument = {
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
                argument = {
                    @ArgumentHint(type = @DataTypeHint("STRING"), name = "f1"),
                    @ArgumentHint(type = @DataTypeHint("INTEGER"), name = "f2")
                },
                isVarArgs = true)
        public String eval(String f1, Integer... f2) {
            return "";
        }
    }
}
