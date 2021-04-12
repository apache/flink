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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.types.Row;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for {@link TypeInferenceExtractor}. */
@RunWith(Parameterized.class)
@SuppressWarnings("unused")
public class TypeInferenceExtractorTest {

    @Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
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
                TestSpec.forScalarFunction(NamedArgumentsScalarFunction.class)
                        .expectNamedArguments("n"),

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
                                                .bridgedTo(RowData.class))));
    }

    @Parameter public TestSpec testSpec;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testArgumentNames() {
        if (testSpec.expectedArgumentNames != null) {
            assertThat(
                    testSpec.typeInferenceExtraction.get().getNamedArguments(),
                    equalTo(Optional.of(testSpec.expectedArgumentNames)));
        } else if (testSpec.expectedErrorMessage == null) {
            assertThat(
                    testSpec.typeInferenceExtraction.get().getNamedArguments(),
                    equalTo(Optional.empty()));
        }
    }

    @Test
    public void testArgumentTypes() {
        if (testSpec.expectedArgumentTypes != null) {
            assertThat(
                    testSpec.typeInferenceExtraction.get().getTypedArguments(),
                    equalTo(Optional.of(testSpec.expectedArgumentTypes)));
        } else if (testSpec.expectedErrorMessage == null) {
            assertThat(
                    testSpec.typeInferenceExtraction.get().getTypedArguments(),
                    equalTo(Optional.empty()));
        }
    }

    @Test
    public void testInputTypeStrategy() {
        if (!testSpec.expectedOutputStrategies.isEmpty()) {
            assertThat(
                    testSpec.typeInferenceExtraction.get().getInputTypeStrategy(),
                    equalTo(
                            testSpec.expectedOutputStrategies.keySet().stream()
                                    .reduce(InputTypeStrategies::or)
                                    .orElseThrow(AssertionError::new)));
        }
    }

    @Test
    public void testAccumulatorTypeStrategy() {
        if (!testSpec.expectedAccumulatorStrategies.isEmpty()) {
            assertThat(
                    testSpec.typeInferenceExtraction.get().getAccumulatorTypeStrategy().isPresent(),
                    equalTo(true));
            assertThat(
                    testSpec.typeInferenceExtraction.get().getAccumulatorTypeStrategy().get(),
                    equalTo(TypeStrategies.mapping(testSpec.expectedAccumulatorStrategies)));
        }
    }

    @Test
    public void testOutputTypeStrategy() {
        if (!testSpec.expectedOutputStrategies.isEmpty()) {
            assertThat(
                    testSpec.typeInferenceExtraction.get().getOutputTypeStrategy(),
                    equalTo(TypeStrategies.mapping(testSpec.expectedOutputStrategies)));
        }
    }

    @Test
    public void testErrorMessage() {
        if (testSpec.expectedErrorMessage != null) {
            thrown.expect(ValidationException.class);
            thrown.expectCause(errorMatcher(testSpec));
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

        static TestSpec forAggregateFunction(Class<? extends AggregateFunction<?, ?>> function) {
            return forAggregateFunction(null, function);
        }

        static TestSpec forAggregateFunction(
                String description, Class<? extends AggregateFunction<?, ?>> function) {
            return new TestSpec(
                    description == null ? function.getSimpleName() : description,
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
            return forTableAggregateFunction(null, function);
        }

        static TestSpec forTableAggregateFunction(
                String description, Class<? extends TableAggregateFunction<?, ?>> function) {
            return new TestSpec(
                    description == null ? function.getSimpleName() : description,
                    () ->
                            TypeInferenceExtractor.forTableAggregateFunction(
                                    new DataTypeFactoryMock(), function));
        }

        TestSpec expectNamedArguments(String... expectedArgumentNames) {
            this.expectedArgumentNames = Arrays.asList(expectedArgumentNames);
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

    static Matcher<Throwable> errorMatcher(TestSpec testSpec) {
        return containsCause(new ValidationException(testSpec.expectedErrorMessage));
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
}
