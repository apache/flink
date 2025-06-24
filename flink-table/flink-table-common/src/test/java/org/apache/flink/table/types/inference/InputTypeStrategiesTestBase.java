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

package org.apache.flink.table.types.inference;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.FunctionDefinitionMock;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base class for testing {@link InputTypeStrategy}. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class InputTypeStrategiesTestBase {

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testStrategy(TestSpec testSpec) {
        if (testSpec.expectedSignature != null) {
            assertThat(generateSignature(testSpec)).isEqualTo(testSpec.expectedSignature);
        }
        for (List<DataType> actualArgumentTypes : testSpec.actualArgumentTypes) {
            if (testSpec.expectedErrorMessage != null) {
                assertThatThrownBy(() -> runTypeInference(actualArgumentTypes, testSpec))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class, testSpec.expectedErrorMessage));
            } else if (testSpec.expectedArgumentTypes != null) {
                assertThat(
                                runTypeInference(actualArgumentTypes, testSpec)
                                        .getExpectedArgumentTypes())
                        .isEqualTo(testSpec.expectedArgumentTypes);
            }
        }
    }

    protected abstract Stream<TestSpec> testData();

    // --------------------------------------------------------------------------------------------

    private String generateSignature(TestSpec testSpec) {
        final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
        functionDefinitionMock.functionKind = FunctionKind.SCALAR;
        return TypeInferenceUtil.generateSignature(
                createTypeInference(testSpec), "f", functionDefinitionMock);
    }

    private TypeInferenceUtil.Result runTypeInference(
            List<DataType> actualArgumentTypes, TestSpec testSpec) {
        final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
        functionDefinitionMock.functionKind = FunctionKind.SCALAR;

        final CallContextMock callContextMock = new CallContextMock();
        callContextMock.typeFactory = new DataTypeFactoryMock();
        callContextMock.functionDefinition = functionDefinitionMock;
        callContextMock.argumentDataTypes = actualArgumentTypes;
        callContextMock.argumentLiterals =
                IntStream.range(0, actualArgumentTypes.size())
                        .mapToObj(i -> testSpec.literals.containsKey(i))
                        .collect(Collectors.toList());
        callContextMock.argumentValues =
                IntStream.range(0, actualArgumentTypes.size())
                        .mapToObj(i -> Optional.ofNullable(testSpec.literals.get(i)))
                        .collect(Collectors.toList());
        callContextMock.argumentNulls =
                IntStream.range(0, actualArgumentTypes.size())
                        .mapToObj(i -> false)
                        .collect(Collectors.toList());
        callContextMock.name = "f";
        callContextMock.outputDataType = Optional.empty();

        final TypeInferenceUtil.SurroundingInfo surroundingInfo;
        if (testSpec.surroundingStrategy != null) {
            final TypeInference outerTypeInference =
                    TypeInference.newBuilder()
                            .inputTypeStrategy(testSpec.surroundingStrategy)
                            .outputTypeStrategy(TypeStrategies.MISSING)
                            .build();
            surroundingInfo =
                    TypeInferenceUtil.SurroundingInfo.of(
                            "f_outer",
                            functionDefinitionMock,
                            outerTypeInference,
                            1,
                            0,
                            callContextMock.isGroupedAggregation);
        } else {
            surroundingInfo = null;
        }
        return TypeInferenceUtil.runTypeInference(
                createTypeInference(testSpec), callContextMock, surroundingInfo);
    }

    private TypeInference createTypeInference(TestSpec testSpec) {
        final TypeInference.Builder builder =
                TypeInference.newBuilder()
                        .inputTypeStrategy(testSpec.strategy)
                        .outputTypeStrategy(TypeStrategies.explicit(DataTypes.BOOLEAN()));

        if (testSpec.namedArguments != null) {
            builder.namedArguments(testSpec.namedArguments);
        }
        if (testSpec.typedArguments != null) {
            builder.typedArguments(testSpec.typedArguments);
        }
        return builder.build();
    }

    // --------------------------------------------------------------------------------------------

    /** A specification for tests to execute. */
    protected static class TestSpec {

        private final @Nullable String description;

        private final InputTypeStrategy strategy;

        private @Nullable List<String> namedArguments;

        private @Nullable List<DataType> typedArguments;

        private List<List<DataType>> actualArgumentTypes = new ArrayList<>();

        private Map<Integer, Object> literals = new HashMap<>();

        private @Nullable InputTypeStrategy surroundingStrategy;

        private @Nullable String expectedSignature;

        private @Nullable List<DataType> expectedArgumentTypes;

        private @Nullable String expectedErrorMessage;

        private TestSpec(@Nullable String description, InputTypeStrategy strategy) {
            this.description = description;
            this.strategy = strategy;
        }

        public static TestSpec forStrategy(InputTypeStrategy strategy) {
            return new TestSpec(null, strategy);
        }

        public static TestSpec forStrategy(String description, InputTypeStrategy strategy) {
            return new TestSpec(description, strategy);
        }

        public TestSpec namedArguments(String... names) {
            this.namedArguments = Arrays.asList(names);
            return this;
        }

        public TestSpec typedArguments(DataType... dataTypes) {
            this.typedArguments = Arrays.asList(dataTypes);
            return this;
        }

        public TestSpec surroundingStrategy(InputTypeStrategy surroundingStrategy) {
            this.surroundingStrategy = surroundingStrategy;
            return this;
        }

        public TestSpec calledWithArgumentTypes(AbstractDataType<?>... dataTypes) {
            this.actualArgumentTypes.add(resolveDataTypes(dataTypes));
            return this;
        }

        public TestSpec calledWithLiteralAt(int pos) {
            this.literals.put(pos, null);
            return this;
        }

        public TestSpec calledWithLiteralAt(int pos, Object value) {
            this.literals.put(pos, value);
            return this;
        }

        public TestSpec expectSignature(String signature) {
            this.expectedSignature = signature;
            return this;
        }

        public TestSpec expectArgumentTypes(AbstractDataType<?>... dataTypes) {
            this.expectedArgumentTypes = resolveDataTypes(dataTypes);
            return this;
        }

        public TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        private List<DataType> resolveDataTypes(AbstractDataType<?>[] dataTypes) {
            final DataTypeFactoryMock factoryMock = new DataTypeFactoryMock();
            return Arrays.stream(dataTypes)
                    .map(factoryMock::createDataType)
                    .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            return description != null ? description : strategy.getClass().getSimpleName();
        }
    }
}
