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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.FunctionDefinitionMock;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.test.TableAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base class for tests of {@link TypeStrategies}. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TypeStrategiesTestBase {

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testTypeStrategy(TestSpec testSpec) {
        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(() -> runTypeInference(testSpec))
                    .satisfies(
                            anyCauseMatches(
                                    ValidationException.class, testSpec.expectedErrorMessage));
        } else if (testSpec.expectedDataType != null) {
            assertThat(runTypeInference(testSpec).getOutputDataType())
                    .isEqualTo(testSpec.expectedDataType);
        }
    }

    protected abstract Stream<TestSpec> testData();

    // --------------------------------------------------------------------------------------------

    private TypeInferenceUtil.Result runTypeInference(TestSpec testSpec) {
        final FunctionDefinitionMock functionDefinitionMock = new FunctionDefinitionMock();
        functionDefinitionMock.functionKind = FunctionKind.SCALAR;
        final CallContextMock callContextMock = new CallContextMock();
        callContextMock.functionDefinition = functionDefinitionMock;
        callContextMock.argumentDataTypes = testSpec.inputTypes;
        callContextMock.name = "f";
        callContextMock.outputDataType = Optional.empty();
        callContextMock.isGroupedAggregation = testSpec.isGroupedAggregation;

        callContextMock.argumentLiterals =
                IntStream.range(0, testSpec.inputTypes.size())
                        .mapToObj(i -> testSpec.literalPos != null && i == testSpec.literalPos)
                        .collect(Collectors.toList());
        callContextMock.argumentValues =
                IntStream.range(0, testSpec.inputTypes.size())
                        .mapToObj(
                                i ->
                                        (testSpec.literalPos != null && i == testSpec.literalPos)
                                                ? Optional.ofNullable(testSpec.literalValue)
                                                : Optional.empty())
                        .collect(Collectors.toList());

        final TypeInference typeInference =
                TypeInference.newBuilder()
                        .inputTypeStrategy(InputTypeStrategies.WILDCARD)
                        .outputTypeStrategy(testSpec.strategy)
                        .build();
        return TypeInferenceUtil.runTypeInference(typeInference, callContextMock, null);
    }

    // --------------------------------------------------------------------------------------------

    /** Specification of a test scenario. */
    public static class TestSpec {

        private @Nullable final String description;

        private final TypeStrategy strategy;

        private List<DataType> inputTypes;

        private @Nullable DataType expectedDataType;

        private @Nullable String expectedErrorMessage;

        private @Nullable Integer literalPos;

        private @Nullable Object literalValue;

        private boolean isGroupedAggregation;

        private TestSpec(@Nullable String description, TypeStrategy strategy) {
            this.description = description;
            this.strategy = strategy;
        }

        public static TestSpec forStrategy(TypeStrategy strategy) {
            return new TestSpec(null, strategy);
        }

        public static TestSpec forStrategy(String description, TypeStrategy strategy) {
            return new TestSpec(description, strategy);
        }

        public TestSpec inputTypes(DataType... dataTypes) {
            this.inputTypes = Arrays.asList(dataTypes);
            return this;
        }

        public TestSpec calledWithLiteralAt(int pos, Object value) {
            this.literalPos = pos;
            this.literalValue = value;
            return this;
        }

        public TestSpec calledWithGroupedAggregation() {
            this.isGroupedAggregation = true;
            return this;
        }

        public TestSpec expectDataType(DataType expectedDataType) {
            this.expectedDataType = expectedDataType;
            return this;
        }

        public TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        @Override
        public String toString() {
            return description != null ? description : "";
        }
    }
}
