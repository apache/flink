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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.FunctionDefinitionMock;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.types.inference.TypeStrategies.MISSING;
import static org.apache.flink.table.types.inference.TypeStrategies.STRING_CONCAT;
import static org.apache.flink.table.types.inference.TypeStrategies.argument;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.table.types.inference.TypeStrategies.nullable;
import static org.apache.flink.table.types.inference.TypeStrategies.varyingString;
import static org.hamcrest.CoreMatchers.equalTo;

/** Tests for built-in {@link TypeStrategies}. */
@RunWith(Parameterized.class)
public class TypeStrategiesTest {

    @Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                // missing strategy with arbitrary argument
                TestSpec.forStrategy(MISSING)
                        .inputTypes(DataTypes.INT())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),

                // valid explicit
                TestSpec.forStrategy(explicit(DataTypes.BIGINT()))
                        .inputTypes()
                        .expectDataType(DataTypes.BIGINT()),

                // infer from input
                TestSpec.forStrategy(argument(0))
                        .inputTypes(DataTypes.INT(), DataTypes.STRING())
                        .expectDataType(DataTypes.INT()),

                // infer from not existing input
                TestSpec.forStrategy(argument(0))
                        .inputTypes()
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),

                // (INT, BOOLEAN) -> STRING
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.BOOLEAN())
                        .expectDataType(DataTypes.STRING()),

                // (INT, STRING) -> BOOLEAN
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.STRING())
                        .expectDataType(DataTypes.BOOLEAN().bridgedTo(boolean.class)),

                // (INT, CHAR(10)) -> BOOLEAN
                // but avoiding casts (mapping actually expects STRING)
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.CHAR(10))
                        .expectDataType(DataTypes.BOOLEAN().bridgedTo(boolean.class)),

                // invalid mapping strategy
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.INT())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),

                // invalid return type
                TestSpec.forStrategy(explicit(DataTypes.NULL()))
                        .inputTypes()
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments. Untyped NULL received."),
                TestSpec.forStrategy(
                                "First type strategy",
                                TypeStrategies.first(
                                        (callContext) -> Optional.empty(),
                                        explicit(DataTypes.INT())))
                        .inputTypes()
                        .expectDataType(DataTypes.INT()),
                TestSpec.forStrategy(
                                "Match root type strategy",
                                TypeStrategies.matchFamily(0, LogicalTypeFamily.NUMERIC))
                        .inputTypes(DataTypes.INT())
                        .expectDataType(DataTypes.INT()),
                TestSpec.forStrategy(
                                "Invalid match root type strategy",
                                TypeStrategies.matchFamily(0, LogicalTypeFamily.NUMERIC))
                        .inputTypes(DataTypes.BOOLEAN())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),
                TestSpec.forStrategy("Infer a row type", TypeStrategies.ROW)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.STRING())
                        .expectDataType(
                                DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.BIGINT()),
                                                DataTypes.FIELD("f1", DataTypes.STRING()))
                                        .notNull()),
                TestSpec.forStrategy("Infer an array type", TypeStrategies.ARRAY)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.BIGINT())
                        .expectDataType(DataTypes.ARRAY(DataTypes.BIGINT()).notNull()),
                TestSpec.forStrategy("Infer a map type", TypeStrategies.MAP)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.STRING().notNull())
                        .expectDataType(
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING().notNull())
                                        .notNull()),
                TestSpec.forStrategy(
                                "Cascading to nullable type",
                                nullable(explicit(DataTypes.BOOLEAN().notNull())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.BOOLEAN().nullable()),
                TestSpec.forStrategy(
                                "Cascading to not null type",
                                nullable(explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).notNull())
                        .expectDataType(DataTypes.BOOLEAN().notNull()),
                TestSpec.forStrategy(
                                "Cascading to not null type but only consider first argument",
                                nullable(
                                        ConstantArgumentCount.to(0),
                                        explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.BOOLEAN().notNull()),
                TestSpec.forStrategy(
                                "Cascading to null type but only consider first two argument",
                                nullable(
                                        ConstantArgumentCount.to(1),
                                        explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.BOOLEAN().nullable()),
                TestSpec.forStrategy(
                                "Cascading to not null type but only consider the second and third argument",
                                nullable(
                                        ConstantArgumentCount.between(1, 2),
                                        explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(
                                DataTypes.BIGINT().nullable(),
                                DataTypes.BIGINT().notNull(),
                                DataTypes.VARCHAR(2).notNull())
                        .expectDataType(DataTypes.BOOLEAN().notNull()),
                TestSpec.forStrategy("Find a common type", TypeStrategies.COMMON)
                        .inputTypes(
                                DataTypes.INT(),
                                DataTypes.TINYINT().notNull(),
                                DataTypes.DECIMAL(20, 10))
                        .expectDataType(DataTypes.DECIMAL(20, 10)),
                TestSpec.forStrategy("Find a decimal sum", TypeStrategies.DECIMAL_PLUS)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(6, 4).notNull()),
                TestSpec.forStrategy("Find a decimal quotient", TypeStrategies.DECIMAL_DIVIDE)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(11, 8).notNull()),
                TestSpec.forStrategy("Find a decimal product", TypeStrategies.DECIMAL_TIMES)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(9, 6).notNull()),
                TestSpec.forStrategy("Find a decimal modulo", TypeStrategies.DECIMAL_MOD)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(5, 4).notNull()),
                TestSpec.forStrategy(
                                "Convert to varying string",
                                varyingString(explicit(DataTypes.CHAR(12).notNull())))
                        .inputTypes(DataTypes.CHAR(12).notNull())
                        .expectDataType(DataTypes.VARCHAR(12).notNull()),
                TestSpec.forStrategy("Concat two strings", STRING_CONCAT)
                        .inputTypes(DataTypes.CHAR(12).notNull(), DataTypes.VARCHAR(12))
                        .expectDataType(DataTypes.VARCHAR(24)),
                TestSpec.forStrategy(
                                "Access field of a row nullable type by name", TypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull())),
                                DataTypes.STRING().notNull())
                        .calledWithLiteralAt(1, "f0")
                        .expectDataType(DataTypes.BIGINT().nullable()),
                TestSpec.forStrategy(
                                "Access field of a row not null type by name", TypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.STRING().notNull())
                        .calledWithLiteralAt(1, "f0")
                        .expectDataType(DataTypes.BIGINT().notNull()),
                TestSpec.forStrategy(
                                "Access field of a structured nullable type by name",
                                TypeStrategies.GET)
                        .inputTypes(
                                new FieldsDataType(
                                                StructuredType.newBuilder(
                                                                ObjectIdentifier.of(
                                                                        "cat", "db", "type"))
                                                        .attributes(
                                                                Collections.singletonList(
                                                                        new StructuredType
                                                                                .StructuredAttribute(
                                                                                "f0",
                                                                                new BigIntType(
                                                                                        false))))
                                                        .build(),
                                                Collections.singletonList(
                                                        DataTypes.BIGINT().notNull()))
                                        .nullable(),
                                DataTypes.STRING().notNull())
                        .calledWithLiteralAt(1, "f0")
                        .expectDataType(DataTypes.BIGINT().nullable()),
                TestSpec.forStrategy(
                                "Access field of a structured not null type by name",
                                TypeStrategies.GET)
                        .inputTypes(
                                new FieldsDataType(
                                                StructuredType.newBuilder(
                                                                ObjectIdentifier.of(
                                                                        "cat", "db", "type"))
                                                        .attributes(
                                                                Collections.singletonList(
                                                                        new StructuredType
                                                                                .StructuredAttribute(
                                                                                "f0",
                                                                                new BigIntType(
                                                                                        false))))
                                                        .build(),
                                                Collections.singletonList(
                                                        DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.STRING().notNull())
                        .calledWithLiteralAt(1, "f0")
                        .expectDataType(DataTypes.BIGINT().notNull()),
                TestSpec.forStrategy(
                                "Access field of a row nullable type by index", TypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull())),
                                DataTypes.INT().notNull())
                        .calledWithLiteralAt(1, 0)
                        .expectDataType(DataTypes.BIGINT().nullable()),
                TestSpec.forStrategy(
                                "Access field of a row not null type by index", TypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.INT().notNull())
                        .calledWithLiteralAt(1, 0)
                        .expectDataType(DataTypes.BIGINT().notNull()),
                TestSpec.forStrategy(
                                "Fields can be accessed only with a literal (name)",
                                TypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.STRING().notNull())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),
                TestSpec.forStrategy(
                                "Fields can be accessed only with a literal (index)",
                                TypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.INT().notNull())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),
                TestSpec.forStrategy(
                                "Average with grouped aggregation",
                                TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                        .inputTypes(DataTypes.INT().notNull())
                        .calledWithGroupedAggregation()
                        .expectDataType(DataTypes.INT().notNull()),
                TestSpec.forStrategy(
                                "Average without grouped aggregation",
                                TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                        .inputTypes(DataTypes.INT().notNull())
                        .expectDataType(DataTypes.INT()));
    }

    @Parameter public TestSpec testSpec;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testTypeStrategy() {
        if (testSpec.expectedErrorMessage != null) {
            thrown.expect(ValidationException.class);
            thrown.expectCause(
                    containsCause(new ValidationException(testSpec.expectedErrorMessage)));
        }
        TypeInferenceUtil.Result result = runTypeInference();
        if (testSpec.expectedDataType != null) {
            Assert.assertThat(result.getOutputDataType(), equalTo(testSpec.expectedDataType));
        }
    }

    // --------------------------------------------------------------------------------------------

    private TypeInferenceUtil.Result runTypeInference() {
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

    private static class TestSpec {

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

        static TestSpec forStrategy(TypeStrategy strategy) {
            return new TestSpec(null, strategy);
        }

        static TestSpec forStrategy(String description, TypeStrategy strategy) {
            return new TestSpec(description, strategy);
        }

        TestSpec inputTypes(DataType... dataTypes) {
            this.inputTypes = Arrays.asList(dataTypes);
            return this;
        }

        TestSpec calledWithLiteralAt(int pos, Object value) {
            this.literalPos = pos;
            this.literalValue = value;
            return this;
        }

        TestSpec calledWithGroupedAggregation() {
            this.isGroupedAggregation = true;
            return this;
        }

        TestSpec expectDataType(DataType expectedDataType) {
            this.expectedDataType = expectedDataType;
            return this;
        }

        TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        @Override
        public String toString() {
            return description != null ? description : "";
        }
    }

    private static TypeStrategy createMappingTypeStrategy() {
        final Map<InputTypeStrategy, TypeStrategy> mappings = new HashMap<>();
        mappings.put(
                InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.INT()),
                        InputTypeStrategies.explicit(DataTypes.STRING())),
                explicit(DataTypes.BOOLEAN().bridgedTo(boolean.class)));
        mappings.put(
                InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.INT()),
                        InputTypeStrategies.explicit(DataTypes.BOOLEAN())),
                explicit(DataTypes.STRING()));
        return TypeStrategies.mapping(mappings);
    }
}
