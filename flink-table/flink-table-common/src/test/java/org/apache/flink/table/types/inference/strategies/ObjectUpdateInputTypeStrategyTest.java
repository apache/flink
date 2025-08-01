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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import java.util.stream.Stream;

/** Tests for {@link ObjectUpdateInputTypeStrategy}. */
class ObjectUpdateInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final InputTypeStrategy OBJECT_UPDATE_INPUT_STRATEGY =
            BuiltInFunctionDefinitions.OBJECT_UPDATE
                    .getTypeInference(new DataTypeFactoryMock())
                    .getInputTypeStrategy();

    private static final String USER_CLASS_PATH = "com.example.User";

    private static final DataType STRUCTURED_TYPE =
            DataTypes.STRUCTURED(
                            USER_CLASS_PATH,
                            DataTypes.FIELD("name", DataTypes.STRING()),
                            DataTypes.FIELD("age", DataTypes.INT()))
                    .notNull();

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Test valid OBJECT_UPDATE with a pair of key-value arguments
                TestSpec.forStrategy(
                                "Valid OBJECT_UPDATE with a pair of key-value arguments",
                                OBJECT_UPDATE_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                STRUCTURED_TYPE, DataTypes.STRING().notNull(), DataTypes.INT())
                        .calledWithLiteralAt(1, "age")
                        .calledWithLiteralAt(2, 14)
                        .expectSignature("f(object STRUCTURED_TYPE, [STRING, ANY]+...)")
                        .expectArgumentTypes(
                                STRUCTURED_TYPE, DataTypes.STRING().notNull(), DataTypes.INT()),
                // Test valid OBJECT_UPDATE with multiple pair of key-value arguments
                TestSpec.forStrategy(
                                "Valid OBJECT_UPDATE with multiple pair of key-value arguments",
                                OBJECT_UPDATE_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                STRUCTURED_TYPE,
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT())
                        .calledWithLiteralAt(1, "name")
                        .calledWithLiteralAt(2, "Alice")
                        .calledWithLiteralAt(3, "age")
                        .calledWithLiteralAt(4, 14)
                        .expectArgumentTypes(
                                STRUCTURED_TYPE,
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT()),
                // Invalid test case - only one argument passed
                TestSpec.forStrategy(
                                "Invalid OBJECT_UPDATE with only one argument",
                                OBJECT_UPDATE_INPUT_STRATEGY)
                        .calledWithArgumentTypes(STRUCTURED_TYPE)
                        .expectErrorMessage(
                                "Invalid number of arguments. At least 3 arguments expected but 1 passed."),
                // Invalid test case - only two arguments passed
                TestSpec.forStrategy(
                                "Invalid OBJECT_UPDATE with only two arguments",
                                OBJECT_UPDATE_INPUT_STRATEGY)
                        .calledWithArgumentTypes(STRUCTURED_TYPE, DataTypes.STRING().notNull())
                        .expectErrorMessage(
                                "Invalid number of arguments. At least 3 arguments expected but 2 passed."),
                // Invalid test case - field key is null
                TestSpec.forStrategy(
                                "Invalid OBJECT_UPDATE with field key as null",
                                OBJECT_UPDATE_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                STRUCTURED_TYPE, DataTypes.STRING(), DataTypes.INT())
                        .calledWithLiteralAt(1, null)
                        .expectErrorMessage(
                                "The field key at position 2 must be a non-null character string literal."),
                // Invalid test case - field key is repeated
                TestSpec.forStrategy(
                                "Invalid OBJECT_UPDATE with field repeated",
                                OBJECT_UPDATE_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                STRUCTURED_TYPE,
                                DataTypes.STRING().notNull(),
                                DataTypes.INT(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT())
                        .calledWithLiteralAt(1, "age")
                        .calledWithLiteralAt(2, 42)
                        .calledWithLiteralAt(3, "age") // Repeated field key
                        .calledWithLiteralAt(4, 14)
                        .expectErrorMessage("The field name 'age' at position 4 is repeated."),
                // Invalid test case - field key is not present in the structured type
                TestSpec.forStrategy(
                                "Invalid OBJECT_UPDATE with field invalid field name",
                                OBJECT_UPDATE_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                STRUCTURED_TYPE, DataTypes.STRING().notNull(), DataTypes.INT())
                        .calledWithLiteralAt(1, "someRandomFieldName")
                        .calledWithLiteralAt(2, 42)
                        .expectErrorMessage(
                                "The field name 'someRandomFieldName' at position 2 is not part of the structured type attributes. Available attributes: [name, age]."));
    }
}
