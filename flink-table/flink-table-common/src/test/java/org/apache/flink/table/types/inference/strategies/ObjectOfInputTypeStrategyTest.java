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
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import java.util.stream.Stream;

/** Tests for {@link ObjectOfInputTypeStrategy}. */
class ObjectOfInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final InputTypeStrategy OBJECT_OF_INPUT_STRATEGY =
            BuiltInFunctionDefinitions.OBJECT_OF
                    .getTypeInference(new DataTypeFactoryMock())
                    .getInputTypeStrategy();
    private static final String USER_CLASS_PATH = "com.example.User";

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Test valid OBJECT_OF with no fields and values
                TestSpec.forStrategy("Valid OBJECT_OF with only class", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(DataTypes.STRING().notNull())
                        .calledWithLiteralAt(0, USER_CLASS_PATH)
                        .expectSignature("f(class name STRING, [STRING, ANY]*...)")
                        .expectArgumentTypes(DataTypes.STRING().notNull()),

                // Test valid number of arguments (odd number >= 1)
                TestSpec.forStrategy(
                                "Valid OBJECT_OF with class and one field and value",
                                OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT())
                        .calledWithLiteralAt(0, USER_CLASS_PATH)
                        .calledWithLiteralAt(1, "field1")
                        .expectArgumentTypes(
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT()),

                // Test with structured fields
                TestSpec.forStrategy(
                                "Valid OBJECT_OF with multiple fields", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING().notNull(), // implementation class
                                DataTypes.STRING().notNull(), // field1 name
                                DataTypes.STRUCTURED(
                                        "c1",
                                        DataTypes.FIELD("f1", DataTypes.INT())), // field1 value
                                DataTypes.STRING().notNull(), // field2 name
                                DataTypes.STRUCTURED(
                                        "c2",
                                        DataTypes.FIELD("f1", DataTypes.FLOAT()))) // field2 value
                        .calledWithLiteralAt(0, USER_CLASS_PATH)
                        .calledWithLiteralAt(1, "field1")
                        .calledWithLiteralAt(3, "field2")
                        .expectArgumentTypes(
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRUCTURED("c1", DataTypes.FIELD("f1", DataTypes.INT())),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRUCTURED(
                                        "c2", DataTypes.FIELD("f1", DataTypes.FLOAT()))),

                // Invalid test case - class argument is type STRING with value null
                TestSpec.forStrategy(
                                "Invalid OBJECT_OF with class argument as type STRING with null value",
                                OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(DataTypes.STRING())
                        .calledWithLiteralAt(0, null)
                        .expectErrorMessage(
                                "The first argument must be a non-nullable character string literal representing the class name."),

                // Invalid test case - key is type STRING with value null
                TestSpec.forStrategy(
                                "Valid OBJECT_OF with key as type STRING with null value",
                                OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING().notNull(), DataTypes.STRING(), DataTypes.INT())
                        .calledWithLiteralAt(0, USER_CLASS_PATH)
                        .calledWithLiteralAt(1, null)
                        .expectErrorMessage(
                                "The field key at position 2 must be a non-nullable character string literal."),

                // Invalid test case - even number of arguments
                TestSpec.forStrategy(
                                "Invalid OBJECT_OF with even number of arguments",
                                OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING().notNull(), // implementation class
                                DataTypes.STRING().notNull()) // only field name, missing value
                        .calledWithLiteralAt(0, USER_CLASS_PATH)
                        .expectErrorMessage("Invalid number of arguments."),

                // Invalid test case - no arguments
                TestSpec.forStrategy(
                                "Invalid OBJECT_OF with no arguments", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes()
                        .expectErrorMessage("Invalid number of arguments."),

                // Invalid test case - class name not a string
                TestSpec.forStrategy(
                                "OBJECT_OF with non-string class name", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(DataTypes.INT())
                        .calledWithLiteralAt(0, 72)
                        .expectArgumentTypes(DataTypes.INT())
                        .expectErrorMessage(
                                "The first argument must be a non-nullable character string representing the class name, but was INT."),

                // Invalid test case - field name not a string
                TestSpec.forStrategy(
                                "OBJECT_OF with non-string field name", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING().notNull(), // implementation class
                                DataTypes.INT(), // field name (not a string)
                                DataTypes.INT()) // field value
                        .calledWithLiteralAt(0, USER_CLASS_PATH)
                        .calledWithLiteralAt(1, 5)
                        .expectArgumentTypes(DataTypes.STRING(), DataTypes.INT(), DataTypes.INT())
                        .expectErrorMessage(
                                "The field key at position 2 must be a non-nullable character string, but was INT."),

                // Invalid test case - repeated field names
                TestSpec.forStrategy(
                                "OBJECT_OF with repeated field names", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT())
                        .calledWithLiteralAt(0, USER_CLASS_PATH)
                        .calledWithLiteralAt(1, "field1")
                        .calledWithLiteralAt(3, "field1")
                        .expectArgumentTypes(
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT())
                        .expectErrorMessage("The field name 'field1' at position 4 is repeated."));
    }
}
