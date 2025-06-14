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

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy("Valid OBJECT_OF with class", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(DataTypes.STRING()) // field value
                        .expectSignature("f(STRING, [STRING, ANY]*...)")
                        .expectArgumentTypes(DataTypes.STRING()),
                // Basic test case - valid number of arguments (odd number >= 1)
                TestSpec.forStrategy(
                                "Valid OBJECT_OF with class and one field",
                                OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(), // implementation class type
                                DataTypes.STRING(), // field name
                                DataTypes.INT()) // field value
                        .expectArgumentTypes(
                                DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()),

                // Test with structured fields
                TestSpec.forStrategy(
                                "Valid OBJECT_OF with multiple fields", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(), // implementation class
                                DataTypes.STRING(), // field1 name
                                DataTypes.STRUCTURED(
                                        "c1",
                                        DataTypes.FIELD("f1", DataTypes.INT())), // field1 value
                                DataTypes.STRING(), // field2 name
                                DataTypes.STRUCTURED(
                                        "c2",
                                        DataTypes.FIELD("f1", DataTypes.FLOAT()))) // field2 value
                        .expectArgumentTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRUCTURED("c1", DataTypes.FIELD("f1", DataTypes.INT())),
                                DataTypes.STRING(),
                                DataTypes.STRUCTURED(
                                        "c2", DataTypes.FIELD("f1", DataTypes.FLOAT()))),

                // Invalid test case - even number of arguments
                TestSpec.forStrategy(
                                "Invalid OBJECT_OF with even number of arguments",
                                OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(), // implementation class
                                DataTypes.STRING()) // only field name, missing  value
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
                        .expectArgumentTypes(DataTypes.INT())
                        .expectErrorMessage(
                                "The first argument must be a STRING/VARCHAR type representing the class name."),

                // Invalid test case - field name not a string
                TestSpec.forStrategy(
                                "OBJECT_OF with non-string field name", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(), // implementation class
                                DataTypes.INT(), // field name (not a string)
                                DataTypes.INT()) // field value
                        .calledWithLiteralAt(1, 5)
                        .expectArgumentTypes(DataTypes.STRING(), DataTypes.INT(), DataTypes.INT())
                        .expectErrorMessage(
                                "The field key at position 2 must be a STRING/VARCHAR type, but was INT."),

                // Invalid test case - repeated field names
                TestSpec.forStrategy(
                                "OBJECT_OF with repeated field names", OBJECT_OF_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.INT())
                        .calledWithLiteralAt(1, "field1")
                        .calledWithLiteralAt(3, "field1")
                        .expectArgumentTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.INT())
                        .expectErrorMessage("The field name 'field1' at position 4 is repeated."));
    }
}
