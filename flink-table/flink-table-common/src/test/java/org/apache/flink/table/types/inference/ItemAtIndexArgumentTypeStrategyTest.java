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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.inference.strategies.ItemAtIndexArgumentTypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import java.util.stream.Stream;

/** Tests for {@link ItemAtIndexArgumentTypeStrategy}. */
class ItemAtIndexArgumentTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final InputTypeStrategy ITEM_AT_INPUT_STRATEGY =
            BuiltInFunctionDefinitions.AT
                    .getTypeInference(new DataTypeFactoryMock())
                    .getInputTypeStrategy();

    @Override
    protected Stream<TestSpec> testData() {

        return Stream.of(
                TestSpec.forStrategy("Validate integer index for an array", ITEM_AT_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.STRING().notNull()),
                                DataTypes.SMALLINT().notNull())
                        .expectSignature(
                                "f([<ARRAY> | <MAP>], [<INTEGER NUMERIC> | <MAP_KEY_TYPE>])")
                        .expectArgumentTypes(
                                DataTypes.ARRAY(DataTypes.STRING().notNull()),
                                DataTypes.SMALLINT().notNull()),
                TestSpec.forStrategy(
                                "Validate not an integer index for an array",
                                ITEM_AT_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.STRING().notNull()), DataTypes.STRING())
                        .expectErrorMessage(
                                "Array can be indexed only using an INTEGER NUMERIC type."),
                TestSpec.forStrategy("Validate correct map key", ITEM_AT_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING().notNull()),
                                DataTypes.SMALLINT())
                        .expectSignature(
                                "f([<ARRAY> | <MAP>], [<INTEGER NUMERIC> | <MAP_KEY_TYPE>])")
                        .expectArgumentTypes(
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING().notNull()),
                                DataTypes.BIGINT()),
                TestSpec.forStrategy("Validate incorrect map key", ITEM_AT_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING().notNull()),
                                DataTypes.STRING())
                        .expectErrorMessage("Expected index for a MAP to be of type: BIGINT"),
                TestSpec.forStrategy("Validate incorrect index", ITEM_AT_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.BIGINT()), DataTypes.INT().notNull())
                        .calledWithLiteralAt(1, 0)
                        .expectErrorMessage(
                                "The provided index must be a valid SQL index starting from 1, but was '0'"));
    }
}
