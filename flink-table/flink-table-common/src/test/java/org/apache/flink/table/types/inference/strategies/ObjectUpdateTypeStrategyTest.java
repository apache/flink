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
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;

import java.util.stream.Stream;

/** Tests for {@link ObjectUpdateTypeStrategy}. */
class ObjectUpdateTypeStrategyTest extends TypeStrategiesTestBase {

    private static final String USER_CLASS_PATH = "com.example.User";

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // The attribute names below are chosen such that their hash order differs from
                // their declaration order. The inferred type must always follow the declaration
                // order of the input type, because the runtime writes the updated values at the
                // declared positions.
                TestSpec.forStrategy(
                                "Attribute order is preserved when the updated value changes the type",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                DataTypes.STRUCTURED(
                                        USER_CLASS_PATH,
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT())),
                                DataTypes.STRING().notNull(),
                                DataTypes.BIGINT())
                        .calledWithLiteralAt(1, "age")
                        .expectDataType(
                                DataTypes.STRUCTURED(
                                        USER_CLASS_PATH,
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.BIGINT()))),
                TestSpec.forStrategy(
                                "Attribute order is preserved when the updated value keeps the type",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                DataTypes.STRUCTURED(
                                        USER_CLASS_PATH,
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT())),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING())
                        .calledWithLiteralAt(1, "name")
                        .expectDataType(
                                DataTypes.STRUCTURED(
                                        USER_CLASS_PATH,
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT()))),
                TestSpec.forStrategy(
                                "Attribute order is preserved for a two-attribute structured type",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                DataTypes.STRUCTURED(
                                        USER_CLASS_PATH,
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING())),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT())
                        .calledWithLiteralAt(1, "name")
                        .expectDataType(
                                DataTypes.STRUCTURED(
                                        USER_CLASS_PATH,
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.INT()))));
    }
}
