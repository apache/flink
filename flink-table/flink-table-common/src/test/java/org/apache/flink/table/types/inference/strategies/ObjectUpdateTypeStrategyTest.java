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
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;

import java.util.stream.Stream;

/** Tests for {@link ObjectUpdateTypeStrategy}. */
class ObjectUpdateTypeStrategyTest extends TypeStrategiesTestBase {

    private static final String USER_CLASS_PATH = "com.example.User";
    private static final String ADDRESS_CLASS_PATH = "com.example.Address";
    private static final String ITEM_CLASS_PATH = "com.example.Item";

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
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT())),
                                DataTypes.STRING().notNull(),
                                DataTypes.BIGINT())
                        .calledWithLiteralAt(1, "age")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.BIGINT()))),
                TestSpec.forStrategy(
                                "Attribute order is preserved when the updated value keeps the type",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT())),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING())
                        .calledWithLiteralAt(1, "name")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("age", DataTypes.INT()))),
                TestSpec.forStrategy(
                                "Attribute order is preserved for a two-attribute structured type",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING())),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT())
                        .calledWithLiteralAt(1, "name")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.INT()))),
                // The nested attribute names are chosen the same way, so that a nested type is
                // also carried over unchanged rather than rebuilt in hash order.
                TestSpec.forStrategy(
                                "Attribute order is preserved when a nested object is updated",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("address", address(DataTypes.INT()))),
                                DataTypes.STRING().notNull(),
                                address(DataTypes.STRING()))
                        .calledWithLiteralAt(1, "address")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("address", address(DataTypes.STRING())))),
                TestSpec.forStrategy(
                                "Attribute order is preserved when an array of objects is updated",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "items", DataTypes.ARRAY(item(DataTypes.INT())))),
                                DataTypes.STRING().notNull(),
                                DataTypes.ARRAY(item(DataTypes.BIGINT())))
                        .calledWithLiteralAt(1, "items")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "items",
                                                DataTypes.ARRAY(item(DataTypes.BIGINT()))))),
                TestSpec.forStrategy(
                                "Attribute order is preserved when a nested array is updated",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "matrix",
                                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))),
                                DataTypes.STRING().notNull(),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT())))
                        .calledWithLiteralAt(1, "matrix")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "matrix",
                                                DataTypes.ARRAY(
                                                        DataTypes.ARRAY(DataTypes.BIGINT()))))),
                TestSpec.forStrategy(
                                "Attribute order is preserved when a map is updated",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "props",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(),
                                                        DataTypes.ARRAY(DataTypes.STRING())))),
                                DataTypes.STRING().notNull(),
                                DataTypes.MAP(DataTypes.STRING(), item(DataTypes.INT())))
                        .calledWithLiteralAt(1, "props")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "props",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(),
                                                        item(DataTypes.INT()))))),
                TestSpec.forStrategy(
                                "Attribute order is preserved for a variant attribute",
                                SpecificTypeStrategies.OBJECT_UPDATE)
                        .inputTypes(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("payload", DataTypes.VARIANT())),
                                DataTypes.STRING().notNull(),
                                DataTypes.VARIANT())
                        .calledWithLiteralAt(1, "name")
                        .expectDataType(
                                user(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("name", DataTypes.VARIANT()),
                                        DataTypes.FIELD("payload", DataTypes.VARIANT()))));
    }

    private static DataType user(final Field... fields) {
        return DataTypes.STRUCTURED(USER_CLASS_PATH, fields);
    }

    private static DataType address(final DataType zipType) {
        return DataTypes.STRUCTURED(
                ADDRESS_CLASS_PATH,
                DataTypes.FIELD("street", DataTypes.STRING()),
                DataTypes.FIELD("zip", zipType),
                DataTypes.FIELD("city", DataTypes.STRING()));
    }

    private static DataType item(final DataType qtyType) {
        return DataTypes.STRUCTURED(
                ITEM_CLASS_PATH,
                DataTypes.FIELD("sku", DataTypes.STRING()),
                DataTypes.FIELD("qty", qtyType),
                DataTypes.FIELD("price", DataTypes.DOUBLE()));
    }
}
