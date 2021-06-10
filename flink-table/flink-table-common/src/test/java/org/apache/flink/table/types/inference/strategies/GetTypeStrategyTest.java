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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.StructuredType;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests for {@link GetTypeStrategy}. */
public class GetTypeStrategyTest extends TypeStrategiesTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forStrategy(
                                "Access field of a row nullable type by name",
                                SpecificTypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull())),
                                DataTypes.STRING().notNull())
                        .calledWithLiteralAt(1, "f0")
                        .expectDataType(DataTypes.BIGINT().nullable()),
                TestSpec.forStrategy(
                                "Access field of a row not null type by name",
                                SpecificTypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.STRING().notNull())
                        .calledWithLiteralAt(1, "f0")
                        .expectDataType(DataTypes.BIGINT().notNull()),
                TestSpec.forStrategy(
                                "Access field of a structured nullable type by name",
                                SpecificTypeStrategies.GET)
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
                                SpecificTypeStrategies.GET)
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
                                "Access field of a row nullable type by index",
                                SpecificTypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull())),
                                DataTypes.INT().notNull())
                        .calledWithLiteralAt(1, 0)
                        .expectDataType(DataTypes.BIGINT().nullable()),
                TestSpec.forStrategy(
                                "Access field of a row not null type by index",
                                SpecificTypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.INT().notNull())
                        .calledWithLiteralAt(1, 0)
                        .expectDataType(DataTypes.BIGINT().notNull()),
                TestSpec.forStrategy(
                                "Fields can be accessed only with a literal (name)",
                                SpecificTypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.STRING().notNull())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Fields can be accessed only with a literal (index)",
                                SpecificTypeStrategies.GET)
                        .inputTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT().notNull()))
                                        .notNull(),
                                DataTypes.INT().notNull())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."));
    }
}
