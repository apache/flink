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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.inference.TypeStrategies.MISSING;
import static org.apache.flink.table.types.inference.TypeStrategies.argument;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.table.types.inference.TypeStrategies.nullableIfArgs;
import static org.apache.flink.table.types.inference.TypeStrategies.varyingString;

/** Tests for built-in {@link TypeStrategies}. */
public class TypeStrategiesTest extends TypeStrategiesTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TypeStrategiesTestBase.TestSpec> testData() {
        return Arrays.asList(
                // missing strategy with arbitrary argument
                TypeStrategiesTestBase.TestSpec.forStrategy(MISSING)
                        .inputTypes(DataTypes.INT())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),

                // valid explicit
                TypeStrategiesTestBase.TestSpec.forStrategy(explicit(DataTypes.BIGINT()))
                        .inputTypes()
                        .expectDataType(DataTypes.BIGINT()),

                // infer from input
                TypeStrategiesTestBase.TestSpec.forStrategy(argument(0))
                        .inputTypes(DataTypes.INT(), DataTypes.STRING())
                        .expectDataType(DataTypes.INT()),

                // infer from not existing input
                TypeStrategiesTestBase.TestSpec.forStrategy(argument(0))
                        .inputTypes()
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),

                // invalid return type
                TypeStrategiesTestBase.TestSpec.forStrategy(explicit(DataTypes.NULL()))
                        .inputTypes()
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments. Untyped NULL received."),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "First type strategy",
                                TypeStrategies.first(
                                        (callContext) -> Optional.empty(),
                                        explicit(DataTypes.INT())))
                        .inputTypes()
                        .expectDataType(DataTypes.INT()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Match root type strategy",
                                TypeStrategies.matchFamily(0, LogicalTypeFamily.NUMERIC))
                        .inputTypes(DataTypes.INT())
                        .expectDataType(DataTypes.INT()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Invalid match root type strategy",
                                TypeStrategies.matchFamily(0, LogicalTypeFamily.NUMERIC))
                        .inputTypes(DataTypes.BOOLEAN())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Cascading to nullable type",
                                nullableIfArgs(explicit(DataTypes.BOOLEAN().notNull())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.BOOLEAN().nullable()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Cascading to not null type",
                                nullableIfArgs(explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).notNull())
                        .expectDataType(DataTypes.BOOLEAN().notNull()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Cascading to not null type but only consider first argument",
                                nullableIfArgs(
                                        ConstantArgumentCount.to(0),
                                        explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.BOOLEAN().notNull()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Cascading to null type but only consider first two argument",
                                nullableIfArgs(
                                        ConstantArgumentCount.to(1),
                                        explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.BOOLEAN().nullable()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Cascading to not null type but only consider the second and third argument",
                                nullableIfArgs(
                                        ConstantArgumentCount.between(1, 2),
                                        explicit(DataTypes.BOOLEAN().nullable())))
                        .inputTypes(
                                DataTypes.BIGINT().nullable(),
                                DataTypes.BIGINT().notNull(),
                                DataTypes.VARCHAR(2).notNull())
                        .expectDataType(DataTypes.BOOLEAN().notNull()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Find a common type", TypeStrategies.COMMON)
                        .inputTypes(
                                DataTypes.INT(),
                                DataTypes.TINYINT().notNull(),
                                DataTypes.DECIMAL(20, 10))
                        .expectDataType(DataTypes.DECIMAL(20, 10)),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Convert to varying string",
                                varyingString(explicit(DataTypes.CHAR(12).notNull())))
                        .inputTypes(DataTypes.CHAR(12).notNull())
                        .expectDataType(DataTypes.VARCHAR(12).notNull()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Average with grouped aggregation",
                                TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                        .inputTypes(DataTypes.INT().notNull())
                        .calledWithGroupedAggregation()
                        .expectDataType(DataTypes.INT().notNull()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Average without grouped aggregation",
                                TypeStrategies.aggArg0(LogicalTypeMerging::findAvgAggType, true))
                        .inputTypes(DataTypes.INT().notNull())
                        .expectDataType(DataTypes.INT()));
    }
}
