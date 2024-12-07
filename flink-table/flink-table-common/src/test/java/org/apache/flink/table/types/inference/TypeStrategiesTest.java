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
import org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.TypeStrategies.MISSING;
import static org.apache.flink.table.types.inference.TypeStrategies.argument;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.table.types.inference.TypeStrategies.nullableIfAllArgs;
import static org.apache.flink.table.types.inference.TypeStrategies.nullableIfArgs;
import static org.apache.flink.table.types.inference.TypeStrategies.varyingString;
import static org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies.PERCENTILE;

/** Tests for built-in {@link TypeStrategies}. */
class TypeStrategiesTest extends TypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
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
                                "Cascading to not null because one argument is not null",
                                nullableIfAllArgs(TypeStrategies.COMMON))
                        .inputTypes(DataTypes.VARCHAR(2).notNull(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.VARCHAR(2).notNull()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Cascading to nullable because all args are nullable",
                                nullableIfAllArgs(TypeStrategies.COMMON))
                        .inputTypes(
                                DataTypes.VARCHAR(2).nullable(), DataTypes.VARCHAR(2).nullable())
                        .expectDataType(DataTypes.VARCHAR(2).nullable()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Find a common type", TypeStrategies.COMMON)
                        .inputTypes(
                                DataTypes.INT(),
                                DataTypes.TINYINT().notNull(),
                                DataTypes.DECIMAL(20, 10))
                        .expectDataType(DataTypes.DECIMAL(20, 10)),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Find a common type of selected arguments",
                                TypeStrategies.commonRange(ConstantArgumentCount.from(1)))
                        .inputTypes(DataTypes.INT(), DataTypes.SMALLINT(), DataTypes.TINYINT())
                        .expectDataType(DataTypes.SMALLINT()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Find a common type of selected arguments",
                                TypeStrategies.commonRange(ConstantArgumentCount.between(1, 2)))
                        .inputTypes(
                                DataTypes.VARCHAR(10),
                                DataTypes.CHAR(3),
                                DataTypes.VARCHAR(4),
                                DataTypes.CHAR(7))
                        .expectDataType(DataTypes.VARCHAR(4)),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Find a common type of selected arguments",
                                TypeStrategies.commonRange(ConstantArgumentCount.to(1)))
                        .inputTypes(DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT())
                        .expectDataType(DataTypes.SMALLINT()),
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
                        .expectDataType(DataTypes.INT()),

                // PercentileTypeStrategy
                TypeStrategiesTestBase.TestSpec.forStrategy(PERCENTILE)
                        .inputTypes(DataTypes.INT(), DataTypes.DOUBLE())
                        .expectDataType(DataTypes.DOUBLE()),
                TypeStrategiesTestBase.TestSpec.forStrategy(PERCENTILE)
                        .inputTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.DECIMAL(5, 2)))
                        .expectDataType(DataTypes.ARRAY(DataTypes.DOUBLE())),

                // LeadLagStrategy
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Expression not null", SpecificTypeStrategies.LEAD_LAG)
                        .inputTypes(DataTypes.INT().notNull(), DataTypes.BIGINT())
                        .expectDataType(DataTypes.INT()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Default value not null", SpecificTypeStrategies.LEAD_LAG)
                        .inputTypes(
                                DataTypes.STRING(),
                                DataTypes.BIGINT(),
                                DataTypes.STRING().notNull())
                        .expectDataType(DataTypes.STRING().notNull()),
                TypeStrategiesTestBase.TestSpec.forStrategy(
                                "Default value nullable", SpecificTypeStrategies.LEAD_LAG)
                        .inputTypes(DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING())
                        .expectDataType(DataTypes.STRING()));
    }
}
