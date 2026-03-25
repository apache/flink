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

/** Tests for {@link ToTimestampLtzTypeStrategy}. */
class ToTimestampLtzTypeStrategyTest extends TypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Single argument: defaults to TIMESTAMP_LTZ(3)
                TestSpec.forStrategy(
                                "Valid single argument of type <VARCHAR> or <CHAR>",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING())
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>)",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT())
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "Invalid single argument type",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BOOLEAN())
                        .expectErrorMessage(
                                "Unsupported argument type. When taking 1 argument, TO_TIMESTAMP_LTZ accepts an argument of type <VARCHAR>, <CHAR>, or <NUMERIC>."),
                // Two arguments without literal: defaults to TIMESTAMP_LTZ(3)
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>)",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.DOUBLE(), DataTypes.INT())
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "Valid two arguments of <VARCHAR> or <CHAR>",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "Invalid second argument when the first argument is <NUMERIC>",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.STRING())
                        .expectErrorMessage(
                                "Unsupported argument type. TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) requires the second argument to be <INTEGER>."),
                TestSpec.forStrategy(
                                "Invalid second argument when the first argument is <VARCHAR> or <CHAR>",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.FLOAT())
                        .expectErrorMessage(
                                "Unsupported argument type. If the first argument is of type <VARCHAR> or <CHAR>, TO_TIMESTAMP_LTZ requires the second argument to be of type <VARCHAR> or <CHAR>."),
                TestSpec.forStrategy(
                                "Invalid first argument when taking 2 arguments",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BOOLEAN(), DataTypes.FLOAT())
                        .expectErrorMessage(
                                "Unsupported argument type. When taking 2 arguments, TO_TIMESTAMP_LTZ requires the first argument to be of type <VARCHAR>, <CHAR>, or <NUMERIC>."),
                // Three arguments: defaults to TIMESTAMP_LTZ(3)
                TestSpec.forStrategy(
                                "Valid three arguments", SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "Invalid three arguments", SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING())
                        .expectErrorMessage(
                                "Unsupported argument type. When taking 3 arguments, TO_TIMESTAMP_LTZ requires all three arguments to be of type <VARCHAR> or <CHAR>."),
                TestSpec.forStrategy("No arguments", SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes()
                        .expectErrorMessage(
                                "Unsupported argument type. TO_TIMESTAMP_LTZ requires 1 to 3 arguments, but 0 were provided."),
                TestSpec.forStrategy("Too many arguments", SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())
                        .expectErrorMessage(
                                "Unsupported argument type. TO_TIMESTAMP_LTZ requires 1 to 3 arguments, but 4 were provided."),
                // Precision 0-3: clamped to TIMESTAMP_LTZ(3)
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) with precision 0",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.INT())
                        .calledWithLiteralAt(1, 0)
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) with precision 3",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.INT())
                        .calledWithLiteralAt(1, 3)
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                // Precision 4-9: follows input precision
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) with precision 4",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.INT())
                        .calledWithLiteralAt(1, 4)
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(4).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) with precision 6",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.INT())
                        .calledWithLiteralAt(1, 6)
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(6).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) with precision 9",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.INT())
                        .calledWithLiteralAt(1, 9)
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(9).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<DOUBLE>, <INTEGER>) with precision 9",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.DOUBLE(), DataTypes.INT())
                        .calledWithLiteralAt(1, 9)
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(9).nullable()),
                // Out of range
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) with precision out of range",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.INT())
                        .calledWithLiteralAt(1, 10)
                        .expectErrorMessage(
                                "Precision for TO_TIMESTAMP_LTZ must be between 0 and 9 but was 10."),
                // Format-based precision inference for string variants
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<STRING>, <STRING>) with no S in format returns TIMESTAMP_LTZ(3)",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss")
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<STRING>, <STRING>) with SSS format returns TIMESTAMP_LTZ(3)",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SSS")
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(3).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<STRING>, <STRING>) with SSSSSS format returns TIMESTAMP_LTZ(6)",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SSSSSS")
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(6).nullable()),
                TestSpec.forStrategy(
                                "TO_TIMESTAMP_LTZ(<STRING>, <STRING>) with SSSSSSSSS format returns TIMESTAMP_LTZ(9)",
                                SpecificTypeStrategies.TO_TIMESTAMP_LTZ)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
                        .expectDataType(DataTypes.TIMESTAMP_LTZ(9).nullable()));
    }
}
