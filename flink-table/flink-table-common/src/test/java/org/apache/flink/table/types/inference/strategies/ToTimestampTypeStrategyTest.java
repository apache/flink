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

/** Tests for {@link ToTimestampTypeStrategy}. */
class ToTimestampTypeStrategyTest extends TypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // 1-arg: always TIMESTAMP(3)
                TestSpec.forStrategy(
                                "TO_TIMESTAMP(<STRING>) returns TIMESTAMP(3)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING())
                        .expectDataType(DataTypes.TIMESTAMP(3).nullable()),
                // 2-arg: non-literal format defaults to TIMESTAMP(3)
                TestSpec.forStrategy(
                                "TO_TIMESTAMP(<STRING>, <STRING>) defaults to TIMESTAMP(3)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .expectDataType(DataTypes.TIMESTAMP(3).nullable()),
                // Format-based precision: SS → TIMESTAMP(3)
                TestSpec.forStrategy(
                                "Format with SS returns TIMESTAMP(3)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SS")
                        .expectDataType(DataTypes.TIMESTAMP(3).nullable()),

                // Format-based precision: SSS → TIMESTAMP(3)
                TestSpec.forStrategy(
                                "Format with SSS returns TIMESTAMP(3)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SSS")
                        .expectDataType(DataTypes.TIMESTAMP(3).nullable()),
                // Format-based precision: SSSSSS → TIMESTAMP(6)
                TestSpec.forStrategy(
                                "Format with SSSSSS returns TIMESTAMP(6)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SSSSSS")
                        .expectDataType(DataTypes.TIMESTAMP(6).nullable()),
                // Format-based precision: SSSSSSS → TIMESTAMP(7)
                TestSpec.forStrategy(
                                "Format with SSSSSSS returns TIMESTAMP(7)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SSSSSSS")
                        .expectDataType(DataTypes.TIMESTAMP(7).nullable()),
                // Format-based precision: SSSSSSSSS → TIMESTAMP(9)
                TestSpec.forStrategy(
                                "Format with SSSSSSSSS returns TIMESTAMP(9)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
                        .expectDataType(DataTypes.TIMESTAMP(9).nullable()),
                // Format without S → TIMESTAMP(3)
                TestSpec.forStrategy(
                                "Format without S returns TIMESTAMP(3)",
                                SpecificTypeStrategies.TO_TIMESTAMP)
                        .inputTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "yyyy-MM-dd HH:mm:ss")
                        .expectDataType(DataTypes.TIMESTAMP(3).nullable()));
    }
}
