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
import org.apache.flink.table.types.inference.TypeStrategy;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/** Tests for decimal {@link TypeStrategy TypeStrategies}. */
public class DecimalTypeStrategyTest extends TypeStrategiesTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forStrategy("Find a decimal sum", SpecificTypeStrategies.DECIMAL_PLUS)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(6, 4).notNull()),
                TestSpec.forStrategy(
                                "Find a decimal quotient", SpecificTypeStrategies.DECIMAL_DIVIDE)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(11, 8).notNull()),
                TestSpec.forStrategy("Find a decimal product", SpecificTypeStrategies.DECIMAL_TIMES)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(9, 6).notNull()),
                TestSpec.forStrategy("Find a decimal modulo", SpecificTypeStrategies.DECIMAL_MOD)
                        .inputTypes(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(3, 2))
                        .expectDataType(DataTypes.DECIMAL(5, 4).notNull()));
    }
}
