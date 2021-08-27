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
import org.apache.flink.table.types.inference.strategies.SubsequenceInputTypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.runners.Parameterized;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.commonType;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicit;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;

/** Tests for {@link SubsequenceInputTypeStrategy}. */
public class SubsequenceInputTypeStrategyTest extends InputTypeStrategiesTestBase {
    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return asList(
                TestSpec.forStrategy(
                                "A strategy used for IF ELSE with valid arguments",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .finish())
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(), DataTypes.SMALLINT(), DataTypes.DECIMAL(10, 2))
                        .expectSignature("f(<BOOLEAN>, <COMMON>, <COMMON>)")
                        .expectArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.DECIMAL(10, 2)),
                TestSpec.forStrategy(
                                "Strategy fails if any of the nested strategies fail",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .finish())
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(), DataTypes.VARCHAR(3), DataTypes.DECIMAL(10, 2))
                        .expectErrorMessage(
                                "Could not find a common type for arguments: [VARCHAR(3), DECIMAL(10, 2)]"),
                TestSpec.forStrategy(
                                "Strategy with a varying argument",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .finishWithVarying(
                                                varyingSequence(logical(LogicalTypeRoot.BIGINT))))
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.SMALLINT(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT(),
                                DataTypes.TINYINT())
                        .expectSignature("f(<BOOLEAN>, <COMMON>, <COMMON>, <BIGINT>...)")
                        .expectArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT()),
                TestSpec.forStrategy(
                                "A complex strategy with few sub sequences",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .argument(explicit(DataTypes.TIME().notNull()))
                                        .subsequence(commonType(2))
                                        .finishWithVarying(
                                                varyingSequence(
                                                        logical(LogicalTypeFamily.TIMESTAMP), ANY)))
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.SMALLINT(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.TIME().notNull(),
                                DataTypes.TINYINT().notNull(),
                                DataTypes.DECIMAL(13, 3).notNull(),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT())
                        .expectSignature(
                                "f(<BOOLEAN>, <COMMON>, <COMMON>, TIME(0) NOT NULL, <COMMON>, <COMMON>, <TIMESTAMP>, <ANY>...)")
                        .expectArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.TIME().notNull(),
                                DataTypes.DECIMAL(13, 3).notNull(),
                                DataTypes.DECIMAL(13, 3).notNull(),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT()));
    }
}
