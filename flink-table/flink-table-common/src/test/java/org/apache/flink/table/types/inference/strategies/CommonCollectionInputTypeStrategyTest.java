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
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;

import java.util.stream.Stream;

/** Tests for {@link CommonCollectionInputTypeStrategy}. */
class CommonCollectionInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(InputTypeStrategies.commonMapType(2))
                        .expectSignature("f(<COMMON>, <COMMON>...)")
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.DOUBLE()))
                        .expectArgumentTypes(
                                DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.DOUBLE()),
                                DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.DOUBLE())),
                TestSpec.forStrategy(
                                "Strategy fails if can not find a common type",
                                InputTypeStrategies.commonMapType(2))
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.INT()))
                        .expectErrorMessage(
                                "Could not find a common type for arguments: [MAP<STRING, INT>, MAP<INT, INT>]"),
                TestSpec.forStrategy(
                                "Strategy fails if not all of the argument types are MAP",
                                InputTypeStrategies.commonMapType(2))
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .expectErrorMessage("All arguments requires to be a MAP type"));
    }
}
