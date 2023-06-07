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

/** Tests for {@link ArrayComparableElementTypeStrategy}. */
public class ArrayComparableElementTypeStrategyTest extends InputTypeStrategiesTestBase {
    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(InputTypeStrategies.arrayFullyComparableElementType())
                        .expectSignature("f(<ARRAY<COMPARABLE>>)")
                        .calledWithArgumentTypes(DataTypes.ARRAY(DataTypes.ROW()))
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "f(<ARRAY<COMPARABLE>>)"),
                TestSpec.forStrategy(
                                "Strategy fails if input argument type is not ARRAY",
                                InputTypeStrategies.arrayFullyComparableElementType())
                        .calledWithArgumentTypes(DataTypes.INT())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "f(<ARRAY<COMPARABLE>>)"),
                TestSpec.forStrategy(
                                "Strategy fails if the number of input arguments are not one",
                                InputTypeStrategies.arrayFullyComparableElementType())
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "f(<ARRAY<COMPARABLE>>)"));
    }
}
