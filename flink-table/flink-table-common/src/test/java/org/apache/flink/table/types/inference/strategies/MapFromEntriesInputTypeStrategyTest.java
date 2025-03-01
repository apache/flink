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
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;

import java.util.stream.Stream;

/** Tests for {@link MapFromEntriesInputTypeStrategy}. */
class MapFromEntriesInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(SpecificInputTypeStrategies.MAP_FROM_ENTRIES)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.INT(), DataTypes.STRING())))
                        .expectSignature("f(ARRAY<ROW<`f0` ANY, `f1` ANY>>)")
                        .expectArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.INT()),
                                                DataTypes.FIELD("f1", DataTypes.STRING())))),
                TestSpec.forStrategy(
                                "ARRAY<ROW<`expected` INT>> doesn't work",
                                SpecificInputTypeStrategies.MAP_FROM_ENTRIES)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("expected", DataTypes.INT()))))
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type 'ARRAY<ROW<`f0` ANY, `f1` ANY>>' but actual type was 'ARRAY<ROW<`expected` INT>>'."));
    }
}
