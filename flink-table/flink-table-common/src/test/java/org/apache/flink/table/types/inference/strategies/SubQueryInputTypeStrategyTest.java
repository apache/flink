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

/** Tests for {@link SubQueryInputTypeStrategy}. */
class SubQueryInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy("IN a set", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.INT())
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT()),
                TestSpec.forStrategy("IN a set, binary", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES())
                        .expectArgumentTypes(
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES()),
                TestSpec.forStrategy("IN a set, string", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())
                        .expectArgumentTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING()),
                TestSpec.forStrategy(
                                "IN a set, multiset(timestamp)", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.MULTISET(DataTypes.TIMESTAMP()),
                                DataTypes.MULTISET(DataTypes.TIMESTAMP()),
                                DataTypes.MULTISET(DataTypes.TIMESTAMP()),
                                DataTypes.MULTISET(DataTypes.TIMESTAMP()))
                        .expectArgumentTypes(
                                DataTypes.MULTISET(DataTypes.TIMESTAMP()),
                                DataTypes.MULTISET(DataTypes.TIMESTAMP()),
                                DataTypes.MULTISET(DataTypes.TIMESTAMP()),
                                DataTypes.MULTISET(DataTypes.TIMESTAMP())),
                TestSpec.forStrategy("IN a set, arrays", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.BIGINT()),
                                DataTypes.ARRAY(DataTypes.BIGINT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.SMALLINT()))
                        .expectArgumentTypes(
                                DataTypes.ARRAY(DataTypes.BIGINT()),
                                DataTypes.ARRAY(DataTypes.BIGINT()),
                                DataTypes.ARRAY(DataTypes.BIGINT()),
                                DataTypes.ARRAY(DataTypes.BIGINT())),
                TestSpec.forStrategy("IN a set of ROWs", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())),
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())))
                        .expectArgumentTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())),
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT()))),
                TestSpec.forStrategy("IN a subquery", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT())))
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT()))),
                TestSpec.forStrategy("IN a set not comparable", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING())
                        .expectErrorMessage(
                                "Types on the right side of IN operator (STRING) are not comparable with INT."),
                TestSpec.forStrategy("IN a subquery not comparable", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.STRING())))
                        .expectErrorMessage(
                                "Types on the right side of IN operator (ROW<`f0` STRING>) are not comparable with INT"),
                TestSpec.forStrategy("IN a subquery of ROWs", SpecificInputTypeStrategies.IN)
                        .calledWithArgumentTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())),
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "f0",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("f0", DataTypes.INT())))))
                        .expectArgumentTypes(
                                DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())),
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "f0",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("f0", DataTypes.INT()))))));
    }
}
