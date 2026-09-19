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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrayOfEntriesArgumentTypeStrategy}. */
class ArrayOfEntriesArgumentTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final InputTypeStrategy MAP_FROM_ENTRIES_INPUT_STRATEGY =
            BuiltInFunctionDefinitions.MAP_FROM_ENTRIES
                    .getTypeInference(new DataTypeFactoryMock())
                    .getInputTypeStrategy();

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(
                                "Array of two-field rows is accepted",
                                MAP_FROM_ENTRIES_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.INT()),
                                                DataTypes.FIELD("value", DataTypes.STRING()))))
                        .expectSignature("f(input ARRAY<ROW<key, value>>)")
                        .expectArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.INT()),
                                                DataTypes.FIELD("value", DataTypes.STRING())))),
                TestSpec.forStrategy(
                                "Nested and NOT NULL element types are preserved",
                                MAP_FROM_ENTRIES_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "key", DataTypes.STRING()),
                                                                DataTypes.FIELD(
                                                                        "value",
                                                                        DataTypes.ARRAY(
                                                                                DataTypes.INT())))
                                                        .notNull())
                                        .notNull())
                        .expectArgumentTypes(
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "key", DataTypes.STRING()),
                                                                DataTypes.FIELD(
                                                                        "value",
                                                                        DataTypes.ARRAY(
                                                                                DataTypes.INT())))
                                                        .notNull())
                                        .notNull()),
                TestSpec.forStrategy(
                                "Non-array argument is rejected", MAP_FROM_ENTRIES_INPUT_STRATEGY)
                        .calledWithArgumentTypes(DataTypes.STRING())
                        .expectErrorMessage("The 'input' argument must be ARRAY<ROW<key, value>>"),
                TestSpec.forStrategy(
                                "Array of non-row elements is rejected",
                                MAP_FROM_ENTRIES_INPUT_STRATEGY)
                        .calledWithArgumentTypes(DataTypes.ARRAY(DataTypes.INT()))
                        .expectErrorMessage(
                                "The 'input' argument must be ARRAY<ROW<key, value>>, but the array "
                                        + "element type was 'INT'. The element must be a ROW with "
                                        + "exactly two fields."),
                TestSpec.forStrategy(
                                "Array of rows without exactly two fields is rejected",
                                MAP_FROM_ENTRIES_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.INT()),
                                                DataTypes.FIELD("value", DataTypes.STRING()),
                                                DataTypes.FIELD("extra", DataTypes.BOOLEAN()))))
                        .expectErrorMessage("The element must be a ROW with exactly two fields."),
                TestSpec.forStrategy(
                                "Key type without equality support is rejected",
                                MAP_FROM_ENTRIES_INPUT_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "key",
                                                        DataTypes.RAW(
                                                                Object.class,
                                                                new KryoSerializer<>(
                                                                        Object.class,
                                                                        new SerializerConfigImpl()))),
                                                DataTypes.FIELD("value", DataTypes.STRING()))))
                        .expectErrorMessage(
                                "does not support equality comparison and therefore cannot be "
                                        + "used as the first field of a map entry."));
    }

    @Test
    void testEqualsAndHashCode() {
        final ArgumentTypeStrategy strategy = new ArrayOfEntriesArgumentTypeStrategy();
        assertThat(strategy)
                .isEqualTo(strategy)
                .isEqualTo(new ArrayOfEntriesArgumentTypeStrategy())
                .hasSameHashCodeAs(new ArrayOfEntriesArgumentTypeStrategy())
                .isNotEqualTo("not a strategy");
    }
}
