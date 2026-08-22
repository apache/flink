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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;

import java.util.stream.Stream;

/** Tests for {@link SpecificTypeStrategies#MAP_FROM_ENTRIES}. */
class MapFromEntriesTypeStrategyTest extends TypeStrategiesTestBase {

    private static DataType entry(boolean elementNullable) {
        final DataType entryType =
                DataTypes.ROW(
                        DataTypes.FIELD("key", DataTypes.INT()),
                        DataTypes.FIELD("value", DataTypes.STRING()));
        return elementNullable ? entryType.nullable() : entryType.notNull();
    }

    private static DataType array(boolean arrayNullable, boolean elementNullable) {
        final DataType arrayType = DataTypes.ARRAY(entry(elementNullable));
        return arrayNullable ? arrayType.nullable() : arrayType.notNull();
    }

    private static final DataType MAP_TYPE = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(
                                "A nullable array yields a nullable map",
                                SpecificTypeStrategies.MAP_FROM_ENTRIES)
                        .inputTypes(array(true, false))
                        .expectDataType(MAP_TYPE.nullable()),
                TestSpec.forStrategy(
                                "A nullable entry yields a nullable map",
                                SpecificTypeStrategies.MAP_FROM_ENTRIES)
                        .inputTypes(array(false, true))
                        .expectDataType(MAP_TYPE.nullable()),
                TestSpec.forStrategy(
                                "NOT NULL array and entries yield a NOT NULL map, "
                                        + "carrying field nullability into the map",
                                SpecificTypeStrategies.MAP_FROM_ENTRIES)
                        .inputTypes(
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "key",
                                                                        DataTypes.INT().notNull()),
                                                                DataTypes.FIELD(
                                                                        "value",
                                                                        DataTypes.STRING()
                                                                                .notNull()))
                                                        .notNull())
                                        .notNull())
                        .expectDataType(
                                DataTypes.MAP(
                                                DataTypes.INT().notNull(),
                                                DataTypes.STRING().notNull())
                                        .notNull()),
                TestSpec.forStrategy(
                                "Nested value type is preserved",
                                SpecificTypeStrategies.MAP_FROM_ENTRIES)
                        .inputTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "value",
                                                        DataTypes.ARRAY(DataTypes.INT())))))
                        .expectDataType(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))
                                        .nullable()));
    }
}
