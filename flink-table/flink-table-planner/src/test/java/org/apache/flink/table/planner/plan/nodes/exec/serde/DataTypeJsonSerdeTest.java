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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link DataType} serialization and deserialization. */
@Execution(CONCURRENT)
public class DataTypeJsonSerdeTest {

    @ParameterizedTest
    @MethodSource("testDataTypeSerde")
    public void testDataTypeSerde(DataType dataType) throws IOException {
        final SerdeContext serdeContext = configuredSerdeContext();
        final String json = toJson(serdeContext, dataType);
        final DataType actual = toObject(serdeContext, json, DataType.class);

        assertThat(actual).isEqualTo(dataType);
    }

    // --------------------------------------------------------------------------------------------
    // Test data
    // --------------------------------------------------------------------------------------------

    private static Stream<DataType> testDataTypeSerde() {
        return Stream.of(
                DataTypes.INT(),
                DataTypes.INT().notNull().bridgedTo(int.class),
                DataTypes.TIMESTAMP_LTZ(3).toInternal(),
                DataTypes.TIMESTAMP_LTZ(9).bridgedTo(long.class),
                DataTypes.ROW(
                        DataTypes.TIMESTAMP_LTZ(3).toInternal(),
                        DataTypes.TIMESTAMP_LTZ(9).bridgedTo(long.class),
                        DataTypes.ROW(
                                DataTypes.INT(),
                                DataTypes.MULTISET(
                                        DataTypes.DOUBLE().notNull().bridgedTo(double.class)))),
                DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class),
                DataTypes.STRUCTURED(
                        PojoClass.class,
                        DataTypes.FIELD("f0", DataTypes.INT().notNull().bridgedTo(int.class)),
                        DataTypes.FIELD("f1", DataTypes.BIGINT().notNull().bridgedTo(long.class)),
                        DataTypes.FIELD("f2", DataTypes.STRING())),
                DataTypes.MAP(DataTypes.STRING().toInternal(), DataTypes.TIMESTAMP(3)),
                DataTypes.ROW(DataTypes.TIMESTAMP_LTZ(3)).toInternal());
    }

    // --------------------------------------------------------------------------------------------
    // Helper POJOs
    // --------------------------------------------------------------------------------------------

    /** Testing class. */
    public static class PojoClass {
        public int f0;
        public long f1;
        public String f2;
    }
}
