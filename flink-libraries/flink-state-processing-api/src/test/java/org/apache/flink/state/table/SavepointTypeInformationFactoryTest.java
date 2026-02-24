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

package org.apache.flink.state.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the SavepointTypeInformationFactory. */
class SavepointTypeInformationFactoryTest {

    public static class TestLongTypeInformationFactory implements SavepointTypeInformationFactory {
        private static volatile boolean wasCalled = false;

        public static boolean wasFactoryCalled() {
            return wasCalled;
        }

        public static void resetCallTracker() {
            wasCalled = false;
        }

        @Override
        public TypeInformation<?> getTypeInformation() {
            wasCalled = true;
            return TypeInformation.of(Long.class);
        }
    }

    private static class TestStringTypeInformationFactory
            implements SavepointTypeInformationFactory {
        @Override
        public TypeInformation<?> getTypeInformation() {
            return TypeInformation.of(String.class);
        }
    }

    @Test
    void testSavepointTypeInformationFactoryEndToEnd() throws Exception {
        TestLongTypeInformationFactory.resetCallTracker();

        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final String sql =
                "CREATE TABLE state_table (\n"
                        + "  k bigint,\n"
                        + "  KeyedPrimitiveValue bigint,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")\n"
                        + "with (\n"
                        + "  'connector' = 'savepoint',\n"
                        + "  'state.path' = 'src/test/resources/table-state',\n"
                        + "  'operator.uid' = 'keyed-state-process-uid',\n"
                        + "  'fields.KeyedPrimitiveValue.value-type-factory' = '"
                        + TestLongTypeInformationFactory.class.getName()
                        + "'\n"
                        + ")";

        tEnv.executeSql(sql);
        Table table = tEnv.sqlQuery("SELECT k, KeyedPrimitiveValue FROM state_table");
        List<Row> result = tEnv.toDataStream(table).executeAndCollect(100);

        assertThat(TestLongTypeInformationFactory.wasFactoryCalled())
                .as(
                        "Factory getTypeInformation() method must be called - this proves factory is used instead of metadata inference")
                .isTrue();

        assertThat(result).hasSize(10);

        Set<Long> keys =
                result.stream().map(r -> (Long) r.getField("k")).collect(Collectors.toSet());
        assertThat(keys).hasSize(10);
        assertThat(keys).containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);

        Set<Long> primitiveValues =
                result.stream()
                        .map(r -> (Long) r.getField("KeyedPrimitiveValue"))
                        .collect(Collectors.toSet());
        assertThat(primitiveValues).containsExactly(1L);
    }

    @Test
    void testBasicFactoryFunctionality() {
        TestLongTypeInformationFactory.resetCallTracker();

        TestLongTypeInformationFactory longFactory = new TestLongTypeInformationFactory();
        TypeInformation<?> longTypeInfo = longFactory.getTypeInformation();

        assertThat(longTypeInfo).isEqualTo(TypeInformation.of(Long.class));
        assertThat(TestLongTypeInformationFactory.wasFactoryCalled()).isTrue();

        TestStringTypeInformationFactory stringFactory = new TestStringTypeInformationFactory();
        TypeInformation<?> stringTypeInfo = stringFactory.getTypeInformation();

        assertThat(stringTypeInfo).isEqualTo(TypeInformation.of(String.class));
    }
}
