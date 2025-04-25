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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the savepoint SQL reader. */
public class SavepointDynamicTableSourceTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testReadKeyedState() throws Exception {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final String sql =
                "CREATE TABLE state_table (\n"
                        + "  k bigint,\n"
                        + "  KeyedPrimitiveValue bigint,\n"
                        + "  KeyedPojoValue ROW<privateLong bigint, publicLong bigint>,\n"
                        + "  KeyedPrimitiveValueList ARRAY<bigint>,\n"
                        + "  KeyedPrimitiveValueMap MAP<bigint, bigint>,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")\n"
                        + "with (\n"
                        + "  'connector' = 'savepoint',\n"
                        + "  'state.path' = 'src/test/resources/table-state',\n"
                        + "  'operator.uid' = 'keyed-state-process-uid',\n"
                        + "  'fields.KeyedPojoValue.value-format' = 'com.example.state.writer.job.schema.PojoData'\n"
                        + ")";
        tEnv.executeSql(sql);
        Table table = tEnv.sqlQuery("SELECT * FROM state_table");
        List<Row> result = tEnv.toDataStream(table).executeAndCollect(100);

        assertThat(result.size()).isEqualTo(10);

        // Check key
        List<Long> keys =
                result.stream().map(r -> (Long) r.getField("k")).collect(Collectors.toList());
        List<Long> expectedKeys = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        assertThat(keys).containsExactlyInAnyOrderElementsOf(expectedKeys);

        // Check primitive value state
        Set<Long> primitiveValues =
                result.stream()
                        .map(r -> (Long) r.getField("KeyedPrimitiveValue"))
                        .collect(Collectors.toSet());
        assertThat(primitiveValues.size()).isEqualTo(1);
        assertThat(primitiveValues.iterator().next()).isEqualTo(1L);

        // Check pojo value state
        Set<Row> pojoValues =
                result.stream()
                        .map(r -> (Row) r.getField("KeyedPojoValue"))
                        .collect(Collectors.toSet());
        assertThat(pojoValues.size()).isEqualTo(1);
        Row pojoData = pojoValues.iterator().next();
        assertThat(pojoData.getField("publicLong")).isEqualTo(1L);
        assertThat(pojoData.getField("privateLong")).isEqualTo(1L);

        // Check list state
        Set<Tuple2<Long, Long[]>> listValues =
                result.stream()
                        .map(
                                r ->
                                        Tuple2.of(
                                                (Long) r.getField("k"),
                                                (Long[]) r.getField("KeyedPrimitiveValueList")))
                        .flatMap(l -> Set.of(l).stream())
                        .collect(Collectors.toSet());
        assertThat(listValues.size()).isEqualTo(10);
        for (Tuple2<Long, Long[]> tuple2 : listValues) {
            assertThat(tuple2.f0).isEqualTo(tuple2.f1[0]);
        }

        // Check map state
        Set<Tuple2<Long, Map<Long, Long>>> mapValues =
                result.stream()
                        .map(
                                r ->
                                        Tuple2.of(
                                                (Long) r.getField("k"),
                                                (Map<Long, Long>)
                                                        r.getField("KeyedPrimitiveValueMap")))
                        .flatMap(l -> Set.of(l).stream())
                        .collect(Collectors.toSet());
        assertThat(mapValues.size()).isEqualTo(10);
        for (Tuple2<Long, Map<Long, Long>> tuple2 : mapValues) {
            assertThat(tuple2.f1.size()).isEqualTo(1);
            assertThat(tuple2.f0).isEqualTo(tuple2.f1.get(tuple2.f0));
        }
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    public void testReadKeyedStateWithNullValues() throws Exception {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final String sql =
                "CREATE TABLE state_table (\n"
                        + "  k bigint,\n"
                        + "  total ROW<privateLong bigint, publicLong bigint>,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")\n"
                        + "with (\n"
                        + "  'connector' = 'savepoint',\n"
                        + "  'state.path' = 'src/test/resources/table-state-nulls',\n"
                        + "  'operator.uid' = 'keyed-state-process-uid-null',\n"
                        + "  'fields.total.value-format' = 'com.example.state.writer.job.schema.PojoData'\n"
                        + ")";
        tEnv.executeSql(sql);
        Table table = tEnv.sqlQuery("SELECT * FROM state_table");
        List<Row> result = tEnv.toDataStream(table).executeAndCollect(100);
        assertThat(result.size()).isEqualTo(5);

        List<Long> keys =
                result.stream().map(row -> (Long) row.getField("k")).collect(Collectors.toList());
        assertThat(keys).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);

        // Check pojo value state
        Map<Long, Row> pojoValues =
                result.stream()
                        .collect(
                                Collectors.toMap(
                                        v -> (Long) v.getField("k"),
                                        v -> (Row) v.getField("total")));
        assertThat(pojoValues.get(1L)).isEqualTo(Row.of(1L, 1L));
        assertThat(pojoValues.get(2L)).isEqualTo(Row.of(null, null));
        assertThat(pojoValues.get(3L)).isEqualTo(Row.of(null, null));
        assertThat(pojoValues.get(4L)).isEqualTo(Row.of(4L, 4L));
        assertThat(pojoValues.get(5L)).isEqualTo(Row.of(5L, 5L));
    }
}
