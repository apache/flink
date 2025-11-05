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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThatList;

/** ITCase for async VECTOR_SEARCH. */
@ExtendWith(ParameterizedTestExtension.class)
public class AsyncVectorSearchITCase extends StreamingWithStateTestBase {

    public AsyncVectorSearchITCase(StateBackendMode state) {
        super(state);
    }

    private final List<Row> data =
            Arrays.asList(
                    Row.of(1L, new Float[] {5f, 12f, 13f}),
                    Row.of(2L, new Float[] {11f, 60f, 61f}),
                    Row.of(3L, new Float[] {8f, 15f, 17f}));

    private final List<Row> nullableData =
            Arrays.asList(Row.of(1L, new Float[] {5f, 12f, 13f}), Row.of(4L, null));

    @BeforeEach
    public void before() {
        super.before();
        createTable("src", data);
        createTable("nullableSrc", nullableData);
        createTable("vector", data);
    }

    @TestTemplate
    void testSimple() {
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT * FROM src, LATERAL TABLE(VECTOR_SEARCH(TABLE vector, DESCRIPTOR(`vector`), src.vector, 2))")
                                .collect());
        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                1.0),
                        Row.of(
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                3L,
                                new Float[] {8f, 15f, 17f},
                                0.9977375565610862),
                        Row.of(
                                2L,
                                new Float[] {11f, 60f, 61f},
                                2L,
                                new Float[] {11f, 60f, 61f},
                                1.0),
                        Row.of(
                                2L,
                                new Float[] {11f, 60f, 61f},
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                0.9886506935687265),
                        Row.of(
                                3L,
                                new Float[] {8f, 15f, 17f},
                                3L,
                                new Float[] {8f, 15f, 17f},
                                1.0000000000000002),
                        Row.of(
                                3L,
                                new Float[] {8f, 15f, 17f},
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                0.9977375565610862));
    }

    @TestTemplate
    void testLeftLateralJoin() {
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT * FROM nullableSrc LEFT JOIN LATERAL TABLE(VECTOR_SEARCH(TABLE vector, DESCRIPTOR(`vector`), nullableSrc.vector, 2)) ON TRUE")
                                .collect());
        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                1.0),
                        Row.of(
                                1L,
                                new Float[] {5.0f, 12.0f, 13.0f},
                                3L,
                                new Float[] {8f, 15f, 17f},
                                0.9977375565610862),
                        Row.of(4L, null, null, null, null));
    }

    @TestTemplate
    void testTimeout() {
        tEnv().getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_VECTOR_SEARCH_TIMEOUT,
                        Duration.ofMillis(100));
        assertThatThrownBy(
                        () ->
                                CollectionUtil.iteratorToList(
                                        tEnv().executeSql(
                                                        "SELECT * FROM nullableSrc LEFT JOIN LATERAL TABLE(VECTOR_SEARCH(TABLE vector, DESCRIPTOR(`vector`), nullableSrc.vector, 2)) ON TRUE")
                                                .collect()))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TimeoutException.class, "Async function call has timed out."));
    }

    @TestTemplate
    void testConstantValue() {
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT * FROM TABLE(VECTOR_SEARCH(TABLE vector, DESCRIPTOR(`vector`), ARRAY[5, 12, 13], 2))")
                                .collect());
        assertThat(actual)
                .containsExactlyInAnyOrder(
                        Row.of(1L, new Float[] {5.0f, 12.0f, 13.0f}, 1.0),
                        Row.of(3L, new Float[] {8f, 15f, 17f}, 0.9977375565610862));
    }

    @TestTemplate
    void testVectorSearchWithCalc() {
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "SELECT * FROM nullableSrc\n "
                                                        + "LEFT JOIN LATERAL TABLE(VECTOR_SEARCH((SELECT id+1, vector FROM vector), DESCRIPTOR(`vector`), nullableSrc.vector, 2)) ON TRUE"))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Don't support calc on VECTOR_SEARCH node now."));
    }

    @TestTemplate
    void testRuntimeConfig() {
        assertThatThrownBy(
                        () ->
                                CollectionUtil.iteratorToList(
                                        tEnv().executeSql(
                                                        "SELECT * FROM nullableSrc LEFT JOIN LATERAL TABLE(VECTOR_SEARCH(TABLE vector, DESCRIPTOR(`vector`), nullableSrc.vector, 2, MAP['timeout', '100ms'])) ON TRUE")
                                                .collect()))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TimeoutException.class, "Async function call has timed out."));
    }

    @Parameters(name = "backend = {0}, objectReuse = {1}, asyncOutputMode = {2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {
                        StreamingWithStateTestBase.HEAP_BACKEND(),
                        true,
                        ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED
                    },
                    {
                        StreamingWithStateTestBase.HEAP_BACKEND(),
                        true,
                        ExecutionConfigOptions.AsyncOutputMode.ORDERED
                    },
                    {
                        StreamingWithStateTestBase.HEAP_BACKEND(),
                        false,
                        ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED
                    },
                    {
                        StreamingWithStateTestBase.HEAP_BACKEND(),
                        false,
                        ExecutionConfigOptions.AsyncOutputMode.ORDERED
                    },
                    {
                        StreamingWithStateTestBase.ROCKSDB_BACKEND(),
                        true,
                        ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED
                    },
                    {
                        StreamingWithStateTestBase.ROCKSDB_BACKEND(),
                        true,
                        ExecutionConfigOptions.AsyncOutputMode.ORDERED
                    },
                    {
                        StreamingWithStateTestBase.ROCKSDB_BACKEND(),
                        false,
                        ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED
                    },
                    {
                        StreamingWithStateTestBase.ROCKSDB_BACKEND(),
                        false,
                        ExecutionConfigOptions.AsyncOutputMode.ORDERED
                    }
                });
    }

    private void createTable(String tableName, List<Row> data) {
        String dataId = TestValuesTableFactory.registerData(data);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE `%s`(\n"
                                        + "  id BIGINT,\n"
                                        + "  vector ARRAY<FLOAT>\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'enable-vector-search' = 'true',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'async' = 'true',\n"
                                        + "  'latency' = '1000'"
                                        + ")",
                                tableName, dataId));
    }
}
