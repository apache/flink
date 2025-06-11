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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesModelFactory;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for async ML_PREDICT. */
@ExtendWith(ParameterizedTestExtension.class)
public class AsyncMLPredictITCase extends StreamingWithStateTestBase {

    private final Boolean objectReuse;
    private final ExecutionConfigOptions.AsyncOutputMode asyncOutputMode;

    public AsyncMLPredictITCase(
            StateBackendMode backend,
            Boolean objectReuse,
            ExecutionConfigOptions.AsyncOutputMode asyncOutputMode) {
        super(backend);

        this.objectReuse = objectReuse;
        this.asyncOutputMode = asyncOutputMode;
    }

    private final List<Row> data =
            Arrays.asList(
                    Row.of(1L, 12, "Julian"),
                    Row.of(2L, 15, "Hello"),
                    Row.of(3L, 15, "Fabian"),
                    Row.of(8L, 11, "Hello world"),
                    Row.of(9L, 12, "Hello world!"));

    private final List<Row> dataWithNull =
            Arrays.asList(
                    Row.of(15L, null, "Hello"),
                    Row.of(3L, 15, "Fabian"),
                    Row.of(11L, null, "Hello world"),
                    Row.of(9L, 12, "Hello world!"));

    private final Map<Row, List<Row>> id2features = new HashMap<>();

    {
        id2features.put(Row.of(1L), Collections.singletonList(Row.of("x1", 1, "z1")));
        id2features.put(Row.of(2L), Collections.singletonList(Row.of("x2", 2, "z2")));
        id2features.put(Row.of(3L), Collections.singletonList(Row.of("x3", 3, "z3")));
        id2features.put(Row.of(8L), Collections.singletonList(Row.of("x8", 8, "z8")));
        id2features.put(Row.of(9L), Collections.singletonList(Row.of("x9", 9, "z9")));
    }

    private final Map<Row, List<Row>> idLen2features = new HashMap<>();

    {
        idLen2features.put(
                Row.of(15L, null), Collections.singletonList(Row.of("x1", 1, "zNull15")));
        idLen2features.put(Row.of(15L, 15), Collections.singletonList(Row.of("x1", 1, "z1515")));
        idLen2features.put(Row.of(3L, 15), Collections.singletonList(Row.of("x2", 2, "z315")));
        idLen2features.put(
                Row.of(11L, null), Collections.singletonList(Row.of("x3", 3, "zNull11")));
        idLen2features.put(Row.of(11L, 11), Collections.singletonList(Row.of("x3", 3, "z1111")));
        idLen2features.put(Row.of(9L, 12), Collections.singletonList(Row.of("x8", 8, "z912")));
        idLen2features.put(Row.of(12L, 12), Collections.singletonList(Row.of("x8", 8, "z1212")));
    }

    private final Map<Row, List<Row>> content2vector = new HashMap<>();

    {
        content2vector.put(
                Row.of("Julian"),
                Collections.singletonList(Row.of((Object) new Float[] {1.0f, 2.0f, 3.0f})));
        content2vector.put(
                Row.of("Hello"),
                Collections.singletonList(Row.of((Object) new Float[] {2.0f, 3.0f, 4.0f})));
        content2vector.put(
                Row.of("Fabian"),
                Collections.singletonList(Row.of((Object) new Float[] {3.0f, 4.0f, 5.0f})));
    }

    @BeforeEach
    public void before() {
        super.before();
        if (objectReuse) {
            env().getConfig().enableObjectReuse();
        } else {
            env().getConfig().disableObjectReuse();
        }
        tEnv().getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE,
                        asyncOutputMode);

        createScanTable("src", data);
        createScanTable("nullable_src", dataWithNull);

        tEnv().executeSql(
                        String.format(
                                "CREATE MODEL m1\n"
                                        + "INPUT (a BIGINT)\n"
                                        + "OUTPUT (x STRING, y INT, z STRING)\n"
                                        + "WITH (\n"
                                        + "  'provider' = 'values',"
                                        + "  'async' = 'true',"
                                        + "  'data-id' = '%s'"
                                        + ")",
                                TestValuesModelFactory.registerData(id2features)));
        tEnv().executeSql(
                        String.format(
                                "CREATE MODEL m2\n"
                                        + "INPUT (a BIGINT, b INT)\n"
                                        + "OUTPUT (x STRING, y INT, z STRING)\n"
                                        + "WITH (\n"
                                        + "  'provider' = 'values',"
                                        + "  'async' = 'true',"
                                        + "  'data-id' = '%s'"
                                        + ")",
                                TestValuesModelFactory.registerData(idLen2features)));
        tEnv().executeSql(
                        String.format(
                                "CREATE MODEL m3\n"
                                        + "INPUT (content STRING)\n"
                                        + "OUTPUT (vector ARRAY<FLOAT>)\n"
                                        + "WITH (\n"
                                        + "  'provider' = 'values',"
                                        + "  'data-id' = '%s',"
                                        + "  'async' = 'true'"
                                        + ")",
                                TestValuesModelFactory.registerData(content2vector)));
    }

    @TestTemplate
    public void testAsyncMLPredict() {
        assertThatList(
                        CollectionUtil.iteratorToList(
                                tEnv().executeSql(
                                                "SELECT id, z FROM ML_PREDICT(TABLE src, MODEL m1, DESCRIPTOR(`id`))")
                                        .collect()))
                .containsExactlyInAnyOrder(
                        Row.of(1L, "z1"),
                        Row.of(2L, "z2"),
                        Row.of(3L, "z3"),
                        Row.of(8L, "z8"),
                        Row.of(9L, "z9"));
    }

    @TestTemplate
    public void testAsyncMLPredictWithMultipleFields() {
        assertThatList(
                        CollectionUtil.iteratorToList(
                                tEnv().executeSql(
                                                "SELECT id, len, z FROM ML_PREDICT(TABLE nullable_src, MODEL m2, DESCRIPTOR(`id`, `len`))")
                                        .collect()))
                .containsExactlyInAnyOrder(
                        Row.of(3L, 15, "z315"),
                        Row.of(9L, 12, "z912"),
                        Row.of(11L, null, "zNull11"),
                        Row.of(15L, null, "zNull15"));
    }

    @TestTemplate
    public void testAsyncMLPredictWithConstantValues() {
        assertThatList(
                        CollectionUtil.iteratorToList(
                                tEnv().executeSql(
                                                "WITH v(id) AS (SELECT * FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)))) "
                                                        + "SELECT * FROM ML_PREDICT(INPUT => TABLE v, MODEL => MODEL `m1`, ARGS => DESCRIPTOR(`id`))")
                                        .collect()))
                .containsExactlyInAnyOrder(Row.of(1L, "x1", 1, "z1"), Row.of(2L, "x2", 2, "z2"));
    }

    @TestTemplate
    public void testAsyncPredictWithRuntimeConfig() {
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "SELECT id, vector FROM ML_PREDICT(TABLE src, MODEL m3, DESCRIPTOR(`content`), MAP['timeout', '1ms'])")
                                        .await())
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TimeoutException.class, "Async function call has timed out."));
    }

    private void createScanTable(String tableName, List<Row> data) {
        String dataId = TestValuesTableFactory.registerData(data);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE `%s`(\n"
                                        + "  id BIGINT,"
                                        + "  len INT,"
                                        + "  content STRING,"
                                        + "  PRIMARY KEY (`id`) NOT ENFORCED"
                                        + ") WITH ("
                                        + "  'connector' = 'values',"
                                        + "  'data-id' = '%s'"
                                        + ")",
                                tableName, dataId));
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
}
