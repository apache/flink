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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.Model;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesModelFactory;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThatList;

/** Base ITCase to verify {@code ML_PREDICT} function. */
public abstract class MLPredictITCaseBase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    protected TableEnvironment tEnv;

    protected abstract TableEnvironment getTableEnvironment();

    protected abstract boolean isAsync();

    private final List<Row> data =
            Arrays.asList(
                    Row.of(1L, 12, "Julian"),
                    Row.of(2L, 15, "Hello"),
                    Row.of(3L, 15, "Fabian"),
                    Row.of(8L, 11, "Hello world"),
                    Row.of(9L, 12, "Hello world!"));

    private final List<Row> dataWithNull =
            Arrays.asList(
                    Row.of(null, 15, "Hello"),
                    Row.of(3L, 15, "Fabian"),
                    Row.of(null, 11, "Hello world"),
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
        idLen2features.put(Row.of(null, 15), Collections.singletonList(Row.of("x1", 1, "zNull15")));
        idLen2features.put(Row.of(15L, 15), Collections.singletonList(Row.of("x1", 1, "z1515")));
        idLen2features.put(Row.of(3L, 15), Collections.singletonList(Row.of("x2", 2, "z315")));
        idLen2features.put(Row.of(null, 11), Collections.singletonList(Row.of("x3", 3, "zNull11")));
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
        content2vector.put(
                Row.of("Hello world"),
                Collections.singletonList(Row.of((Object) new Float[] {4.0f, 5.0f, 6.0f})));
        content2vector.put(
                Row.of("Hello world!"),
                Collections.singletonList(Row.of((Object) new Float[] {5.0f, 6.0f, 7.0f})));
    }

    @BeforeEach
    public void before() throws Exception {
        tEnv = getTableEnvironment();
        createScanTable("src", data);
        createScanTable("nullable_src", dataWithNull);

        tEnv.executeSql(
                String.format(
                        "CREATE MODEL m1\n"
                                + "INPUT (a BIGINT)\n"
                                + "OUTPUT (x STRING, y INT, z STRING)\n"
                                + "WITH (\n"
                                + "  'provider' = 'values',"
                                + "  'async' = '%s',"
                                + "  'data-id' = '%s'"
                                + ")",
                        isAsync(), TestValuesModelFactory.registerData(id2features)));
        tEnv.executeSql(
                String.format(
                        "CREATE MODEL m2\n"
                                + "INPUT (a BIGINT, b INT)\n"
                                + "OUTPUT (x STRING, y INT, z STRING)\n"
                                + "WITH (\n"
                                + "  'provider' = 'values',"
                                + "  'async' = '%s',"
                                + "  'data-id' = '%s'"
                                + ")",
                        isAsync(), TestValuesModelFactory.registerData(idLen2features)));
        tEnv.executeSql(
                String.format(
                        "CREATE MODEL m3\n"
                                + "INPUT (content STRING)\n"
                                + "OUTPUT (vector ARRAY<FLOAT>)\n"
                                + "WITH (\n"
                                + "  'provider' = 'values',"
                                + "  'data-id' = '%s',"
                                + "  'latency' = '1000',"
                                + "  'async' = '%s'"
                                + ")",
                        TestValuesModelFactory.registerData(content2vector), isAsync()));
    }

    @Test
    public void testMLPredict() {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT id, z "
                                                + "FROM ML_PREDICT(TABLE src, MODEL m1, DESCRIPTOR(`id`)) ")
                                .collect());

        assertThatList(result)
                .containsExactlyInAnyOrder(
                        Row.of(1L, "z1"),
                        Row.of(2L, "z2"),
                        Row.of(3L, "z3"),
                        Row.of(8L, "z8"),
                        Row.of(9L, "z9"));
    }

    @Test
    public void testMLPredictWithMultipleFields() {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT id, len, z "
                                                + "FROM ML_PREDICT(TABLE nullable_src, MODEL m2, DESCRIPTOR(`id`, `len`)) ")
                                .collect());

        assertThatList(result)
                .containsExactlyInAnyOrder(
                        Row.of(3L, 15, "z315"),
                        Row.of(9L, 12, "z912"),
                        Row.of(null, 11, "zNull11"),
                        Row.of(null, 15, "zNull15"));
    }

    @Test
    public void testPredictWithConstantValues() {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "WITH v(id) AS (SELECT * FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)))) "
                                                + "SELECT * FROM ML_PREDICT( "
                                                + "  INPUT => TABLE v, "
                                                + "  MODEL => MODEL `m1`, "
                                                + "  ARGS => DESCRIPTOR(`id`) "
                                                + ")")
                                .collect());

        assertThatList(result)
                .containsExactlyInAnyOrder(Row.of(1L, "x1", 1, "z1"), Row.of(2L, "x2", 2, "z2"));
    }

    @Test
    public void testPredictTableApiWithView() {
        Model model = tEnv.fromModel("m1");
        Table table = tEnv.from("src");
        tEnv.createView("view_src", model.predict(table, ColumnList.of("id")));
        List<Row> results =
                CollectionUtil.iteratorToList(tEnv.executeSql("select * from view_src").collect());
        assertThatList(results)
                .containsExactlyInAnyOrder(
                        Row.of(1L, 12, "Julian", "x1", 1, "z1"),
                        Row.of(2L, 15, "Hello", "x2", 2, "z2"),
                        Row.of(3L, 15, "Fabian", "x3", 3, "z3"),
                        Row.of(8L, 11, "Hello world", "x8", 8, "z8"),
                        Row.of(9L, 12, "Hello world!", "x9", 9, "z9"));
    }

    @Test
    public void testPredictWithRuntimeConfig() {
        if (!isAsync()) {
            // Only test async timeout for async mode
            return;
        }
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT id, vector FROM ML_PREDICT(TABLE src, MODEL m3, DESCRIPTOR(`content`), MAP['timeout', '1ms'])")
                                        .await())
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TimeoutException.class, "Async function call has timed out."));
    }

    private void createScanTable(String tableName, List<Row> data) {
        String dataId = TestValuesTableFactory.registerData(data);
        String bounded =
                tEnv.getConfig().get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.BATCH
                        ? "true"
                        : "false";
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE `%s`(\n"
                                + "  id BIGINT,\n"
                                + "  len INT,\n"
                                + "  content STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'bounded' = '%s'\n"
                                + ")",
                        tableName, dataId, bounded));
    }
}
