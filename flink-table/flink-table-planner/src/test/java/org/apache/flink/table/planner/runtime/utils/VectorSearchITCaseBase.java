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

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThatList;

/** Base ITCase to verify {@code VECTOR_SEARCH} function. */
public abstract class VectorSearchITCaseBase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    protected TableEnvironment tEnv;

    protected abstract TableEnvironment getTableEnvironment();

    protected abstract boolean isAsync();

    private final List<Row> data =
            Arrays.asList(
                    Row.of(1L, new Float[] {5f, 12f, 13f}),
                    Row.of(2L, new Float[] {11f, 60f, 61f}),
                    Row.of(3L, new Float[] {8f, 15f, 17f}));
    private final List<Row> nullableData =
            Arrays.asList(Row.of(1L, new Float[] {5f, 12f, 13f}), Row.of(4L, null));

    @BeforeEach
    public void before() throws Exception {
        tEnv = getTableEnvironment();
        createTable("src", data);
        createTable("nullableSrc", nullableData);
        createTable("vector", data);
    }

    @Test
    void testSimple() {
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
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

    @Test
    void testLeftLateralJoin() {
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
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

    @Test
    void testConstantValue() {
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT * FROM TABLE(VECTOR_SEARCH(TABLE vector, DESCRIPTOR(`vector`), ARRAY[5, 12, 13], 2))")
                                .collect());
        assertThat(actual)
                .containsExactlyInAnyOrder(
                        Row.of(1L, new Float[] {5.0f, 12.0f, 13.0f}, 1.0),
                        Row.of(3L, new Float[] {8f, 15f, 17f}, 0.9977375565610862));
    }

    @Test
    void testVectorSearchWithCalc() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "SELECT * FROM nullableSrc\n "
                                                + "LEFT JOIN LATERAL TABLE(VECTOR_SEARCH((SELECT id+1, vector FROM vector), DESCRIPTOR(`vector`), nullableSrc.vector, 2)) ON TRUE"))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Don't support calc on VECTOR_SEARCH node now."));
    }

    private void createTable(String tableName, List<Row> data) {
        String dataId = TestValuesTableFactory.registerData(data);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE `%s`(\n"
                                + "  id BIGINT,\n"
                                + "  vector ARRAY<FLOAT>\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'enable-vector-search' = 'true',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'bounded' = 'true',\n"
                                + "  'async' = '%s' "
                                + ")",
                        tableName, dataId, isAsync()));
    }
}
