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

package org.apache.flink.table.planner.runtime.batch.sql.join;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for {@link
 * org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalLateralSnapshotJoinRule}:
 * in batch mode a {@code LATERAL SNAPSHOT} join is executed as a regular join of the probe side
 * against the (final) build side.
 */
class LateralSnapshotJoinITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private TableEnvironment tEnv;

    @BeforeEach
    void setup() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        final String probeId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of("a", 1), Row.of("b", 2), Row.of("c", 3)));
        tEnv.executeSql(
                "CREATE TABLE probe (pk STRING, pv INT) WITH ("
                        + "'connector' = 'values', 'data-id' = '"
                        + probeId
                        + "', 'bounded' = 'true')");

        // Append-only build side. The key "a" appears twice: the batch join is against the final
        // (complete) build side, so both "a" rows participate.
        final String buildId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of("a", 10), Row.of("a", 11), Row.of("b", 20)));
        tEnv.executeSql(
                "CREATE TABLE b (bk STRING, bv INT) WITH ("
                        + "'connector' = 'values', 'data-id' = '"
                        + buildId
                        + "', 'bounded' = 'true')");
    }

    @AfterEach
    void after() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    void testInnerJoin() throws Exception {
        assertThat(
                        execute(
                                "SELECT pk, pv, bk, bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b)) AS s ON probe.pk = s.bk"))
                .containsExactlyInAnyOrder(
                        Row.of("a", 1, "a", 10), Row.of("a", 1, "a", 11), Row.of("b", 2, "b", 20));
    }

    @Test
    void testLeftJoin() throws Exception {
        assertThat(
                        execute(
                                "SELECT pk, pv, bk, bv FROM probe LEFT JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b)) AS s ON probe.pk = s.bk"))
                .containsExactlyInAnyOrder(
                        Row.of("a", 1, "a", 10),
                        Row.of("a", 1, "a", 11),
                        Row.of("b", 2, "b", 20),
                        Row.of("c", 3, null, null));
    }

    @Test
    void testInnerJoinWithNonEquiCondition() throws Exception {
        assertThat(
                        execute(
                                "SELECT pk, pv, bk, bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b)) AS s ON probe.pk = s.bk AND s.bv > 10"))
                .containsExactlyInAnyOrder(Row.of("a", 1, "a", 11), Row.of("b", 2, "b", 20));
    }

    @Test
    void testSnapshotArgumentsAreIgnored() throws Exception {
        // The streaming-only arguments do not affect the batch result.
        assertThat(
                        execute(
                                "SELECT pk, pv, bk, bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b, "
                                        + "load_completed_condition => 'compile_time', "
                                        + "load_completed_idle_timeout => INTERVAL '10' SECOND, "
                                        + "state_ttl => INTERVAL '1' DAY"
                                        + ")) AS s ON probe.pk = s.bk"))
                .containsExactlyInAnyOrder(
                        Row.of("a", 1, "a", 10), Row.of("a", 1, "a", 11), Row.of("b", 2, "b", 20));
    }

    private List<Row> execute(String sql) throws Exception {
        final TableResult result = tEnv.executeSql(sql);
        try (CloseableIterator<Row> iterator = result.collect()) {
            return CollectionUtil.iteratorToList(iterator);
        }
    }
}
