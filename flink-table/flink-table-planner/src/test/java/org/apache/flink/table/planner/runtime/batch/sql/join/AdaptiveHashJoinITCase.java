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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.TestingTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for adaptive hash join. */
public class AdaptiveHashJoinITCase extends TestLogger {

    public static final int DEFAULT_PARALLELISM = 3;

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("6m"));
        return config;
    }

    private final TableEnvironment tEnv =
            TestingTableEnvironment.create(
                    EnvironmentSettings.newInstance().inBatchMode().build(),
                    null,
                    TableConfig.getDefault());

    @Before
    public void before() throws Exception {
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, JoinType.HashJoin());

        // prepare data
        List<Row> data1 = new ArrayList<>();
        data1.addAll(getRepeatedRow(2, 100000));
        data1.addAll(getRepeatedRow(5, 100000));
        data1.addAll(getRepeatedRow(10, 100000));
        String dataId1 = TestValuesTableFactory.registerData(data1);

        List<Row> data2 = new ArrayList<>();
        data2.addAll(getRepeatedRow(5, 10));
        data2.addAll(getRepeatedRow(10, 10));
        data2.addAll(getRepeatedRow(20, 10));
        String dataId2 = TestValuesTableFactory.registerData(data2);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE t1 (\n"
                                + "  x INT,\n"
                                + "  y BIGINT,\n"
                                + "  z VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId1));

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE t2 (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));

        tEnv.executeSql(
                "CREATE TABLE sink (\n"
                        + "  x INT,\n"
                        + "  z VARCHAR,\n"
                        + "  a INT,\n"
                        + "  b BIGINT,\n"
                        + "  c VARCHAR\n"
                        + ")  WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")");
    }

    @After
    public void after() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    public void testBuildLeftIntKeyAdaptiveHashJoin() throws Exception {
        tEnv.executeSql("INSERT INTO sink SELECT x, z, a, b, c FROM t1 JOIN t2 ON t1.x=t2.a")
                .await(60, TimeUnit.SECONDS);

        asserResult("sink", 2000000);
    }

    @Test
    public void testBuildRightIntKeyAdaptiveHashJoin() throws Exception {
        tEnv.executeSql("INSERT INTO sink SELECT x, z, a, b, c FROM t2 JOIN t1 ON t1.x=t2.a")
                .await(60, TimeUnit.SECONDS);

        asserResult("sink", 2000000);
    }

    @Test
    public void testBuildLeftStringKeyAdaptiveHashJoin() throws Exception {
        tEnv.executeSql("INSERT INTO sink SELECT x, z, a, b, c FROM t1 JOIN t2 ON t1.z=t2.c")
                .await(60, TimeUnit.SECONDS);

        asserResult("sink", 2000000);
    }

    @Test
    public void testBuildRightStringKeyAdaptiveHashJoin() throws Exception {
        tEnv.executeSql("INSERT INTO sink SELECT x, z, a, b, c FROM t2 JOIN t1 ON t1.z=t2.c")
                .await(60, TimeUnit.SECONDS);

        asserResult("sink", 2000000);
    }

    private void asserResult(String sinkTableName, int resultSize) {
        // Due to concern OOM and record value is same, here just assert result size
        List<String> result = TestValuesTableFactory.getResultsAsStrings(sinkTableName);
        assertThat(result.size()).isEqualTo(resultSize);
    }

    private List<Row> getRepeatedRow(int key, int nums) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            rows.add(Row.of(key, (long) key, String.valueOf(key)));
        }
        return rows;
    }
}
