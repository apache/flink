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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link HiveTableSink} with {@link
 * org.apache.flink.connector.file.table.FileSystemOutputFormat} and speculative execution enabled.
 */
class HiveTableSpeculativeSinkITCase {

    private static final int PARALLELISM = 3;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configure(new Configuration()))
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    private HiveCatalog hiveCatalog;

    @BeforeEach
    void createCatalog() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
    }

    @AfterEach
    void closeCatalog() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    void testBatchWritingWithoutCompactionWithSpeculativeSink() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configure(new Configuration()));
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        TableEnvExecutorUtil.executeInSeparateDatabase(
                tEnv,
                true,
                () -> {
                    tEnv.executeSql(
                            "create table append_table("
                                    + "i int, "
                                    + "j int) TBLPROPERTIES ("
                                    + "'sink.parallelism' ='"
                                    + PARALLELISM
                                    + "')"
                                    + "");
                    DataStream<Row> slowStream =
                            tEnv.toChangelogStream(tEnv.sqlQuery("select 0, 0"));
                    slowStream =
                            slowStream
                                    .map(
                                            new RichMapFunction<Row, Row>() {
                                                @Override
                                                public Row map(Row value) throws Exception {
                                                    if (getRuntimeContext().getAttemptNumber()
                                                            <= 0) {
                                                        Thread.sleep(Integer.MAX_VALUE);
                                                    }
                                                    assert getRuntimeContext().getAttemptNumber()
                                                            > 0;
                                                    value.setField(
                                                            1,
                                                            getRuntimeContext().getAttemptNumber());
                                                    return value;
                                                }
                                            })
                                    .name("slowMap")
                                    .returns(
                                            Types.ROW_NAMED(
                                                    new String[] {"i", "j"}, Types.INT, Types.INT))
                                    .setParallelism(PARALLELISM);

                    Table t =
                            tEnv.fromChangelogStream(
                                    slowStream,
                                    Schema.newBuilder()
                                            .column("i", DataTypes.INT())
                                            .column("j", DataTypes.INT())
                                            .build(),
                                    ChangelogMode.insertOnly());

                    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                    tEnv.createTemporaryView("mappedTable", t);
                    String insertQuery = "insert into append_table select * from mappedTable";

                    // assert that the SlowMap operator is chained with the hive table sink, which
                    // will lead
                    // to a slow sink as well.
                    StreamTableEnvironmentImpl tEnvImpl = (StreamTableEnvironmentImpl) tEnv;
                    JobGraph jobGraph =
                            tEnvImpl.execEnv()
                                    .generateStreamGraph(
                                            tEnvImpl.getPlanner()
                                                    .translate(
                                                            Collections.singletonList(
                                                                    (ModifyOperation)
                                                                            tEnvImpl.getParser()
                                                                                    .parse(
                                                                                            insertQuery)
                                                                                    .get(0))))
                                    .getJobGraph();

                    for (JobVertex jobVertex : jobGraph.getVertices()) {
                        if (jobVertex.getName().contains("slowMap")) {
                            assertThat(jobVertex.getName().contains("Sink")).isTrue();
                        }
                    }
                    tEnv.executeSql(insertQuery).await();

                    List<Row> rows =
                            CollectionUtil.iteratorToList(
                                    tEnv.executeSql("select * from append_table").collect());
                    rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));

                    // Finally we will get the output value with attemptNumber larger than 0
                    assertThat(rows).isEqualTo(Collections.singletonList(Row.of(0, 1)));
                });
    }

    private static Configuration configure(Configuration configuration) {
        // for speculative execution
        configuration.set(BatchExecutionOptions.SPECULATIVE_ENABLED, true);
        // for testing, does not block node by default
        configuration.set(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION, Duration.ZERO);
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER, 1.0);
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO, 0.2);
        configuration.set(
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND, Duration.ofMillis(0));
        return configuration;
    }
}
