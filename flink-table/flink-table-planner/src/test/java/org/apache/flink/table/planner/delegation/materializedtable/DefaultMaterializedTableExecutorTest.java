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

package org.apache.flink.table.planner.delegation.materializedtable;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.delegation.materializedtable.RefreshWorkflowContext;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.net.URL;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Test for {@link DefaultMaterializedTableExecutor}. */
class DefaultMaterializedTableExecutorTest {

    private static final String CATALOG = "mt_cat";
    private static final String DATABASE = "mt_db";
    private static final String SAVEPOINT_DIRECTORY = "file:///savepoints";

    private static final String CREATE_MATERIALIZED_TABLE =
            "CREATE MATERIALIZED TABLE my_mt WITH ('connector' = 'values')"
                    + "\n FRESHNESS = INTERVAL '30' SECOND"
                    + "\n REFRESH_MODE = CONTINUOUS"
                    + "\n AS SELECT k, v FROM datagen_source";

    private final FailingCatalog catalog = new FailingCatalog(CATALOG, DATABASE);

    @Test
    void testGetManuallyRefreshStatement() {
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of("catalog", "database", "my_materialized_table");
        String query = "SELECT * FROM my_source_table";
        assertThat(
                        DefaultMaterializedTableExecutor.getRefreshStatement(
                                tableIdentifier,
                                query,
                                Collections.emptyMap(),
                                Collections.emptyMap()))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table`\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)");

        Map<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("k1", "v1");
        partitionSpec.put("k2", "v2");
        assertThat(
                        DefaultMaterializedTableExecutor.getRefreshStatement(
                                tableIdentifier, query, partitionSpec, Collections.emptyMap()))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table`\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)\n"
                                + "  WHERE k1 = 'v1' AND k2 = 'v2'");
    }

    @Test
    void testGenerateInsertStatement() {
        ObjectIdentifier materializedTableIdentifier =
                ObjectIdentifier.of("catalog", "database", "table");
        String definitionQuery = "SELECT * FROM source_table";
        String expectedStatement =
                "INSERT INTO `catalog`.`database`.`table`\n" + "SELECT * FROM source_table";

        String actualStatement =
                DefaultMaterializedTableExecutor.getInsertStatement(
                        materializedTableIdentifier, definitionQuery, Collections.emptyMap());

        assertThat(actualStatement).isEqualTo(expectedStatement);
    }

    @Test
    void testGenerateInsertStatementWithDynamicOptions() {
        ObjectIdentifier materializedTableIdentifier =
                ObjectIdentifier.of("catalog", "database", "table");
        String definitionQuery = "SELECT * FROM source_table";
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("option1", "value1");
        dynamicOptions.put("option2", "value2");

        String expectedStatement =
                "INSERT INTO `catalog`.`database`.`table` "
                        + "/*+ OPTIONS('option1'='value1', 'option2'='value2') */\n"
                        + "SELECT * FROM source_table";

        String actualStatement =
                DefaultMaterializedTableExecutor.getInsertStatement(
                        materializedTableIdentifier, definitionQuery, dynamicOptions);
        assertThat(actualStatement).isEqualTo(expectedStatement);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testGetPeriodRefreshPartition(TestSpec testSpec) {
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of("catalog", "database", "table");

        if (testSpec.errorMessage == null) {
            Map<String, String> actualRefreshPartition =
                    DefaultMaterializedTableExecutor.getPeriodRefreshPartition(
                            testSpec.schedulerTime,
                            testSpec.freshness,
                            objectIdentifier,
                            testSpec.tableOptions,
                            ZoneId.systemDefault());

            assertThat(actualRefreshPartition).isEqualTo(testSpec.expectedRefreshPartition);
        } else {
            assertThatThrownBy(
                            () ->
                                    DefaultMaterializedTableExecutor.getPeriodRefreshPartition(
                                            testSpec.schedulerTime,
                                            testSpec.freshness,
                                            objectIdentifier,
                                            testSpec.tableOptions,
                                            ZoneId.systemDefault()))
                    .hasMessage(testSpec.errorMessage);
        }
    }

    static Stream<TestSpec> testData() {
        return Stream.of(
                // The interval of freshness match the partition specified by the 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 00:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 01:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-02")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "22"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "20"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(8))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "16"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(12))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "12"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 12:00:00")
                        .freshness(IntervalFreshness.ofHour(12))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "59"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "58"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "56"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(5))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "55"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(6))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "54"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(10))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "50"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(12))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "48"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(15))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "45"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(30))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "30"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:30:00")
                        .freshness(IntervalFreshness.ofMinute(30))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),

                // The interval of freshness is larger than the partition specified by the
                // 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 01:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),
                // The interval of freshness is less than the partition specified by the
                // 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 01:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 02:00:00")
                        .freshness(IntervalFreshness.ofHour(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 04:00:00")
                        .freshness(IntervalFreshness.ofHour(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(15))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:01:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),

                // Invalid test case.
                TestSpec.create()
                        .schedulerTime(null)
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .errorMessage(
                                "The scheduler time must not be null during the periodic refresh of the materialized table `catalog`.`database`.`table`."),
                TestSpec.create()
                        .schedulerTime("2024-01-01")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .errorMessage(
                                "Failed to parse a valid partition value for the field 'day' in materialized table `catalog`.`database`.`table` using the scheduler time '2024-01-01' based on the date format 'yyyy-MM-dd HH:mm:ss'."));
    }

    @Test
    void resumeDoesNotRestartARefreshJobThatIsOnlyLocallySuspended() {
        final TestingJobClient suspendedJob =
                new TestingJobClient(new JobID(), JobStatus.SUSPENDED);
        final ScriptedJobSubmitter jobSubmitter = new ScriptedJobSubmitter(suspendedJob);
        final TableEnvironment tableEnv = createTableEnvironment(jobSubmitter);
        tableEnv.executeSql(CREATE_MATERIALIZED_TABLE);

        assertThatThrownBy(() -> tableEnv.executeSql("ALTER MATERIALIZED TABLE my_mt RESUME"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "continuous refresh job has been resumed, jobId is "
                                + suspendedJob.getJobID());
        assertThat(jobSubmitter.submissions).isEqualTo(1);
    }

    @Test
    void createReportsARefreshJobItCouldNeitherRecordNorCancel() throws Exception {
        final JobID jobId = new JobID();
        final TestingJobClient uncancellableJob =
                new TestingJobClient(jobId, JobStatus.RUNNING)
                        .withCancelRequest(
                                () ->
                                        FutureUtils.completedExceptionally(
                                                new RuntimeException("injected cancel failure")));
        final TableEnvironment tableEnv =
                createTableEnvironment(new ScriptedJobSubmitter(uncancellableJob));
        catalog.failNextAlterTable(hasRefreshStatus(RefreshStatus.ACTIVATED));

        final Throwable createFailure =
                catchThrowable(() -> tableEnv.executeSql(CREATE_MATERIALIZED_TABLE));

        assertThat(createFailure)
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Failed to submit continuous refresh job")
                .hasRootCauseMessage("injected cancel failure");
        final Throwable unrecordedJobFailure = createFailure.getCause();
        assertThat(unrecordedJobFailure)
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("and cancelling it failed as well")
                .hasMessageContaining("must be cancelled manually");
        assertThat(unrecordedJobFailure.getSuppressed())
                .singleElement()
                .satisfies(
                        catalogFailure ->
                                assertThat(catalogFailure)
                                        .hasRootCauseMessage("injected alter failure"));
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, "my_mt"))).isFalse();
    }

    @Test
    void createRecordsTheClusterIdOfAKubernetesSessionTarget() throws Exception {
        final TableEnvironment tableEnv =
                createTableEnvironment(
                        new ScriptedJobSubmitter(
                                new TestingJobClient(new JobID(), JobStatus.RUNNING)));
        tableEnv.getConfig().set("execution.target", "kubernetes-session");
        tableEnv.getConfig().set("kubernetes.cluster-id", "c1");

        tableEnv.executeSql(CREATE_MATERIALIZED_TABLE);

        final ContinuousRefreshHandler refreshHandler =
                getRefreshHandler(catalog, new ObjectPath(DATABASE, "my_mt"));
        assertThat(refreshHandler.getExecutionTarget()).isEqualTo("kubernetes-session");
        assertThat(refreshHandler.getClusterId()).isEqualTo("c1");
    }

    private TableEnvironment createTableEnvironment(ScriptedJobSubmitter jobSubmitter) {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(DeploymentOptions.TARGET, "remote");
        rootConfiguration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, SAVEPOINT_DIRECTORY);

        final StreamExecutionEnvironment executionEnvironment =
                new StreamExecutionEnvironment(rootConfiguration);
        final ResourceManager resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0], getClass().getClassLoader(), rootConfiguration);
        final ClassLoader userClassLoader = resourceManager.getUserClassLoader();
        final Executor executor =
                AbstractStreamTableEnvironmentImpl.lookupExecutor(
                        userClassLoader, executionEnvironment);

        final TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(executor.getConfiguration());
        jobSubmitter.tableConfig = tableConfig;

        final ModuleManager moduleManager = new ModuleManager();
        final CatalogManager catalogManager =
                CatalogManagerMocks.preparedCatalogManager()
                        .classLoader(userClassLoader)
                        .config(tableConfig)
                        .defaultCatalog(CATALOG, catalog)
                        .executionConfig(executionEnvironment.getConfig())
                        .build();
        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);
        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        userClassLoader,
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        final TableEnvironment tableEnv =
                new StreamTableEnvironmentImpl(
                        catalogManager,
                        moduleManager,
                        resourceManager,
                        functionCatalog,
                        tableConfig,
                        executionEnvironment,
                        planner,
                        executor,
                        true,
                        new DefaultMaterializedTableExecutorFactory(),
                        jobSubmitter);
        tableEnv.executeSql(
                "CREATE TABLE datagen_source (k INT, v BIGINT) WITH ('connector' = 'datagen')");
        return tableEnv;
    }

    private static ContinuousRefreshHandler getRefreshHandler(Catalog catalog, ObjectPath tablePath)
            throws Exception {
        return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                ((CatalogMaterializedTable) catalog.getTable(tablePath))
                        .getSerializedRefreshHandler(),
                DefaultMaterializedTableExecutorTest.class.getClassLoader());
    }

    private static Predicate<CatalogBaseTable> hasRefreshStatus(RefreshStatus refreshStatus) {
        return table ->
                table instanceof CatalogMaterializedTable
                        && ((CatalogMaterializedTable) table).getRefreshStatus() == refreshStatus;
    }

    /** Fails a selected table alteration once, so tests can observe a rejected catalog write. */
    private static final class FailingCatalog extends GenericInMemoryCatalog {

        @Nullable private Predicate<CatalogBaseTable> failingAlterMatcher;

        private FailingCatalog(String name, String defaultDatabase) {
            super(name, defaultDatabase);
        }

        /** The next {@code alterTable} whose new table matches fails; the hook then clears. */
        private void failNextAlterTable(Predicate<CatalogBaseTable> newTableMatches) {
            this.failingAlterMatcher = newTableMatches;
        }

        @Override
        public void alterTable(
                ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
                throws TableNotExistException {
            if (failingAlterMatcher != null && failingAlterMatcher.test(newTable)) {
                failingAlterMatcher = null;
                throw new CatalogException("injected alter failure");
            }
            super.alterTable(tablePath, newTable, ignoreIfNotExists);
        }
    }

    private static final class ScriptedJobSubmitter implements MaterializedTableJobSubmitter {

        private final Deque<JobClient> jobClients;
        private int submissions;
        private TableConfig tableConfig;

        private ScriptedJobSubmitter(JobClient... jobClients) {
            this.jobClients = new ArrayDeque<>(Arrays.asList(jobClients));
        }

        @Override
        public RefreshJobResult submitRefreshJob(
                Configuration executionConfig, String insertStatement) {
            submissions++;
            final JobClient jobClient = jobClients.poll();
            if (jobClient == null) {
                throw new IllegalStateException(
                        "Unexpected refresh job submission: " + insertStatement);
            }
            return new RefreshJobResult(
                    RefreshJobTarget.forNewJob(tableConfig),
                    jobClient.getJobID().toHexString(),
                    jobClient);
        }

        @Override
        public boolean supportsApplicationTargets() {
            return false;
        }

        @Override
        public Optional<RefreshWorkflowContext> getRefreshWorkflowContext() {
            return Optional.empty();
        }
    }

    private static class TestSpec {
        private String schedulerTime;
        private IntervalFreshness freshness;
        private final Map<String, String> tableOptions;
        private final Map<String, String> expectedRefreshPartition;

        private @Nullable String errorMessage;

        private TestSpec() {
            this.tableOptions = new HashMap<>();
            this.expectedRefreshPartition = new HashMap<>();
        }

        public static TestSpec create() {
            return new TestSpec();
        }

        public TestSpec schedulerTime(String schedulerTime) {
            this.schedulerTime = schedulerTime;
            return this;
        }

        public TestSpec freshness(IntervalFreshness freshness) {
            this.freshness = freshness;
            return this;
        }

        public TestSpec tableOptions(String key, String value) {
            this.tableOptions.put(key, value);
            return this;
        }

        public TestSpec expectedRefreshPartition(String key, String value) {
            this.expectedRefreshPartition.put(key, value);
            return this;
        }

        public TestSpec errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "schedulerTime="
                    + schedulerTime
                    + ", freshness="
                    + freshness
                    + ", tableOptions="
                    + tableOptions
                    + ", expectedRefreshPartition="
                    + expectedRefreshPartition
                    + '}';
        }
    }
}
