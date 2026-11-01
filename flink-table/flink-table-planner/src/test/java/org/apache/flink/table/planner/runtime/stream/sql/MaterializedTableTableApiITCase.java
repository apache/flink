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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.RestoredCheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for materialized table related statement via {@link TableEnvironment}. */
class MaterializedTableTableApiITCase {

    private static final String CATALOG = "test_catalog";
    private static final String DATABASE = "test_db";
    private static final ObjectIdentifier USERS_SHOPS =
            ObjectIdentifier.of(CATALOG, DATABASE, "users_shops");

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration PAUSE = Duration.ofMillis(200);

    private static final ConfigOption<String> KUBERNETES_CLUSTER_ID =
            ConfigOptions.key("kubernetes.cluster-id").stringType().noDefaultValue();

    private static final String UPDATING_VALUES_SINK =
            " WITH(\n"
                    + "   'connector' = 'values',\n"
                    + "   'sink-insert-only' = 'false'\n"
                    + " )\n";

    private static final String USERS_SHOPS_QUERY =
            " AS SELECT \n"
                    + "  user_id,\n"
                    + "  shop_id,\n"
                    + "  ds,\n"
                    + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                    + "  SUM (1) AS pv\n"
                    + " FROM (\n"
                    + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                    + " ) AS tmp\n"
                    + " GROUP BY (user_id, shop_id, ds)";

    private static final String USERS_SHOPS_DDL =
            "CREATE MATERIALIZED TABLE users_shops"
                    + " PARTITIONED BY (ds)\n"
                    + UPDATING_VALUES_SINK
                    + " FRESHNESS = INTERVAL '30' SECOND\n"
                    + USERS_SHOPS_QUERY;

    private static final ResolvedSchema USERS_SHOPS_SCHEMA =
            ResolvedSchema.of(
                    Arrays.asList(
                            Column.physical("user_id", DataTypes.BIGINT()),
                            Column.physical("shop_id", DataTypes.BIGINT()),
                            Column.physical("ds", DataTypes.STRING()),
                            Column.physical("payed_buy_fee_sum", DataTypes.BIGINT()),
                            Column.physical("pv", DataTypes.INT().notNull())));

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private StreamTableEnvironment tEnv;
    private GenericInMemoryCatalog catalog;
    private RestClusterClient<?> clusterClient;

    @BeforeEach
    void before(@InjectClusterClient RestClusterClient<?> injectedClusterClient) {
        TestStreamEnvironment.unsetAsContext();
        clusterClient = injectedClusterClient;

        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        new Configuration(MINI_CLUSTER.getClientConfiguration()));
        executionEnvironment.setParallelism(1);
        tEnv = StreamTableEnvironment.create(executionEnvironment);
        tEnv.getConfig().set(DeploymentOptions.TARGET, "remote");
        tEnv.getConfig()
                .set(
                        RestOptions.ADDRESS,
                        MINI_CLUSTER.getClientConfiguration().get(RestOptions.ADDRESS));

        catalog = new GenericInMemoryCatalog(CATALOG, DATABASE);
        tEnv.registerCatalog(CATALOG, catalog);
        tEnv.useCatalog(CATALOG);
        tEnv.useDatabase(DATABASE);

        tEnv.executeSql(
                "CREATE TABLE datagenSource (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '10'\n"
                        + ")");
    }

    @AfterEach
    void after() throws Exception {
        for (JobStatusMessage job :
                clusterClient.listJobs().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            if (!job.getJobState().isTerminalState()) {
                cancelUnlessEnded(job.getJobId());
            }
        }
        TestValuesTableFactory.clearAllData();
    }

    private void cancelUnlessEnded(JobID jobId) throws Exception {
        try {
            clusterClient.cancel(jobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception cancelFailure) {
            if (!getJobStatus(jobId).isGloballyTerminalState()) {
                throw cancelFailure;
            }
        }
    }

    @Test
    void testCreateMaterializedTableInContinuousMode() throws Exception {
        tEnv.executeSql(USERS_SHOPS_DDL);

        final ResolvedCatalogMaterializedTable actualMaterializedTable = getUsersShops();
        assertThat(actualMaterializedTable.getResolvedSchema()).isEqualTo(USERS_SHOPS_SCHEMA);
        assertThat(actualMaterializedTable.getLogicalRefreshMode())
                .isSameAs(LogicalRefreshMode.AUTOMATIC);
        assertThat(actualMaterializedTable.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);
        assertThat(actualMaterializedTable.getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(actualMaterializedTable.getRefreshHandlerDescription()).isNotEmpty();
        assertThat(actualMaterializedTable.getSerializedRefreshHandler()).isNotEmpty();

        final JobID refreshJobId = getRefreshJobId();
        waitUntilAllTasksAreRunning(clusterClient, refreshJobId);
        assertThat(getJobStatus(refreshJobId)).isEqualTo(JobStatus.RUNNING);
        assertThat(getCheckpointInterval(refreshJobId)).isEqualTo(30 * 1000);
    }

    @Test
    void testCreateMaterializedTableInContinuousModeWithoutFreshnessAndRefreshMode()
            throws Exception {
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + UPDATING_VALUES_SINK
                        + USERS_SHOPS_QUERY);

        final ResolvedCatalogMaterializedTable actualMaterializedTable = getUsersShops();
        assertThat(actualMaterializedTable.getResolvedSchema()).isEqualTo(USERS_SHOPS_SCHEMA);
        assertThat(actualMaterializedTable.getDefinitionFreshness())
                .isEqualTo(IntervalFreshness.ofMinute(3));
        assertThat(actualMaterializedTable.getLogicalRefreshMode())
                .isSameAs(LogicalRefreshMode.AUTOMATIC);
        assertThat(actualMaterializedTable.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);
        assertThat(actualMaterializedTable.getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(actualMaterializedTable.getRefreshHandlerDescription()).isNotEmpty();
        assertThat(actualMaterializedTable.getSerializedRefreshHandler()).isNotEmpty();

        final JobID refreshJobId = getRefreshJobId();
        waitUntilAllTasksAreRunning(clusterClient, refreshJobId);
        assertThat(getJobStatus(refreshJobId)).isEqualTo(JobStatus.RUNNING);
        // default checkpoint interval is 3 minutes
        assertThat(getCheckpointInterval(refreshJobId)).isEqualTo(60 * 3 * 1000);
    }

    @Test
    void testCreateMaterializedTableInContinuousModeWithCustomCheckpointInterval()
            throws Exception {
        final long checkpointInterval = 60 * 1000;
        tEnv.getConfig()
                .set(
                        CheckpointingOptions.CHECKPOINTING_INTERVAL,
                        Duration.ofMillis(checkpointInterval));

        tEnv.executeSql(USERS_SHOPS_DDL);

        final JobID refreshJobId = getRefreshJobId();
        waitUntilAllTasksAreRunning(clusterClient, refreshJobId);
        assertThat(getCheckpointInterval(refreshJobId)).isEqualTo(checkpointInterval);
    }

    @Test
    void testCreateMaterializedTableFailedInContinuousMode() {
        // an insert-only sink cannot consume the updates of the aggregation
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE MATERIALIZED TABLE users_shops"
                                                + " PARTITIONED BY (ds)\n"
                                                + " WITH(\n"
                                                + "   'connector' = 'values',\n"
                                                + "   'sink-insert-only' = 'true'\n"
                                                + " )\n"
                                                + " FRESHNESS = INTERVAL '30' SECOND\n"
                                                + USERS_SHOPS_QUERY))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        String.format(
                                "Failed to submit continuous refresh job for materialized table %s.",
                                USERS_SHOPS.asSerializableString()));

        assertThat(catalog.tableExists(USERS_SHOPS.toObjectPath())).isFalse();
    }

    @Test
    void testAlterMaterializedTableRefresh() throws Exception {
        final List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));
        data.add(Row.of(3L, 3L, 3L, "2024-01-02"));
        createBoundedValuesSource(data);

        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + UPDATING_VALUES_SINK
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = CONTINUOUS\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  COUNT(order_id) AS order_cnt\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, order_created_at AS ds, order_id FROM my_source"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)");
        CommonTestUtils.waitUtil(
                () -> getUsersShopsResults().size() == data.size(),
                TIMEOUT,
                PAUSE,
                "Failed to verify the data in materialized table.");

        data.remove(2);

        final long currentTime = System.currentTimeMillis();
        final List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE users_shops REFRESH PARTITION (ds = '2024-01-02')")
                                .collect());
        assertThat(result).hasSize(1);
        final JobID refreshJobId = JobID.fromHexString(result.get(0).getFieldAs(0));

        verifyRefreshJobCreated(refreshJobId, currentTime);

        assertThat(getUsersShopsResults()).containsExactly("+I[2, 2, 2024-01-02, 1]");
    }

    @Test
    void testAlterMaterializedTableRefreshWithInvalidPartitionSpec() {
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds1, ds2)\n"
                        + UPDATING_VALUES_SINK
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds1,\n"
                        + "  ds2,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds1, user_id % 10 as ds2, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds1, ds2)");

        // CASE 1: check unknown partition keys
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE users_shops REFRESH PARTITION (ds3 = '2024-01-01')"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "The partition spec contains unknown partition keys:\n"
                                + "\n"
                                + "ds3\n"
                                + "\n"
                                + "All known partition keys are:\n"
                                + "\n"
                                + "ds2\n"
                                + "ds1");

        // CASE 2: check specific non-string partition keys as partition spec to refresh
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE users_shops REFRESH PARTITION (ds2 = 5)"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Currently, refreshing materialized table only supports referring to char, varchar and string type partition keys. All specified partition keys in partition specs with unsupported types are:\n"
                                + "\n"
                                + "ds2");
    }

    @Test
    void testAlterMaterializedTableSuspendAndResumeInContinuousMode(@TempDir Path temporaryPath)
            throws Exception {
        tEnv.executeSql(USERS_SHOPS_DDL);

        assertThat(getUsersShops().getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        waitUntilAllTasksAreRunning(clusterClient, getRefreshJobId());

        setupSavepointDir(temporaryPath);

        tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND");

        assertThat(getUsersShops().getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
        final ContinuousRefreshHandler suspendRefreshHandler = getRefreshHandler();
        assertThat(getJobStatus(JobID.fromHexString(suspendRefreshHandler.getJobId())))
                .isEqualTo(JobStatus.FINISHED);
        assertThat(suspendRefreshHandler.getRestorePath()).isNotEmpty();
        final String actualSavepointPath = suspendRefreshHandler.getRestorePath().get();

        tEnv.executeSql(
                "ALTER MATERIALIZED TABLE users_shops RESUME WITH ('sink.drop-late-event' = 'false')");

        final ResolvedCatalogMaterializedTable resumedMaterializedTable = getUsersShops();
        assertThat(resumedMaterializedTable.getOptions()).doesNotContainKey("sink.drop-late-event");
        assertThat(resumedMaterializedTable.getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);

        final JobID resumeJobId = getRefreshJobId();
        waitUntilAllTasksAreRunning(clusterClient, resumeJobId);
        assertThat(getJobStatus(resumeJobId)).isEqualTo(JobStatus.RUNNING);
        assertThat(getJobRestoreSavepointPath(resumeJobId)).hasValue(actualSavepointPath);
    }

    @Test
    void testAlterMaterializedTableWithoutSavepointDirConfiguredInContinuousMode()
            throws Exception {
        tEnv.executeSql(USERS_SHOPS_DDL);
        waitUntilAllTasksAreRunning(clusterClient, getRefreshJobId());

        assertThatThrownBy(() -> tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND"))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Savepoint directory is not configured, can't stop job with savepoint.");
    }

    @Test
    void testAlterMaterializedTableWithRepeatedSuspendAndResumeInContinuousMode(
            @TempDir Path temporaryPath) throws Exception {
        tEnv.executeSql(USERS_SHOPS_DDL);
        waitUntilAllTasksAreRunning(clusterClient, getRefreshJobId());

        setupSavepointDir(temporaryPath);

        tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND");

        assertThatThrownBy(() -> tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND"))
                .rootCause()
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        String.format(
                                "Materialized table %s continuous refresh job has been suspended",
                                USERS_SHOPS));

        tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops RESUME");

        assertThatThrownBy(() -> tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops RESUME"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        String.format(
                                "Materialized table %s continuous refresh job has been resumed",
                                USERS_SHOPS));
    }

    @Test
    void testCreateOrAlterMaterializedTableColumnListReordersSchemaOnCreatePath() {
        createBoundedValuesSource(List.of());

        tEnv.executeSql(
                "CREATE OR ALTER MATERIALIZED TABLE users_shops (shop_id, user_id, oca, order_id)"
                        + UPDATING_VALUES_SINK
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  order_created_at AS oca,\n"
                        + "  order_id"
                        + " FROM my_source");

        assertThat(getUsersShops().getResolvedSchema().getColumnNames())
                .containsExactly("shop_id", "user_id", "oca", "order_id");
    }

    @Test
    void testCreateMaterializedTableWithDistribution() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE MATERIALIZED TABLE users_shops ("
                                                + " PRIMARY KEY (shop_id, user_id) not enforced)"
                                                + " DISTRIBUTED BY HASH (user_id) INTO 7 BUCKETS\n"
                                                + " WITH(\n"
                                                + "   'connector' = 'blackhole'\n"
                                                + " )\n"
                                                + " FRESHNESS = INTERVAL '30' SECOND\n"
                                                + " AS SELECT 1 as shop_id, 2 as user_id"))
                .isInstanceOf(TableException.class)
                .hasRootCauseMessage(
                        String.format(
                                "Table '%s' is distributed into buckets,"
                                        + " but the underlying DynamicTableSink doesn't implement the SupportsBucketing interface.",
                                USERS_SHOPS.asSummaryString()));
    }

    @Test
    void testAlterMaterializedTableAsQueryInContinuousMode(@TempDir Path temporaryPath)
            throws Exception {
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE users_shops ("
                        + " PRIMARY KEY (ds, user_id) not enforced)"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'connector' = 'values',\n"
                        + "   'sink-insert-only' = 'false',\n"
                        + "   'sink-changelog-mode-enforced' = 'I,UB,UA,D'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " AS SELECT \n"
                        + "  coalesce(user_id, 0) as user_id,\n"
                        + "  shop_id,\n"
                        + "  coalesce(ds, '') as ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)");
        final ResolvedCatalogMaterializedTable oldTable = getUsersShops();
        waitUntilAllTasksAreRunning(clusterClient, getRefreshJobId());

        setupSavepointDir(temporaryPath);

        tEnv.executeSql(
                "ALTER MATERIALIZED TABLE users_shops"
                        + " AS SELECT \n"
                        + "  coalesce(user_id, 0) as user_id,\n"
                        + "  shop_id,\n"
                        + "  coalesce(ds, '') as ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)");
        final ResolvedCatalogMaterializedTable newTable = getUsersShops();

        assertThat(getAddedColumns(newTable.getResolvedSchema(), oldTable.getResolvedSchema()))
                .containsExactly(Column.physical("pv", DataTypes.INT()));
        assertThat(newTable.getResolvedSchema().getPrimaryKey())
                .isEqualTo(oldTable.getResolvedSchema().getPrimaryKey());
        assertThat(newTable.getResolvedSchema().getWatermarkSpecs())
                .isEqualTo(oldTable.getResolvedSchema().getWatermarkSpecs());
        assertThat(newTable.getExpandedQuery())
                .hasToString(
                        String.format(
                                "SELECT COALESCE(`tmp`.`user_id`, 0) AS `user_id`, `tmp`.`shop_id`, COALESCE(`tmp`.`ds`, '') AS `ds`, SUM(`tmp`.`payment_amount_cents`) AS `payed_buy_fee_sum`, SUM(1) AS `pv`\n"
                                        + "FROM (SELECT `datagenSource`.`user_id`, `datagenSource`.`shop_id`, DATE_FORMAT(`datagenSource`.`order_created_at`, 'yyyy-MM-dd') AS `ds`, `datagenSource`.`payment_amount_cents`\n"
                                        + "FROM `%s`.`%s`.`datagenSource` AS `datagenSource`) AS `tmp`\n"
                                        + "GROUP BY ROW(`tmp`.`user_id`, `tmp`.`shop_id`, `tmp`.`ds`)",
                                CATALOG, DATABASE));
        assertThat(oldTable.getSerializedRefreshHandler())
                .isNotEqualTo(newTable.getSerializedRefreshHandler());
        assertThat(getJobRestoreSavepointPath(getRefreshJobId())).isEmpty();
    }

    @Test
    void testAlterMaterializedTableAsQueryInContinuousModeWithSuspendStatus(
            @TempDir Path temporaryPath) throws Exception {
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + UPDATING_VALUES_SINK
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)");
        final ResolvedCatalogMaterializedTable oldTable = getUsersShops();
        waitUntilAllTasksAreRunning(clusterClient, getRefreshJobId());

        setupSavepointDir(temporaryPath);

        tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND");

        tEnv.executeSql(
                "ALTER MATERIALIZED TABLE users_shops"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)");
        final ResolvedCatalogMaterializedTable newTable = getUsersShops();

        assertThat(getAddedColumns(newTable.getResolvedSchema(), oldTable.getResolvedSchema()))
                .containsExactly(Column.physical("pv", DataTypes.INT()));
        assertThat(newTable.getExpandedQuery())
                .hasToString(
                        String.format(
                                "SELECT `tmp`.`user_id`, `tmp`.`shop_id`, `tmp`.`ds`, SUM(`tmp`.`payment_amount_cents`) AS `payed_buy_fee_sum`, SUM(1) AS `pv`\n"
                                        + "FROM (SELECT `datagenSource`.`user_id`, `datagenSource`.`shop_id`, DATE_FORMAT(`datagenSource`.`order_created_at`, 'yyyy-MM-dd') AS `ds`, `datagenSource`.`payment_amount_cents`\n"
                                        + "FROM `%s`.`%s`.`datagenSource` AS `datagenSource`) AS `tmp`\n"
                                        + "GROUP BY ROW(`tmp`.`user_id`, `tmp`.`shop_id`, `tmp`.`ds`)",
                                CATALOG, DATABASE));
        assertThat(oldTable.getSerializedRefreshHandler())
                .isEqualTo(newTable.getSerializedRefreshHandler());
        assertThat(oldTable.getDefinitionFreshness()).isEqualTo(newTable.getDefinitionFreshness());

        // verify the restore path is empty
        assertThat(getRefreshHandler().getRestorePath()).isEmpty();
    }

    @Test
    void testDropMaterializedTableInContinuousMode() throws Exception {
        tEnv.executeSql(USERS_SHOPS_DDL);

        assertThat(catalog.getTable(USERS_SHOPS.toObjectPath()).getTableKind())
                .isEqualTo(TableKind.MATERIALIZED_TABLE);
        final JobID refreshJobId = getRefreshJobId();
        waitUntilAllTasksAreRunning(clusterClient, refreshJobId);
        assertThat(getJobStatus(refreshJobId)).isEqualTo(JobStatus.RUNNING);

        // Drop materialized table using drop table statement
        assertThatThrownBy(() -> tEnv.executeSql("DROP TABLE users_shops"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Table with identifier '%s' does not exist.",
                                USERS_SHOPS.asSummaryString()));

        tEnv.executeSql("DROP MATERIALIZED TABLE IF EXISTS users_shops");

        assertThat(catalog.tableExists(USERS_SHOPS.toObjectPath())).isFalse();
        awaitJobStatus(refreshJobId, JobStatus.CANCELED);

        // verify drop materialized table that doesn't exist
        assertThatThrownBy(() -> tEnv.executeSql("DROP MATERIALIZED TABLE users_shops"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Materialized table with identifier %s does not exist.",
                                USERS_SHOPS.asSerializableString()));

        tEnv.executeSql("DROP MATERIALIZED TABLE IF EXISTS users_shops");

        // Drop a table using drop materialized table statement
        assertThatThrownBy(() -> tEnv.executeSql("DROP MATERIALIZED TABLE IF EXISTS datagenSource"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Table %s is not a materialized table, does not support materialized table related operation.",
                                ObjectIdentifier.of(CATALOG, DATABASE, "datagenSource")
                                        .asSerializableString()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedTargets")
    void unsupportedTargetIsRejectedBeforeAnyCatalogChange(
            String description, @Nullable String tableConfigTarget, String expectedMessage)
            throws Exception {
        final TableEnvironment tableEnv;
        if (tableConfigTarget == null) {
            tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            tableEnv.registerCatalog(CATALOG, catalog);
            tableEnv.useCatalog(CATALOG);
            tableEnv.useDatabase(DATABASE);
        } else {
            tableEnv = tEnv;
            tableEnv.getConfig().set(DeploymentOptions.TARGET, tableConfigTarget);
        }
        final Set<JobID> jobsBefore = listJobIds();

        assertThatThrownBy(() -> tableEnv.executeSql(USERS_SHOPS_DDL))
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedMessage);

        assertThat(catalog.tableExists(USERS_SHOPS.toObjectPath())).isFalse();
        assertThat(listJobIds()).isEqualTo(jobsBefore);
    }

    private static Stream<Arguments> unsupportedTargets() {
        return Stream.of(
                Arguments.of(
                        "default environment",
                        null,
                        "Unsupported execution target detected: local. Materialized table"
                                + " refresh jobs need a session cluster: set 'execution.target'"
                                + " to 'remote', 'yarn-session' or 'kubernetes-session' (with"
                                + " 'yarn.application.id' or 'kubernetes.cluster-id')."),
                Arguments.of(
                        "kubernetes-application",
                        "kubernetes-application",
                        "Application-mode materialized table refresh is not supported in"
                                + " this environment: execution target 'kubernetes-application'"
                                + " requires the SQL Gateway. Use a session cluster target such"
                                + " as 'remote', 'yarn-session' or 'kubernetes-session'."),
                Arguments.of(
                        "kubernetes-session without its cluster id",
                        "kubernetes-session",
                        "Execution target 'kubernetes-session' requires"
                                + " 'kubernetes.cluster-id' to be configured to submit or"
                                + " control a materialized table refresh job."));
    }

    @Test
    void resumeOntoAnotherClusterIsRejectedBeforeAnythingChanges(@TempDir Path temporaryPath)
            throws Exception {
        tEnv.executeSql(USERS_SHOPS_DDL);
        waitUntilAllTasksAreRunning(clusterClient, getRefreshJobId());
        setupSavepointDir(temporaryPath);
        tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND");
        final ResolvedCatalogMaterializedTable suspendedTable = getUsersShops();
        final Set<JobID> jobsBefore = listJobIds();

        tEnv.getConfig().set(DeploymentOptions.TARGET, "kubernetes-session");
        tEnv.getConfig().set(KUBERNETES_CLUSTER_ID, "other");

        assertThatThrownBy(() -> tEnv.executeSql("ALTER MATERIALIZED TABLE users_shops RESUME"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "RESUME, ALTER and a redeploy never move a refresh job to another cluster");

        final ResolvedCatalogMaterializedTable unchangedTable = getUsersShops();
        assertThat(unchangedTable.getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
        assertThat(unchangedTable.getSerializedRefreshHandler())
                .isEqualTo(suspendedTable.getSerializedRefreshHandler());
        assertThat(listJobIds()).isEqualTo(jobsBefore);
    }

    private void createBoundedValuesSource(List<Row> data) {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS my_source (\n"
                                + "  order_id BIGINT,\n"
                                + "  user_id BIGINT,\n"
                                + "  shop_id BIGINT,\n"
                                + "  order_created_at STRING\n"
                                + ")\n"
                                + "WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true',\n"
                                + "  'data-id' = '%s'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data)));
    }

    private void setupSavepointDir(Path temporaryPath) {
        tEnv.getConfig()
                .set(
                        CheckpointingOptions.SAVEPOINT_DIRECTORY,
                        "file://" + temporaryPath.toAbsolutePath());
    }

    private ResolvedCatalogMaterializedTable getUsersShops() {
        return (ResolvedCatalogMaterializedTable)
                ((TableEnvironmentInternal) tEnv)
                        .getCatalogManager()
                        .getTableOrError(USERS_SHOPS)
                        .getResolvedTable();
    }

    private ContinuousRefreshHandler getRefreshHandler() throws Exception {
        return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                getUsersShops().getSerializedRefreshHandler(), getClass().getClassLoader());
    }

    private JobID getRefreshJobId() throws Exception {
        return JobID.fromHexString(getRefreshHandler().getJobId());
    }

    private static List<String> getUsersShopsResults() {
        return TestValuesTableFactory.getResultsAsStrings(USERS_SHOPS.getObjectName());
    }

    private static List<Column> getAddedColumns(
            ResolvedSchema newSchema, ResolvedSchema oldSchema) {
        return newSchema.getColumns().stream()
                .filter(column -> !oldSchema.getColumns().contains(column))
                .collect(Collectors.toList());
    }

    private void verifyRefreshJobCreated(JobID jobId, long startTime) throws Exception {
        assertThat(clusterClient.listJobs().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .filteredOn(job -> job.getJobId().equals(jobId))
                .singleElement()
                .satisfies(job -> assertThat(job.getStartTime()).isGreaterThan(startTime));
        assertThat(
                        clusterClient
                                .getJobDetails(jobId)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                .getJobType())
                .isEqualTo(JobType.BATCH);
        awaitJobStatus(jobId, JobStatus.FINISHED);
    }

    private Set<JobID> listJobIds() throws Exception {
        return clusterClient.listJobs().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).stream()
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toSet());
    }

    private JobStatus getJobStatus(JobID jobId) throws Exception {
        return clusterClient.getJobStatus(jobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void awaitJobStatus(JobID jobId, JobStatus expectedStatus) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return getJobStatus(jobId) == expectedStatus;
                    } catch (Exception e) {
                        return false;
                    }
                },
                TIMEOUT,
                PAUSE,
                String.format("Job %s did not reach %s in time.", jobId, expectedStatus));
    }

    private long getCheckpointInterval(JobID jobId) throws Exception {
        return RestMapperUtils.getStrictObjectMapper()
                .valueToTree(sendJobRequest(CheckpointConfigHeaders.getInstance(), jobId))
                .get(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_INTERVAL)
                .asLong();
    }

    private Optional<String> getJobRestoreSavepointPath(JobID jobId) throws Exception {
        return Optional.ofNullable(
                        sendJobRequest(CheckpointingStatisticsHeaders.getInstance(), jobId)
                                .getLatestCheckpoints()
                                .getRestoredCheckpointStatistics())
                .map(RestoredCheckpointStatistics::getExternalPath);
    }

    private <P extends ResponseBody, M extends JobMessageParameters> P sendJobRequest(
            MessageHeaders<EmptyRequestBody, P, M> headers, JobID jobId) throws Exception {
        final M parameters = headers.getUnresolvedMessageParameters();
        parameters.jobPathParameter.resolve(jobId);
        return clusterClient
                .sendRequest(headers, parameters, EmptyRequestBody.getInstance())
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
