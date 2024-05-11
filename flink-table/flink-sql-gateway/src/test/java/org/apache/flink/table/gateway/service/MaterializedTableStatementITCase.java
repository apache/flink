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

package org.apache.flink.table.gateway.service;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_KIND;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchAllResults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for materialized table related statement via {@link SqlGatewayServiceImpl}. Use a separate
 * test class rather than adding test cases to {@link SqlGatewayServiceITCase}, both because the
 * syntax related to Materialized table is relatively independent, and to try to avoid conflicts
 * with the code in {@link SqlGatewayServiceITCase}.
 */
public class MaterializedTableStatementITCase {

    private static final String FILE_CATALOG_STORE = "file_store";
    private static final String TEST_CATALOG_PREFIX = "test_catalog";
    private static final String TEST_DEFAULT_DATABASE = "test_db";

    private static final AtomicLong COUNTER = new AtomicLong(0);

    @RegisterExtension
    @Order(1)
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @RegisterExtension
    @Order(2)
    static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    private static SqlGatewayServiceImpl service;
    private static SessionEnvironment defaultSessionEnvironment;
    private static Path baseCatalogPath;

    private String fileSystemCatalogPath;
    private String fileSystemCatalogName;

    @BeforeAll
    static void setUp(@TempDir Path temporaryFolder) throws Exception {
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();

        // initialize file catalog store path
        Path fileCatalogStore = temporaryFolder.resolve(FILE_CATALOG_STORE);
        Files.createDirectory(fileCatalogStore);
        Map<String, String> catalogStoreOptions = new HashMap<>();
        catalogStoreOptions.put(TABLE_CATALOG_STORE_KIND.key(), "file");
        catalogStoreOptions.put("table.catalog-store.file.path", fileCatalogStore.toString());

        // initialize test-filesystem catalog base path
        baseCatalogPath = temporaryFolder.resolve(TEST_CATALOG_PREFIX);
        Files.createDirectory(baseCatalogPath);

        defaultSessionEnvironment =
                SessionEnvironment.newBuilder()
                        .addSessionConfig(catalogStoreOptions)
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .build();
    }

    @BeforeEach
    void before() throws Exception {
        String randomStr = String.valueOf(COUNTER.incrementAndGet());
        // initialize test-filesystem catalog path with random uuid
        Path fileCatalogPath = baseCatalogPath.resolve(randomStr);
        Files.createDirectory(fileCatalogPath);
        Path dbPath = fileCatalogPath.resolve(TEST_DEFAULT_DATABASE);
        Files.createDirectory(dbPath);

        fileSystemCatalogPath = fileCatalogPath.toString();
        fileSystemCatalogName = TEST_CATALOG_PREFIX + randomStr;
    }

    @Test
    void testCreateMaterializedTableInContinuousMode() throws Exception {
        // initialize session handle, create test-filesystem catalog and register it to catalog
        // store
        SessionHandle sessionHandle = initializeSession();

        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)";
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        // validate materialized table: schema, refresh mode, refresh status, refresh handler,
        // doesn't check the data because it generates randomly.
        ResolvedCatalogMaterializedTable actualMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        // Expected schema
        ResolvedSchema expectedSchema =
                ResolvedSchema.of(
                        Arrays.asList(
                                Column.physical("user_id", DataTypes.BIGINT()),
                                Column.physical("shop_id", DataTypes.BIGINT()),
                                Column.physical("ds", DataTypes.STRING()),
                                Column.physical("payed_buy_fee_sum", DataTypes.BIGINT()),
                                Column.physical("pv", DataTypes.INT().notNull())));

        assertThat(actualMaterializedTable.getResolvedSchema()).isEqualTo(expectedSchema);
        assertThat(actualMaterializedTable.getFreshness()).isEqualTo(Duration.ofSeconds(30));
        assertThat(actualMaterializedTable.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC);
        assertThat(actualMaterializedTable.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.CONTINUOUS);
        assertThat(actualMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);
        assertThat(actualMaterializedTable.getRefreshHandlerDescription()).isNotEmpty();
        assertThat(actualMaterializedTable.getSerializedRefreshHandler()).isNotEmpty();
    }

    @Test
    void testCreateMaterializedTableInFullMode() {
        // initialize session handle, create test-filesystem catalog and register it to catalog
        // store
        SessionHandle sessionHandle = initializeSession();

        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '1' DAY\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)";
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service, sessionHandle, materializedTableHandle))
                .rootCause()
                .isInstanceOf(SqlExecutionException.class)
                .hasMessage(
                        "Only support create materialized table in continuous refresh mode currently.");
    }

    @Test
    void testAlterMaterializedTableRefresh(
            @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
        long timeout = Duration.ofSeconds(20).toMillis();
        long pause = Duration.ofSeconds(2).toMillis();
        // initialize session handle, create test-filesystem catalog and register it to catalog
        // store
        SessionHandle sessionHandle = initializeSession();

        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));
        data.add(Row.of(3L, 3L, 3L, "2024-01-02"));
        String dataId = TestValuesTableFactory.registerData(data);

        String sourceDdl =
                String.format(
                        "CREATE TABLE my_source (\n"
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
                        dataId);
        OperationHandle sourceHandle =
                service.executeStatement(sessionHandle, sourceDdl, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, sourceHandle);

        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE my_materialized_table"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '2' SECOND\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  COUNT(order_id) AS order_cnt\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, order_created_at AS ds, order_id FROM my_source"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)";

        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        // verify data exists in materialized table
        CommonTestUtils.waitUtil(
                () ->
                        fetchTableData(sessionHandle, "SELECT * FROM my_materialized_table").size()
                                == data.size(),
                Duration.ofMillis(timeout),
                Duration.ofMillis(pause),
                "Failed to verify the data in materialized table.");
        assertThat(
                        fetchTableData(
                                        sessionHandle,
                                        "SELECT * FROM my_materialized_table where ds = '2024-01-02'")
                                .size())
                .isEqualTo(2);

        // remove the last element
        data.remove(2);

        long currentTime = System.currentTimeMillis();
        String alterStatement =
                "ALTER MATERIALIZED TABLE my_materialized_table REFRESH PARTITION (ds = '2024-01-02')";
        OperationHandle alterHandle =
                service.executeStatement(sessionHandle, alterStatement, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterHandle);
        List<RowData> result = fetchAllResults(service, sessionHandle, alterHandle);
        assertThat(result.size()).isEqualTo(1);
        String jobId = result.get(0).getString(0).toString();

        // 1. verify a new job is created
        Optional<JobStatusMessage> job =
                restClusterClient.listJobs().get(timeout, TimeUnit.MILLISECONDS).stream()
                        .filter(j -> j.getJobId().toString().equals(jobId))
                        .findFirst();
        assertThat(job).isPresent();
        assertThat(job.get().getStartTime()).isGreaterThan(currentTime);

        // 2. verify the new job is a batch job
        JobDetailsInfo jobDetailsInfo =
                restClusterClient
                        .getJobDetails(JobID.fromHexString(jobId))
                        .get(timeout, TimeUnit.MILLISECONDS);
        assertThat(jobDetailsInfo.getJobType()).isEqualTo(JobType.BATCH);

        // 3. verify the new job is finished
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return JobStatus.FINISHED.equals(
                                restClusterClient
                                        .getJobStatus(JobID.fromHexString(jobId))
                                        .get(5, TimeUnit.SECONDS));
                    } catch (Exception ignored) {
                    }
                    return false;
                },
                Duration.ofMillis(timeout),
                Duration.ofMillis(pause),
                "Failed to verify whether the job is finished.");

        // 4. verify the new job overwrite the data
        CommonTestUtils.waitUtil(
                () ->
                        fetchTableData(sessionHandle, "SELECT * FROM my_materialized_table").size()
                                == data.size(),
                Duration.ofMillis(timeout),
                Duration.ofMillis(pause),
                "Failed to verify the data in materialized table.");
        assertThat(
                        fetchTableData(
                                        sessionHandle,
                                        "SELECT * FROM my_materialized_table where ds = '2024-01-02'")
                                .size())
                .isEqualTo(1);
    }

    @Test
    void testAlterMaterializedTableRefreshWithInvalidPartitionSpec() throws Exception {
        // initialize session handle, create test-filesystem catalog and register it to catalog
        // store
        SessionHandle sessionHandle = initializeSession();

        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds1, ds2)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
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
                        + " GROUP BY (user_id, shop_id, ds1, ds2)";

        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        // CASE 1: check unknown partition keys
        String alterStatementWithUnknownPartitionKey =
                "ALTER MATERIALIZED TABLE users_shops REFRESH PARTITION (ds3 = '2024-01-01')";
        OperationHandle alterStatementWithUnknownPartitionKeyHandle =
                service.executeStatement(
                        sessionHandle,
                        alterStatementWithUnknownPartitionKey,
                        -1,
                        new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        alterStatementWithUnknownPartitionKeyHandle))
                .isInstanceOf(SqlExecutionException.class)
                .rootCause()
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
        String alterStatementWithNonStringPartitionKey =
                "ALTER MATERIALIZED TABLE users_shops REFRESH PARTITION (ds2 = 5)";
        OperationHandle alterStatementWithNonStringPartitionKeyHandle =
                service.executeStatement(
                        sessionHandle,
                        alterStatementWithNonStringPartitionKey,
                        -1,
                        new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        alterStatementWithNonStringPartitionKeyHandle))
                .isInstanceOf(SqlExecutionException.class)
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Currently, manually refreshing materialized table only supports specifying char and string type partition keys. All specific partition keys with unsupported types are:\n"
                                + "\n"
                                + "ds2");
    }

    private SessionHandle initializeSession() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String catalogDDL =
                String.format(
                        "CREATE CATALOG %s\n"
                                + "WITH (\n"
                                + "  'type' = 'test-filesystem',\n"
                                + "  'path' = '%s',\n"
                                + "  'default-database' = '%s'\n"
                                + "  )",
                        fileSystemCatalogName, fileSystemCatalogPath, TEST_DEFAULT_DATABASE);
        service.configureSession(sessionHandle, catalogDDL, -1);
        service.configureSession(
                sessionHandle, String.format("USE CATALOG %s", fileSystemCatalogName), -1);

        // create source table
        String dataGenSource =
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
                        + ")";
        service.configureSession(sessionHandle, dataGenSource, -1);
        return sessionHandle;
    }

    private List<RowData> fetchTableData(SessionHandle sessionHandle, String query) {
        OperationHandle queryHandle =
                service.executeStatement(sessionHandle, query, -1, new Configuration());

        return fetchAllResults(service, sessionHandle, queryHandle);
    }
}
