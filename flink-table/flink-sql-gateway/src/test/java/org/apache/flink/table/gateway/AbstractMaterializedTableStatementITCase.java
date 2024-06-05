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

package org.apache.flink.table.gateway;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_KIND;
import static org.apache.flink.table.factories.FactoryUtil.WORKFLOW_SCHEDULER_TYPE;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchAllResults;
import static org.assertj.core.api.Assertions.assertThat;

/** Base ITCase tests for materialized table. */
public abstract class AbstractMaterializedTableStatementITCase {

    private static final String FILE_CATALOG_STORE = "file_store";
    private static final String TEST_CATALOG_PREFIX = "test_catalog";
    protected static final String TEST_DEFAULT_DATABASE = "test_db";

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
    protected static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    protected static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    @RegisterExtension
    @Order(4)
    protected static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    protected static SqlGatewayServiceImpl service;
    private static SessionEnvironment defaultSessionEnvironment;
    private static Path baseCatalogPath;

    private String fileSystemCatalogPath;
    protected String fileSystemCatalogName;

    protected SessionHandle sessionHandle;

    protected RestClusterClient<?> restClusterClient;

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

        // workflow scheduler config
        Map<String, String> workflowSchedulerConfig = new HashMap<>();
        workflowSchedulerConfig.put(WORKFLOW_SCHEDULER_TYPE.key(), "embedded");
        workflowSchedulerConfig.put(
                "sql-gateway.endpoint.rest.address",
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress());
        workflowSchedulerConfig.put(
                "sql-gateway.endpoint.rest.port",
                String.valueOf(SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()));

        // Session conf for testing purpose
        Map<String, String> testConf = new HashMap<>();
        testConf.put("k1", "v1");
        testConf.put("k2", "v2");

        defaultSessionEnvironment =
                SessionEnvironment.newBuilder()
                        .addSessionConfig(catalogStoreOptions)
                        .addSessionConfig(workflowSchedulerConfig)
                        .addSessionConfig(testConf)
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .build();
    }

    @BeforeEach
    void before(@InjectClusterClient RestClusterClient<?> injectClusterClient) throws Exception {
        String randomStr = String.valueOf(COUNTER.incrementAndGet());
        // initialize test-filesystem catalog path with random uuid
        Path fileCatalogPath = baseCatalogPath.resolve(randomStr);
        Files.createDirectory(fileCatalogPath);
        Path dbPath = fileCatalogPath.resolve(TEST_DEFAULT_DATABASE);
        Files.createDirectory(dbPath);

        fileSystemCatalogPath = fileCatalogPath.toString();
        fileSystemCatalogName = TEST_CATALOG_PREFIX + randomStr;
        // initialize session handle, create test-filesystem catalog and register it to catalog
        // store
        sessionHandle = initializeSession();

        // init rest cluster client
        restClusterClient = injectClusterClient;
    }

    @AfterEach
    void after() throws Exception {
        Set<TableInfo> tableInfos =
                service.listTables(
                        sessionHandle,
                        fileSystemCatalogName,
                        TEST_DEFAULT_DATABASE,
                        Collections.singleton(CatalogBaseTable.TableKind.TABLE));

        // drop all materialized tables
        for (TableInfo tableInfo : tableInfos) {
            ResolvedCatalogBaseTable<?> resolvedTable =
                    service.getTable(sessionHandle, tableInfo.getIdentifier());
            if (CatalogBaseTable.TableKind.MATERIALIZED_TABLE == resolvedTable.getTableKind()) {
                String dropTableDDL =
                        String.format(
                                "DROP MATERIALIZED TABLE %s",
                                tableInfo.getIdentifier().asSerializableString());
                OperationHandle dropTableHandle =
                        service.executeStatement(
                                sessionHandle, dropTableDDL, -1, new Configuration());
                awaitOperationTermination(service, sessionHandle, dropTableHandle);
            }
        }
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

    public void createAndVerifyCreateMaterializedTableWithData(
            String materializedTableName,
            List<Row> data,
            Map<String, String> partitionFormatter,
            CatalogMaterializedTable.RefreshMode refreshMode)
            throws Exception {
        long timeout = Duration.ofSeconds(20).toMillis();
        long pause = Duration.ofSeconds(2).toMillis();

        String dataId = TestValuesTableFactory.registerData(data);
        String sourceDdl =
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
                        dataId);
        OperationHandle sourceHandle =
                service.executeStatement(sessionHandle, sourceDdl, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, sourceHandle);

        String partitionFields =
                partitionFormatter != null && !partitionFormatter.isEmpty()
                        ? partitionFormatter.entrySet().stream()
                                .map(
                                        e ->
                                                String.format(
                                                        "'partition.fields.%s.date-formatter' = '%s'",
                                                        e.getKey(), e.getValue()))
                                .collect(Collectors.joining(",\n", "", ",\n"))
                        : "\n";
        String materializedTableDDL =
                String.format(
                        "CREATE MATERIALIZED TABLE %s"
                                + " PARTITIONED BY (ds)\n"
                                + " WITH(\n"
                                + "    %s"
                                + "   'format' = 'debezium-json'\n"
                                + " )\n"
                                + " FRESHNESS = INTERVAL '30' SECOND\n"
                                + " REFRESH_MODE = %s\n"
                                + " AS SELECT \n"
                                + "  user_id,\n"
                                + "  shop_id,\n"
                                + "  ds,\n"
                                + "  COUNT(order_id) AS order_cnt\n"
                                + " FROM (\n"
                                + "    SELECT user_id, shop_id, order_created_at AS ds, order_id FROM my_source"
                                + " ) AS tmp\n"
                                + " GROUP BY (user_id, shop_id, ds)",
                        materializedTableName, partitionFields, refreshMode.toString());

        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        // verify data exists in materialized table
        CommonTestUtils.waitUtil(
                () ->
                        fetchTableData(
                                                sessionHandle,
                                                String.format(
                                                        "SELECT * FROM %s", materializedTableName))
                                        .size()
                                == data.size(),
                Duration.ofMillis(timeout),
                Duration.ofMillis(pause),
                "Failed to verify the data in materialized table.");
    }

    public List<RowData> fetchTableData(SessionHandle sessionHandle, String query) {
        OperationHandle queryHandle =
                service.executeStatement(sessionHandle, query, -1, new Configuration());

        return fetchAllResults(service, sessionHandle, queryHandle);
    }

    public void verifyRefreshJobCreated(
            RestClusterClient<?> restClusterClient, String jobId, long startTime) throws Exception {
        long timeout = Duration.ofSeconds(20).toMillis();
        long pause = Duration.ofSeconds(2).toMillis();

        // 1. verify a new job is created
        Optional<JobStatusMessage> job =
                restClusterClient.listJobs().get(timeout, TimeUnit.MILLISECONDS).stream()
                        .filter(j -> j.getJobId().toString().equals(jobId))
                        .findFirst();
        assertThat(job).isPresent();
        assertThat(job.get().getStartTime()).isGreaterThan(startTime);

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
    }
}
