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
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_KIND;
import static org.apache.flink.table.factories.FactoryUtil.WORKFLOW_SCHEDULER_TYPE;
import static org.apache.flink.table.gateway.service.MaterializedTableTestUtils.executeStatement;
import static org.apache.flink.table.gateway.service.MaterializedTableTestUtils.getContinuousRefreshHandler;
import static org.apache.flink.table.gateway.service.MaterializedTableTestUtils.getTable;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchAllResults;
import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for converting a regular table in place to a materialized table via CREATE OR ALTER
 * MATERIALIZED TABLE.
 *
 * <p>This case runs against its own gateway with {@link
 * TableConfigOptions#MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED} turned on, so that the
 * default-disabled behavior stays covered by the other materialized table cases and the planner
 * unit tests.
 */
class MaterializedTableConversionITCase {

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
            new SqlGatewayServiceExtension(
                    MaterializedTableConversionITCase::getGatewayConfiguration);

    @RegisterExtension
    @Order(3)
    static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    @RegisterExtension
    @Order(4)
    static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private static SqlGatewayServiceImpl service;
    private static SessionEnvironment defaultSessionEnvironment;
    private static Path baseCatalogPath;

    private String fileSystemCatalogPath;
    private String fileSystemCatalogName;

    private SessionHandle sessionHandle;
    private RestClusterClient<?> restClusterClient;

    private static Configuration getGatewayConfiguration() {
        final Configuration configuration =
                new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.set(
                TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);
        return configuration;
    }

    @BeforeAll
    static void setUp(@TempDir Path temporaryFolder) throws Exception {
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();

        Path fileCatalogStore = temporaryFolder.resolve(FILE_CATALOG_STORE);
        Files.createDirectory(fileCatalogStore);
        Map<String, String> catalogStoreOptions = new HashMap<>();
        catalogStoreOptions.put(TABLE_CATALOG_STORE_KIND.key(), "file");
        catalogStoreOptions.put("table.catalog-store.file.path", fileCatalogStore.toString());

        baseCatalogPath = temporaryFolder.resolve(TEST_CATALOG_PREFIX);
        Files.createDirectory(baseCatalogPath);

        Map<String, String> workflowSchedulerConfig = new HashMap<>();
        workflowSchedulerConfig.put(WORKFLOW_SCHEDULER_TYPE.key(), "embedded");
        workflowSchedulerConfig.put(
                "sql-gateway.endpoint.rest.address",
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress());
        workflowSchedulerConfig.put(
                "sql-gateway.endpoint.rest.port",
                String.valueOf(SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()));

        defaultSessionEnvironment =
                SessionEnvironment.newBuilder()
                        .addSessionConfig(catalogStoreOptions)
                        .addSessionConfig(workflowSchedulerConfig)
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .build();
    }

    @BeforeEach
    void before(@InjectClusterClient RestClusterClient<?> injectClusterClient) throws Exception {
        String randomStr = String.valueOf(COUNTER.incrementAndGet());
        Path fileCatalogPath = baseCatalogPath.resolve(randomStr);
        Files.createDirectory(fileCatalogPath);
        Files.createDirectory(fileCatalogPath.resolve(TEST_DEFAULT_DATABASE));

        fileSystemCatalogPath = fileCatalogPath.toString();
        fileSystemCatalogName = TEST_CATALOG_PREFIX + randomStr;
        sessionHandle = initializeSession();

        restClusterClient = injectClusterClient;
    }

    @Test
    void testConvertTableToMaterializedTableInContinuousMode() throws Exception {
        // pre-create a regular table that will be converted in place to a materialized table
        String createRegularTableDDL =
                "CREATE TABLE users_shops (\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  ds STRING,\n"
                        + "  payed_buy_fee_sum BIGINT,\n"
                        + "  pv INT NOT NULL\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '10'\n"
                        + ")";
        OperationHandle createTableHandle =
                executeStatement(service, sessionHandle, createRegularTableDDL);
        awaitOperationTermination(service, sessionHandle, createTableHandle);

        String convertDDL =
                "CREATE OR ALTER MATERIALIZED TABLE users_shops"
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
        OperationHandle convertHandle = executeStatement(service, sessionHandle, convertDDL);
        awaitOperationTermination(service, sessionHandle, convertHandle);

        // the entry is now a materialized table with an active continuous refresh job
        ObjectIdentifier userShopsIdentifier = getObjectIdentifier("users_shops");
        ResolvedCatalogMaterializedTable actualMaterializedTable =
                getTable(service, sessionHandle, userShopsIdentifier);
        assertThat(actualMaterializedTable.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);
        assertThat(actualMaterializedTable.getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(actualMaterializedTable.getSerializedRefreshHandler()).isNotEmpty();

        ContinuousRefreshHandler activeRefreshHandler =
                getContinuousRefreshHandler(actualMaterializedTable, getClass().getClassLoader());
        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(activeRefreshHandler.getJobId()));

        // verify the background refresh job is running
        String describeJobDDL = String.format("DESCRIBE JOB '%s'", activeRefreshHandler.getJobId());
        OperationHandle describeJobHandle =
                executeStatement(service, sessionHandle, describeJobDDL);
        awaitOperationTermination(service, sessionHandle, describeJobHandle);
        List<RowData> jobResults = fetchAllResults(service, sessionHandle, describeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("RUNNING");

        dropMaterializedTable(userShopsIdentifier);
    }

    @Test
    void testConvertTableLeftSuspendedWhenRefreshJobFailsToStart() throws Exception {
        String createRegularTableDDL =
                "CREATE TABLE users_shops (\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  ds STRING,\n"
                        + "  payed_buy_fee_sum BIGINT,\n"
                        + "  pv INT NOT NULL\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '10'\n"
                        + ")";
        OperationHandle createTableHandle =
                executeStatement(service, sessionHandle, createRegularTableDDL);
        awaitOperationTermination(service, sessionHandle, createTableHandle);

        // 'json' cannot encode the updating changelog produced by the GROUP BY, so the continuous
        // refresh job fails to start after the catalog entry has already been swapped to a
        // materialized table
        String convertDDL =
                "CREATE OR ALTER MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'json'\n"
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
        OperationHandle convertHandle = executeStatement(service, sessionHandle, convertDDL);

        assertThatThrownBy(() -> awaitOperationTermination(service, sessionHandle, convertHandle))
                .cause()
                .hasMessageContaining(
                        "Failed to start the continuous refresh job when converting table")
                .hasMessageContaining("left in SUSPENDED status");

        // the conversion is not rolled back: the table stays a materialized table but suspended
        ObjectIdentifier userShopsIdentifier = getObjectIdentifier("users_shops");
        ResolvedCatalogMaterializedTable suspendedMaterializedTable =
                getTable(service, sessionHandle, userShopsIdentifier);
        assertThat(suspendedMaterializedTable.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);
        assertThat(suspendedMaterializedTable.getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
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

    private ObjectIdentifier getObjectIdentifier(String name) {
        return ObjectIdentifier.of(fileSystemCatalogName, TEST_DEFAULT_DATABASE, name);
    }

    private void dropMaterializedTable(ObjectIdentifier objectIdentifier) throws Exception {
        String dropMaterializedTableDDL =
                String.format(
                        "DROP MATERIALIZED TABLE %s", objectIdentifier.asSerializableString());
        OperationHandle dropHandle =
                executeStatement(service, sessionHandle, dropMaterializedTableDDL);
        awaitOperationTermination(service, sessionHandle, dropHandle);
    }
}
