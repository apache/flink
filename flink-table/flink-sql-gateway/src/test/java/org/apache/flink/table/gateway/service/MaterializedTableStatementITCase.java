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
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.AbstractMaterializedTableStatementITCase;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.gateway.workflow.EmbeddedRefreshHandler;
import org.apache.flink.table.gateway.workflow.EmbeddedRefreshHandlerSerializer;
import org.apache.flink.table.gateway.workflow.WorkflowInfo;
import org.apache.flink.table.gateway.workflow.scheduler.EmbeddedQuartzScheduler;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.table.api.config.TableConfigOptions.RESOURCES_DOWNLOAD_DIR;
import static org.apache.flink.table.factories.FactoryUtil.WORKFLOW_SCHEDULER_TYPE;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchAllResults;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.WORKFLOW_INFO;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.fromJson;
import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.apache.flink.test.util.TestUtils.waitUntilJobCanceled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for materialized table related statement via {@link SqlGatewayServiceImpl}. Use a separate
 * test class rather than adding test cases to {@link SqlGatewayServiceITCase}, both because the
 * syntax related to Materialized table is relatively independent, and to try to avoid conflicts
 * with the code in {@link SqlGatewayServiceITCase}.
 */
public class MaterializedTableStatementITCase extends AbstractMaterializedTableStatementITCase {

    @Test
    void testCreateMaterializedTableInContinuousMode() throws Exception {
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

        ContinuousRefreshHandler activeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        actualMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());

        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(activeRefreshHandler.getJobId()));

        // verify the background job is running
        String describeJobDDL = String.format("DESCRIBE JOB '%s'", activeRefreshHandler.getJobId());
        OperationHandle describeJobHandle =
                service.executeStatement(sessionHandle, describeJobDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, describeJobHandle);
        List<RowData> jobResults = fetchAllResults(service, sessionHandle, describeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("RUNNING");

        // get checkpoint interval
        long checkpointInterval =
                getCheckpointIntervalConfig(restClusterClient, activeRefreshHandler.getJobId());
        assertThat(checkpointInterval).isEqualTo(30 * 1000);
    }

    @Test
    void testCreateMaterializedTableInContinuousModeWithCustomCheckpointInterval()
            throws Exception {

        // set checkpoint interval to 60 seconds
        long checkpointInterval = 60 * 1000;

        OperationHandle checkpointSetHandle =
                service.executeStatement(
                        sessionHandle,
                        String.format(
                                "SET '%s' = '%d'",
                                CHECKPOINTING_INTERVAL.key(), checkpointInterval),
                        -1,
                        new Configuration());
        awaitOperationTermination(service, sessionHandle, checkpointSetHandle);

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

        ResolvedCatalogMaterializedTable actualMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        ContinuousRefreshHandler activeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        actualMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());

        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(activeRefreshHandler.getJobId()));

        long actualCheckpointInterval =
                getCheckpointIntervalConfig(restClusterClient, activeRefreshHandler.getJobId());
        assertThat(actualCheckpointInterval).isEqualTo(checkpointInterval);
    }

    @Test
    void testCreateMaterializedTableInFullMode() throws Exception {
        String dataId = TestValuesTableFactory.registerData(Collections.emptyList());
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

        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "    'partition.fields.ds.date-formatter' = 'yyyy-MM-dd',\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '1' MINUTE\n"
                        + " REFRESH_MODE = FULL\n"
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

        // verify materialized table is created
        ResolvedCatalogMaterializedTable actualMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        // verify refresh mode
        assertThat(actualMaterializedTable.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.FULL);

        // verify refresh handler
        byte[] serializedHandler = actualMaterializedTable.getSerializedRefreshHandler();
        EmbeddedRefreshHandler embeddedRefreshHandler =
                EmbeddedRefreshHandlerSerializer.INSTANCE.deserialize(
                        serializedHandler, getClass().getClassLoader());
        assertThat(embeddedRefreshHandler.getWorkflowName())
                .isEqualTo(
                        "quartz_job_"
                                + ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSerializableString());

        EmbeddedQuartzScheduler embeddedWorkflowScheduler =
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION
                        .getSqlGatewayRestEndpoint()
                        .getQuartzScheduler();
        JobKey jobKey =
                new JobKey(
                        embeddedRefreshHandler.getWorkflowName(),
                        embeddedRefreshHandler.getWorkflowGroup());

        // verify the job is created
        assertThat(embeddedWorkflowScheduler.getQuartzScheduler().checkExists(jobKey)).isTrue();

        // verify initialization conf
        JobDetail jobDetail = embeddedWorkflowScheduler.getQuartzScheduler().getJobDetail(jobKey);
        String workflowJsonStr = jobDetail.getJobDataMap().getString(WORKFLOW_INFO);
        WorkflowInfo workflowInfo = fromJson(workflowJsonStr, WorkflowInfo.class);
        assertThat(workflowInfo.getInitConfig())
                .containsEntry("k1", "v1")
                .containsEntry("k2", "v2")
                .containsKey("sql-gateway.endpoint.rest.address")
                .containsKey("sql-gateway.endpoint.rest.port")
                .containsKey("table.catalog-store.kind")
                .containsKey("table.catalog-store.file.path")
                .doesNotContainKey(WORKFLOW_SCHEDULER_TYPE.key())
                .doesNotContainKey(RESOURCES_DOWNLOAD_DIR.key());
    }

    @Test
    void testCreateMaterializedTableFailedInInContinuousMode() {
        // create a materialized table with invalid SQL
        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE users_shops"
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
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service, sessionHandle, materializedTableHandle))
                .cause()
                .hasMessageContaining(
                        String.format(
                                "Submit continuous refresh job for materialized table %s occur exception.",
                                ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSerializableString()));

        // verify the materialized table is not created
        assertThatThrownBy(
                        () ->
                                service.getTable(
                                        sessionHandle,
                                        ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")))
                .isInstanceOf(SqlGatewayException.class)
                .hasMessageContaining("Failed to getTable.");
    }

    @Test
    void testAlterMaterializedTableRefresh() throws Exception {
        long timeout = Duration.ofSeconds(20).toMillis();
        long pause = Duration.ofSeconds(2).toMillis();

        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));
        data.add(Row.of(3L, 3L, 3L, "2024-01-02"));

        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table",
                data,
                Collections.singletonMap("ds", "yyyy-MM-dd"),
                RefreshMode.CONTINUOUS);

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
        verifyRefreshJobCreated(restClusterClient, jobId, currentTime);

        // 2. verify the new job overwrite the data
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
                        "Currently, refreshing materialized table only supports referring to char, varchar and string type partition keys. All specified partition keys in partition specs with unsupported types are:\n"
                                + "\n"
                                + "ds2");
    }

    @Test
    void testAlterMaterializedTableSuspendAndResumeInContinuousMode(@TempDir Path temporaryPath)
            throws Exception {
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

        ResolvedCatalogMaterializedTable activeMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        assertThat(activeMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        ContinuousRefreshHandler activeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        activeMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());

        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(activeRefreshHandler.getJobId()));

        // set up savepoint dir
        String savepointDir = temporaryPath.toString();
        String alterJobSavepointDDL =
                String.format(
                        "SET 'execution.checkpointing.savepoint-dir' = 'file://%s'", savepointDir);
        OperationHandle alterMaterializedTableSavepointHandle =
                service.executeStatement(
                        sessionHandle, alterJobSavepointDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSavepointHandle);

        // suspend materialized table
        String alterMaterializedTableSuspendDDL = "ALTER MATERIALIZED TABLE users_shops SUSPEND";
        OperationHandle alterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableSuspendDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSuspendHandle);

        ResolvedCatalogMaterializedTable suspendMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        assertThat(suspendMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.SUSPENDED);

        // verify background job is stopped
        byte[] refreshHandler = suspendMaterializedTable.getSerializedRefreshHandler();
        ContinuousRefreshHandler suspendRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        refreshHandler, getClass().getClassLoader());
        String suspendJobId = suspendRefreshHandler.getJobId();

        String describeJobDDL = String.format("DESCRIBE JOB '%s'", suspendJobId);
        OperationHandle describeJobHandle =
                service.executeStatement(sessionHandle, describeJobDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSuspendHandle);
        List<RowData> jobResults = fetchAllResults(service, sessionHandle, describeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("FINISHED");

        // verify savepoint is created
        assertThat(suspendRefreshHandler.getRestorePath()).isNotEmpty();
        String actualSavepointPath = suspendRefreshHandler.getRestorePath().get();

        // resume materialized table
        String alterMaterializedTableResumeDDL =
                "ALTER MATERIALIZED TABLE users_shops RESUME WITH ('debezium-json.ignore-parse-errors' = 'true')";
        OperationHandle alterMaterializedTableResumeHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableResumeDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableResumeHandle);

        ResolvedCatalogMaterializedTable resumedCatalogMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));
        assertThat(resumedCatalogMaterializedTable.getOptions())
                .doesNotContainKey("debezium-json.ignore-parse-errors");
        assertThat(resumedCatalogMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        waitUntilAllTasksAreRunning(
                restClusterClient,
                JobID.fromHexString(
                        ContinuousRefreshHandlerSerializer.INSTANCE
                                .deserialize(
                                        resumedCatalogMaterializedTable
                                                .getSerializedRefreshHandler(),
                                        getClass().getClassLoader())
                                .getJobId()));

        // verify background job is running
        refreshHandler = resumedCatalogMaterializedTable.getSerializedRefreshHandler();
        ContinuousRefreshHandler resumeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        refreshHandler, getClass().getClassLoader());
        String resumeJobId = resumeRefreshHandler.getJobId();
        String describeResumeJobDDL = String.format("DESCRIBE JOB '%s'", resumeJobId);
        OperationHandle describeResumeJobHandle =
                service.executeStatement(
                        sessionHandle, describeResumeJobDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, describeResumeJobHandle);
        jobResults = fetchAllResults(service, sessionHandle, describeResumeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("RUNNING");

        // verify resumed job is restored from savepoint
        Optional<String> actualRestorePath =
                getJobRestoreSavepointPath(restClusterClient, resumeJobId);
        assertThat(actualRestorePath).isNotEmpty();
        assertThat(actualRestorePath.get()).isEqualTo(actualSavepointPath);
    }

    @Test
    void testAlterMaterializedTableWithoutSavepointDirConfiguredInContinuousMode()
            throws Exception {
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

        ResolvedCatalogMaterializedTable activeMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));
        waitUntilAllTasksAreRunning(
                restClusterClient,
                JobID.fromHexString(
                        ContinuousRefreshHandlerSerializer.INSTANCE
                                .deserialize(
                                        activeMaterializedTable.getSerializedRefreshHandler(),
                                        getClass().getClassLoader())
                                .getJobId()));

        // suspend materialized table
        String alterMaterializedTableSuspendDDL = "ALTER MATERIALIZED TABLE users_shops SUSPEND";
        OperationHandle alterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableSuspendDDL, -1, new Configuration());
        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        alterMaterializedTableSuspendHandle))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Savepoint directory is not configured, can't stop job with savepoint.");
    }

    @Test
    void testAlterMaterializedTableWithRepeatedSuspendAndResumeInContinuousMode(
            @TempDir Path temporaryPath) throws Exception {
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

        ResolvedCatalogMaterializedTable activeMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));
        waitUntilAllTasksAreRunning(
                restClusterClient,
                JobID.fromHexString(
                        ContinuousRefreshHandlerSerializer.INSTANCE
                                .deserialize(
                                        activeMaterializedTable.getSerializedRefreshHandler(),
                                        getClass().getClassLoader())
                                .getJobId()));

        // suspend materialized table
        String savepointDir = temporaryPath.toString();
        String alterJobSavepointDDL =
                String.format(
                        "SET 'execution.checkpointing.savepoint-dir' = 'file://%s'", savepointDir);
        OperationHandle alterMaterializedTableSavepointHandle =
                service.executeStatement(
                        sessionHandle, alterJobSavepointDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSavepointHandle);

        // suspend materialized table
        String alterMaterializedTableSuspendDDL = "ALTER MATERIALIZED TABLE users_shops SUSPEND";
        OperationHandle alterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableSuspendDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSuspendHandle);

        // verify repeated suspend materialized table
        OperationHandle repeatedAlterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableSuspendDDL, -1, new Configuration());
        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        repeatedAlterMaterializedTableSuspendHandle))
                .rootCause()
                .isInstanceOf(SqlExecutionException.class)
                .hasMessageContaining(
                        String.format(
                                "Materialized table %s continuous refresh job has been suspended",
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops")));

        // resume materialized table
        String alterMaterializedTableResumeDDL = "ALTER MATERIALIZED TABLE users_shops RESUME";
        OperationHandle alterMaterializedTableResumeHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableResumeDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableResumeHandle);

        // verify repeated resume materialized table
        OperationHandle repeatedAlterMaterializedTableResumeHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableResumeDDL, -1, new Configuration());
        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        repeatedAlterMaterializedTableResumeHandle))
                .rootCause()
                .isInstanceOf(SqlExecutionException.class)
                .hasMessageContaining(
                        String.format(
                                "Materialized table %s continuous refresh job has been resumed",
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops")));
    }

    @Test
    void testAlterMaterializedTableSuspendAndResumeInFullMode() throws Exception {
        createAndVerifyCreateMaterializedTableWithData(
                "users_shops", Collections.emptyList(), Collections.emptyMap(), RefreshMode.FULL);

        ResolvedCatalogMaterializedTable activeMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        assertThat(activeMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        // suspend materialized table
        String alterMaterializedTableSuspendDDL = "ALTER MATERIALIZED TABLE users_shops SUSPEND";
        OperationHandle alterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableSuspendDDL, -1, new Configuration());

        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSuspendHandle);

        ResolvedCatalogMaterializedTable suspendMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        assertThat(suspendMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.SUSPENDED);

        // verify workflow is suspended
        byte[] refreshHandler = suspendMaterializedTable.getSerializedRefreshHandler();
        EmbeddedRefreshHandler suspendRefreshHandler =
                EmbeddedRefreshHandlerSerializer.INSTANCE.deserialize(
                        refreshHandler, getClass().getClassLoader());

        String workflowName = suspendRefreshHandler.getWorkflowName();
        String workflowGroup = suspendRefreshHandler.getWorkflowGroup();
        EmbeddedQuartzScheduler embeddedWorkflowScheduler =
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION
                        .getSqlGatewayRestEndpoint()
                        .getQuartzScheduler();
        JobKey jobKey = JobKey.jobKey(workflowName, workflowGroup);
        Trigger.TriggerState suspendTriggerState =
                embeddedWorkflowScheduler
                        .getQuartzScheduler()
                        .getTriggerState(TriggerKey.triggerKey(workflowName, workflowGroup));

        assertThat(suspendTriggerState).isEqualTo(Trigger.TriggerState.PAUSED);

        // resume materialized table
        String alterMaterializedTableResumeDDL =
                "ALTER MATERIALIZED TABLE users_shops RESUME WITH ('debezium-json.ignore-parse-errors' = 'true')";
        OperationHandle alterMaterializedTableResumeHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableResumeDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableResumeHandle);

        ResolvedCatalogMaterializedTable resumedCatalogMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        assertThat(resumedCatalogMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        // verify workflow is resumed
        refreshHandler = resumedCatalogMaterializedTable.getSerializedRefreshHandler();
        EmbeddedRefreshHandler resumeRefreshHandler =
                EmbeddedRefreshHandlerSerializer.INSTANCE.deserialize(
                        refreshHandler, getClass().getClassLoader());

        assertThat(resumeRefreshHandler.getWorkflowName()).isEqualTo(workflowName);
        assertThat(resumeRefreshHandler.getWorkflowGroup()).isEqualTo(workflowGroup);

        JobDetail jobDetail = embeddedWorkflowScheduler.getQuartzScheduler().getJobDetail(jobKey);
        Trigger.TriggerState resumedTriggerState =
                embeddedWorkflowScheduler
                        .getQuartzScheduler()
                        .getTriggerState(TriggerKey.triggerKey(workflowName, workflowGroup));
        assertThat(resumedTriggerState).isEqualTo(Trigger.TriggerState.NORMAL);

        WorkflowInfo workflowInfo =
                fromJson((String) jobDetail.getJobDataMap().get(WORKFLOW_INFO), WorkflowInfo.class);
        assertThat(workflowInfo.getDynamicOptions())
                .containsEntry("debezium-json.ignore-parse-errors", "true");
    }

    @Test
    void testAlterMaterializedTableWithRepeatedSuspendAndResumeInFullMode() throws Exception {
        createAndVerifyCreateMaterializedTableWithData(
                "users_shops", Collections.emptyList(), Collections.emptyMap(), RefreshMode.FULL);

        ResolvedCatalogMaterializedTable activeMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops"));

        assertThat(activeMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        // suspend materialized table
        String alterMaterializedTableSuspendDDL = "ALTER MATERIALIZED TABLE users_shops SUSPEND";
        OperationHandle alterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableSuspendDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSuspendHandle);

        // repeated suspend materialized table
        OperationHandle repeatedAlterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableSuspendDDL, -1, new Configuration());
        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        repeatedAlterMaterializedTableSuspendHandle))
                .rootCause()
                .isInstanceOf(SqlExecutionException.class)
                .hasMessageContaining(
                        String.format(
                                "Materialized table %s refresh workflow has been suspended.",
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops")));

        // resume materialized table
        String alterMaterializedTableResumeDDL =
                "ALTER MATERIALIZED TABLE users_shops RESUME WITH ('debezium-json.ignore-parse-errors' = 'true')";
        OperationHandle alterMaterializedTableResumeHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableResumeDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableResumeHandle);

        // verify repeated resume materialized table
        OperationHandle repeatedAlterMaterializedTableResumeHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableResumeDDL, -1, new Configuration());
        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        repeatedAlterMaterializedTableResumeHandle))
                .rootCause()
                .isInstanceOf(SqlExecutionException.class)
                .hasMessageContaining(
                        String.format(
                                "Materialized table %s refresh workflow has been resumed.",
                                ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "users_shops")));
    }

    @Test
    void testDropMaterializedTableInContinuousMode() throws Exception {
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

        // verify materialized table exists
        ResolvedCatalogBaseTable<?> activeMaterializedTable =
                service.getTable(
                        sessionHandle,
                        ObjectIdentifier.of(
                                fileSystemCatalogName, TEST_DEFAULT_DATABASE, "users_shops"));

        assertThat(activeMaterializedTable).isInstanceOf(ResolvedCatalogMaterializedTable.class);

        waitUntilAllTasksAreRunning(
                restClusterClient,
                JobID.fromHexString(
                        ContinuousRefreshHandlerSerializer.INSTANCE
                                .deserialize(
                                        ((ResolvedCatalogMaterializedTable) activeMaterializedTable)
                                                .getSerializedRefreshHandler(),
                                        getClass().getClassLoader())
                                .getJobId()));

        // verify background job is running
        ContinuousRefreshHandler activeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        ((ResolvedCatalogMaterializedTable) activeMaterializedTable)
                                .getSerializedRefreshHandler(),
                        getClass().getClassLoader());
        String describeJobDDL = String.format("DESCRIBE JOB '%s'", activeRefreshHandler.getJobId());
        OperationHandle describeJobHandle =
                service.executeStatement(sessionHandle, describeJobDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, describeJobHandle);
        List<RowData> jobResults = fetchAllResults(service, sessionHandle, describeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("RUNNING");

        // Drop materialized table using drop table statement
        String dropTableUsingMaterializedTableDDL = "DROP TABLE users_shops";
        OperationHandle dropTableUsingMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle, dropTableUsingMaterializedTableDDL, -1, new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        dropTableUsingMaterializedTableHandle))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Table with identifier '%s' does not exist.",
                                ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSummaryString()));

        // drop materialized table
        String dropMaterializedTableDDL = "DROP MATERIALIZED TABLE IF EXISTS users_shops";
        OperationHandle dropMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle, dropMaterializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, dropMaterializedTableHandle);

        // verify materialized table metadata is removed
        assertThatThrownBy(
                        () ->
                                service.getTable(
                                        sessionHandle,
                                        ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")))
                .isInstanceOf(SqlGatewayException.class)
                .hasMessageContaining("Failed to getTable.");

        // verify background job is canceled
        waitUntilJobCanceled(
                JobID.fromHexString(activeRefreshHandler.getJobId()), restClusterClient);

        String describeJobAfterDropDDL =
                String.format("DESCRIBE JOB '%s'", activeRefreshHandler.getJobId());
        OperationHandle describeJobAfterDropHandle =
                service.executeStatement(
                        sessionHandle, describeJobAfterDropDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, describeJobAfterDropHandle);
        List<RowData> jobResultsAfterDrop =
                fetchAllResults(service, sessionHandle, describeJobAfterDropHandle);
        assertThat(jobResultsAfterDrop.get(0).getString(2).toString()).isEqualTo("CANCELED");

        // verify drop materialized table that doesn't exist
        String dropNonExistMaterializedTableDDL = "DROP MATERIALIZED TABLE users_shops";
        OperationHandle dropNonExistTableHandle =
                service.executeStatement(
                        sessionHandle, dropNonExistMaterializedTableDDL, -1, new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service, sessionHandle, dropNonExistTableHandle))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Materialized table with identifier %s does not exist.",
                                ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSerializableString()));

        String dropNonExistMaterializedTableDDL2 = "DROP MATERIALIZED TABLE IF EXISTS users_shops";
        OperationHandle dropNonExistMaterializedTableHandle2 =
                service.executeStatement(
                        sessionHandle, dropNonExistMaterializedTableDDL2, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, dropNonExistMaterializedTableHandle2);

        // Drop a table using drop materialized table statement
        dropMaterializedTableDDL = "DROP MATERIALIZED TABLE IF EXISTS datagenSource";
        OperationHandle dropTableHandle =
                service.executeStatement(
                        sessionHandle, dropMaterializedTableDDL, -1, new Configuration());
        assertThatThrownBy(() -> awaitOperationTermination(service, sessionHandle, dropTableHandle))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Table %s is not a materialized table, does not support materialized table related operation.",
                                ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "datagenSource")
                                        .asSerializableString()));
    }

    @Test
    void testDropMaterializedTableInFullMode() throws Exception {
        createAndVerifyCreateMaterializedTableWithData(
                "users_shops", Collections.emptyList(), Collections.emptyMap(), RefreshMode.FULL);

        JobKey jobKey =
                JobKey.jobKey(
                        "quartz_job_"
                                + ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSerializableString(),
                        "default_group");
        EmbeddedQuartzScheduler embeddedWorkflowScheduler =
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION
                        .getSqlGatewayRestEndpoint()
                        .getQuartzScheduler();

        // verify refresh workflow is created
        assertThat(embeddedWorkflowScheduler.getQuartzScheduler().checkExists(jobKey)).isTrue();

        // Drop materialized table using drop table statement
        String dropTableUsingMaterializedTableDDL = "DROP TABLE users_shops";
        OperationHandle dropTableUsingMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle, dropTableUsingMaterializedTableDDL, -1, new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        dropTableUsingMaterializedTableHandle))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Table with identifier '%s' does not exist.",
                                ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSummaryString()));

        // drop materialized table
        String dropMaterializedTableDDL = "DROP MATERIALIZED TABLE IF EXISTS users_shops";
        OperationHandle dropMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle, dropMaterializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, dropMaterializedTableHandle);

        // verify materialized table metadata is removed
        assertThatThrownBy(
                        () ->
                                service.getTable(
                                        sessionHandle,
                                        ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")))
                .isInstanceOf(SqlGatewayException.class)
                .hasMessageContaining("Failed to getTable.");

        // verify refresh workflow is removed
        assertThat(embeddedWorkflowScheduler.getQuartzScheduler().checkExists(jobKey)).isFalse();
    }

    @Test
    void testDropMaterializedTableWithDeletedRefreshWorkflowInFullMode() throws Exception {
        createAndVerifyCreateMaterializedTableWithData(
                "users_shops", Collections.emptyList(), Collections.emptyMap(), RefreshMode.FULL);

        JobKey jobKey =
                JobKey.jobKey(
                        "quartz_job_"
                                + ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSerializableString(),
                        "default_group");
        EmbeddedQuartzScheduler embeddedWorkflowScheduler =
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION
                        .getSqlGatewayRestEndpoint()
                        .getQuartzScheduler();

        // verify refresh workflow is created
        assertThat(embeddedWorkflowScheduler.getQuartzScheduler().checkExists(jobKey)).isTrue();

        // delete the workflow
        embeddedWorkflowScheduler.deleteScheduleWorkflow(jobKey.getName(), jobKey.getGroup());

        // drop materialized table
        String dropMaterializedTableDDL = "DROP MATERIALIZED TABLE IF EXISTS users_shops";
        OperationHandle dropMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle, dropMaterializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, dropMaterializedTableHandle);

        // verify materialized table metadata is removed
        assertThatThrownBy(
                        () ->
                                service.getTable(
                                        sessionHandle,
                                        ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")))
                .isInstanceOf(SqlGatewayException.class)
                .hasMessageContaining("Failed to getTable.");
    }

    @Test
    void testRefreshMaterializedTableWithStaticPartitionInContinuousMode() throws Exception {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));

        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table",
                data,
                Collections.singletonMap("ds", "yyyy-MM-dd"),
                RefreshMode.CONTINUOUS);

        ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of(
                        fileSystemCatalogName, TEST_DEFAULT_DATABASE, "my_materialized_table");

        // add more data to all data list
        data.add(Row.of(3L, 3L, 3L, "2024-01-01"));
        data.add(Row.of(4L, 4L, 4L, "2024-01-02"));

        // refresh the materialized table with static partition
        long startTime = System.currentTimeMillis();
        Map<String, String> staticPartitions = new HashMap<>();
        staticPartitions.put("ds", "2024-01-02");

        OperationHandle refreshTableHandle =
                service.refreshMaterializedTable(
                        sessionHandle,
                        objectIdentifier.asSerializableString(),
                        false,
                        null,
                        Collections.emptyMap(),
                        staticPartitions,
                        Collections.emptyMap());
        awaitOperationTermination(service, sessionHandle, refreshTableHandle);
        List<RowData> result = fetchAllResults(service, sessionHandle, refreshTableHandle);
        assertThat(result.size()).isEqualTo(1);
        String jobId = result.get(0).getString(0).toString();

        // 1. verify fresh job created
        verifyRefreshJobCreated(restClusterClient, jobId, startTime);

        // 2. verify the new job overwrite the data
        assertThat(
                        fetchTableData(
                                        sessionHandle,
                                        "SELECT * FROM my_materialized_table where ds = '2024-01-02'")
                                .size())
                .isEqualTo(getPartitionSize(data, "2024-01-02"));

        // 3. verify the data of partition '2024-01-01' is not changed
        assertThat(
                        fetchTableData(
                                        sessionHandle,
                                        "SELECT * FROM my_materialized_table where ds = '2024-01-01'")
                                .size())
                .isNotEqualTo(getPartitionSize(data, "2024-01-01"));
    }

    @Test
    void testPeriodicRefreshMaterializedTableWithoutPartitionOptions() throws Exception {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));

        // create materialized table without partition formatter
        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table_without_partition_options",
                data,
                Collections.emptyMap(),
                RefreshMode.CONTINUOUS);

        ObjectIdentifier materializedTableWithoutFormatterIdentifier =
                ObjectIdentifier.of(
                        fileSystemCatalogName,
                        TEST_DEFAULT_DATABASE,
                        "my_materialized_table_without_partition_options");

        // add more data to all data list
        data.add(Row.of(3L, 3L, 3L, "2024-01-01"));
        data.add(Row.of(4L, 4L, 4L, "2024-01-02"));

        long startTime = System.currentTimeMillis();
        OperationHandle periodRefreshTableWithoutFormatterHandle =
                service.refreshMaterializedTable(
                        sessionHandle,
                        materializedTableWithoutFormatterIdentifier.asSerializableString(),
                        true,
                        "2024-01-02 00:00:00",
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        awaitOperationTermination(service, sessionHandle, periodRefreshTableWithoutFormatterHandle);

        List<RowData> periodRefreshWithoutFormatterResult =
                fetchAllResults(service, sessionHandle, periodRefreshTableWithoutFormatterHandle);
        String periodWithoutFormatterJobId =
                periodRefreshWithoutFormatterResult.get(0).getString(0).toString();

        // 1. verify fresh job created
        verifyRefreshJobCreated(restClusterClient, periodWithoutFormatterJobId, startTime);

        // 2. verify the new job overwrite the data
        // for non partition materialized table, the data of partition '2024-01-01' and '2024-01-02'
        // are
        // both changed
        assertThat(
                        fetchTableData(
                                        sessionHandle,
                                        "SELECT * FROM my_materialized_table_without_partition_options where ds = '2024-01-01'")
                                .size())
                .isEqualTo(getPartitionSize(data, "2024-01-01"));

        assertThat(
                        fetchTableData(
                                        sessionHandle,
                                        "SELECT * FROM my_materialized_table_without_partition_options where ds = '2024-01-02'")
                                .size())
                .isEqualTo(getPartitionSize(data, "2024-01-02"));
    }

    @Test
    void testPeriodicRefreshMaterializedTableWithPartitionOptions() throws Exception {
        List<Row> data = new ArrayList<>();

        // create materialized table with partition formatter
        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table",
                data,
                Collections.singletonMap("ds", "yyyy-MM-dd"),
                RefreshMode.FULL);

        ObjectIdentifier materializedTableIdentifier =
                ObjectIdentifier.of(
                        fileSystemCatalogName, TEST_DEFAULT_DATABASE, "my_materialized_table");

        // add more data to all data list
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));
        data.add(Row.of(4L, 4L, 4L, "2024-01-02"));

        // refresh the materialized table with period schedule
        long startTime = System.currentTimeMillis();
        OperationHandle periodRefreshTableHandle =
                service.refreshMaterializedTable(
                        sessionHandle,
                        materializedTableIdentifier.asSerializableString(),
                        true,
                        "2024-01-03 00:00:00",
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        awaitOperationTermination(service, sessionHandle, periodRefreshTableHandle);

        List<RowData> periodRefreshResult =
                fetchAllResults(service, sessionHandle, periodRefreshTableHandle);
        assertThat(periodRefreshResult.size()).isEqualTo(1);

        String periodJobId = periodRefreshResult.get(0).getString(0).toString();

        // 1. verify fresh job created
        verifyRefreshJobCreated(restClusterClient, periodJobId, startTime);

        // 2. verify the new job overwrite the data
        // for partition with formatter, only the data of partition '2024-01-02' is changed and the
        // data of partition '2024-01-01' is not changed
        assertThat(
                        fetchTableData(
                                sessionHandle,
                                "SELECT * FROM my_materialized_table where ds = '2024-01-02'"))
                .size()
                .isEqualTo(getPartitionSize(data, "2024-01-02"));

        assertThat(
                        fetchTableData(
                                        sessionHandle,
                                        "SELECT * FROM my_materialized_table where ds = '2024-01-01'")
                                .size())
                .isNotEqualTo(getPartitionSize(data, "2024-01-01"));
    }

    @Test
    void testRefreshMaterializedTableWithInvalidParameterInContinuousMode() throws Exception {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));

        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table",
                data,
                Collections.singletonMap("ds", "yyyy-MM-dd"),
                RefreshMode.CONTINUOUS);

        ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of(
                        fileSystemCatalogName, TEST_DEFAULT_DATABASE, "my_materialized_table");

        // refresh the materialized table with schedule time not specified
        OperationHandle invalidRefreshTableHandle1 =
                service.refreshMaterializedTable(
                        sessionHandle,
                        objectIdentifier.asSerializableString(),
                        true,
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service, sessionHandle, invalidRefreshTableHandle1))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "The scheduler time must not be null during the periodic refresh of the materialized table %s.",
                                ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "my_materialized_table")
                                        .asSerializableString()));

        // refresh the materialized table with invalid schedule time
        String invalidTime = "20240103 00:00:00.000";
        OperationHandle invalidRefreshTableHandle2 =
                service.refreshMaterializedTable(
                        sessionHandle,
                        objectIdentifier.asSerializableString(),
                        true,
                        invalidTime,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service, sessionHandle, invalidRefreshTableHandle2))
                .rootCause()
                .isInstanceOf(SqlExecutionException.class)
                .hasMessage(
                        String.format(
                                "Failed to parse a valid partition value for the field 'ds' in materialized table %s using the scheduler time '20240103 00:00:00.000' based on the date format 'yyyy-MM-dd HH:mm:ss'.",
                                ObjectIdentifier.of(
                                                fileSystemCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "my_materialized_table")
                                        .asSerializableString()));
    }

    private int getPartitionSize(List<Row> data, String partition) {
        return (int)
                data.stream().filter(row -> row.getField(3).toString().equals(partition)).count();
    }

    private long getCheckpointIntervalConfig(RestClusterClient<?> restClusterClient, String jobId)
            throws Exception {
        CheckpointConfigInfo checkpointConfigInfo =
                sendJobRequest(
                        restClusterClient,
                        CheckpointConfigHeaders.getInstance(),
                        EmptyRequestBody.getInstance(),
                        jobId);
        return RestMapperUtils.getStrictObjectMapper()
                .readTree(
                        RestMapperUtils.getStrictObjectMapper()
                                .writeValueAsString(checkpointConfigInfo))
                .get("interval")
                .asLong();
    }

    private Optional<String> getJobRestoreSavepointPath(
            RestClusterClient<?> restClusterClient, String jobId) throws Exception {
        CheckpointingStatistics checkpointingStatistics =
                sendJobRequest(
                        restClusterClient,
                        CheckpointingStatisticsHeaders.getInstance(),
                        EmptyRequestBody.getInstance(),
                        jobId);

        CheckpointingStatistics.RestoredCheckpointStatistics restoredCheckpointStatistics =
                checkpointingStatistics.getLatestCheckpoints().getRestoredCheckpointStatistics();
        return restoredCheckpointStatistics != null
                ? Optional.ofNullable(restoredCheckpointStatistics.getExternalPath())
                : Optional.empty();
    }

    private static <M extends JobMessageParameters, R extends RequestBody, P extends ResponseBody>
            P sendJobRequest(
                    RestClusterClient<?> restClusterClient,
                    MessageHeaders<R, P, M> headers,
                    R requestBody,
                    String jobId)
                    throws Exception {
        M jobMessageParameters = headers.getUnresolvedMessageParameters();
        jobMessageParameters.jobPathParameter.resolve(JobID.fromHexString(jobId));

        return restClusterClient
                .sendRequest(headers, jobMessageParameters, requestBody)
                .get(5, TimeUnit.SECONDS);
    }
}
