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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.AbstractMaterializedTableStatementITCase;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.materializedtable.RefreshMaterializedTableHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableParameters;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.rest.util.TestingRestClient;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.table.gateway.rest.util.TestingRestClient.getTestingRestClient;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in materialized
 * table related cases.
 */
public class SqlGatewayRestEndpointMaterializedTableITCase
        extends AbstractMaterializedTableStatementITCase {

    private static TestingRestClient restClient;

    @BeforeAll
    static void setup() throws Exception {
        restClient = getTestingRestClient();
    }

    @Test
    void testStaticPartitionRefreshMaterializedTableViaRestAPI() throws Exception {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));

        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table", data, Collections.emptyMap(), RefreshMode.CONTINUOUS);

        RefreshMaterializedTableHeaders refreshMaterializedTableHeaders =
                new RefreshMaterializedTableHeaders();

        RefreshMaterializedTableParameters refreshMaterializedTableParameters =
                new RefreshMaterializedTableParameters(
                        sessionHandle,
                        ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "my_materialized_table")
                                .asSerializableString());

        Map<String, String> staticPartitions = new HashMap<>();
        staticPartitions.put("ds", "2024-01-02");
        RefreshMaterializedTableRequestBody refreshMaterializedTableRequestBody =
                new RefreshMaterializedTableRequestBody(
                        false,
                        null,
                        Collections.emptyMap(),
                        staticPartitions,
                        Collections.emptyMap());

        long startTime = System.currentTimeMillis();
        // refresh materialized table
        RefreshMaterializedTableResponseBody response =
                restClient
                        .sendRequest(
                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                                refreshMaterializedTableHeaders,
                                refreshMaterializedTableParameters,
                                refreshMaterializedTableRequestBody)
                        .get();

        assertThat(response.getOperationHandle()).isNotNull();

        // verify refresh job is created
        OperationHandle operationHandle =
                new OperationHandle(UUID.fromString(response.getOperationHandle()));

        CommonTestUtils.waitUtil(
                () ->
                        SQL_GATEWAY_SERVICE_EXTENSION
                                .getService()
                                .getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(100),
                "Failed to wait operation finish.");

        // fetch all results
        FetchResultsResponseBody fetchResultsResponseBody =
                fetchResults(sessionHandle, operationHandle);
        List<RowData> results = fetchResultsResponseBody.getResults().getData();

        String jobId = results.get(0).getString(0).toString();
        verifyRefreshJobCreated(restClusterClient, jobId, startTime);

        GenericMapData clusterInfo = ((GenericMapData) results.get(0).getMap(1));
        assertThat(clusterInfo.get(StringData.fromString(TARGET.key())))
                .isEqualTo(StringData.fromString("remote"));
    }

    @Test
    void testPeriodicRefreshMaterializedTableViaRestAPI() throws Exception {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));

        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table",
                data,
                Collections.singletonMap("ds", "yyyy-MM-dd"),
                RefreshMode.CONTINUOUS);

        RefreshMaterializedTableHeaders refreshMaterializedTableHeaders =
                new RefreshMaterializedTableHeaders();

        RefreshMaterializedTableParameters refreshMaterializedTableParameters =
                new RefreshMaterializedTableParameters(
                        sessionHandle,
                        ObjectIdentifier.of(
                                        fileSystemCatalogName,
                                        TEST_DEFAULT_DATABASE,
                                        "my_materialized_table")
                                .asSerializableString());

        Map<String, String> staticPartitions = new HashMap<>();
        RefreshMaterializedTableRequestBody refreshMaterializedTableRequestBody =
                new RefreshMaterializedTableRequestBody(
                        true,
                        "2024-01-02 00:00:00",
                        Collections.emptyMap(),
                        staticPartitions,
                        Collections.emptyMap());

        long startTime = System.currentTimeMillis();
        // refresh materialized table
        RefreshMaterializedTableResponseBody response =
                restClient
                        .sendRequest(
                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                                refreshMaterializedTableHeaders,
                                refreshMaterializedTableParameters,
                                refreshMaterializedTableRequestBody)
                        .get();

        assertThat(response.getOperationHandle()).isNotNull();

        // verify refresh job is created
        OperationHandle operationHandle =
                new OperationHandle(UUID.fromString(response.getOperationHandle()));

        CommonTestUtils.waitUtil(
                () ->
                        SQL_GATEWAY_SERVICE_EXTENSION
                                .getService()
                                .getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(100),
                "Failed to wait operation finish.");

        // fetch all results
        FetchResultsResponseBody fetchResultsResponseBody =
                fetchResults(sessionHandle, operationHandle);
        List<RowData> results = fetchResultsResponseBody.getResults().getData();

        String jobId = results.get(0).getString(0).toString();
        verifyRefreshJobCreated(restClusterClient, jobId, startTime);

        GenericMapData clusterInfo = ((GenericMapData) results.get(0).getMap(1));
        assertThat(clusterInfo.get(StringData.fromString(TARGET.key())))
                .isEqualTo(StringData.fromString("remote"));
    }

    FetchResultsResponseBody fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle) throws Exception {
        FetchResultsMessageParameters fetchResultsMessageParameters =
                new FetchResultsMessageParameters(
                        sessionHandle, operationHandle, 0L, RowFormat.JSON);
        CompletableFuture<FetchResultsResponseBody> response =
                restClient.sendRequest(
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                        FetchResultsHeaders.getDefaultInstance(),
                        fetchResultsMessageParameters,
                        EmptyRequestBody.getInstance());
        return response.get();
    }
}
