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

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.operation.CancelOperationHeaders;
import org.apache.flink.table.gateway.rest.header.operation.CloseOperationHeaders;
import org.apache.flink.table.gateway.rest.header.operation.GetOperationStatusHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationMessageParameters;
import org.apache.flink.table.gateway.rest.message.operation.OperationStatusResponseBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.service.result.NotReadyResult;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in operation
 * related cases.
 */
class OperationRelatedITCase extends RestAPIITCaseBase {

    private static final String sessionName = "test";
    private static final Map<String, String> properties = new HashMap<>();

    static {
        properties.put("k1", "v1");
        properties.put("k2", "v2");
    }

    private static final OpenSessionHeaders openSessionHeaders = OpenSessionHeaders.getInstance();
    private static final OpenSessionRequestBody openSessionRequestBody =
            new OpenSessionRequestBody(sessionName, properties);
    private static final EmptyMessageParameters emptyParameters =
            EmptyMessageParameters.getInstance();

    private static final EmptyRequestBody emptyRequestBody = EmptyRequestBody.getInstance();
    private static final GetOperationStatusHeaders getOperationStatusHeaders =
            GetOperationStatusHeaders.getInstance();
    private static final CancelOperationHeaders cancelOperationHeaders =
            CancelOperationHeaders.getInstance();
    private static final CloseOperationHeaders closeOperationHeaders =
            CloseOperationHeaders.getInstance();

    @Test
    void testWhenSubmitOperation() throws Exception {
        submitOperation();
    }

    @Test
    void testOperationRelatedApis() throws Exception {
        List<String> ids;
        String status;
        // Get the RUNNING status when an operation is submitted
        ids = submitOperation();
        status = getOperationStatus(ids);
        assertThat(OperationStatus.RUNNING).hasToString(status);
        // Get the CANCELED status when an operation is canceled
        ids = submitOperation();
        status = cancelOperation(ids);
        assertThat(OperationStatus.CANCELED).hasToString(status);
        status = getOperationStatus(ids);
        assertThat(OperationStatus.CANCELED).hasToString(status);
        // Get the CLOSED status when an operation is closed
        ids = submitOperation();
        status = closeOperation(ids);
        assertThat(OperationStatus.CLOSED).hasToString(status);
        SessionHandle sessionHandle = new SessionHandle(UUID.fromString(ids.get(0)));
        OperationHandle operationHandle = new OperationHandle(UUID.fromString(ids.get(1)));
        assertThatThrownBy(
                        () ->
                                SQL_GATEWAY_SERVICE_EXTENSION
                                        .getSessionManager()
                                        .getSession(sessionHandle)
                                        .getOperationManager()
                                        .getOperation(operationHandle))
                .isInstanceOf(SqlGatewayException.class);
    }

    List<String> submitOperation() throws Exception {
        CompletableFuture<OpenSessionResponseBody> response =
                sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
        String sessionHandleId = response.get().getSessionHandle();
        assertThat(sessionHandleId).isNotNull();
        SessionHandle sessionHandle = new SessionHandle(UUID.fromString(sessionHandleId));
        assertThat(SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager().getSession(sessionHandle))
                .isNotNull();

        OneShotLatch startLatch = new OneShotLatch();
        Thread main = Thread.currentThread();
        OperationHandle operationHandle =
                SQL_GATEWAY_SERVICE_EXTENSION
                        .getService()
                        .submitOperation(
                                sessionHandle,
                                () -> {
                                    try {
                                        startLatch.trigger();
                                        // keep operation in RUNNING state in response to cancel
                                        // or close operations.
                                        main.join();
                                    } catch (InterruptedException ignored) {
                                    }
                                    return NotReadyResult.INSTANCE;
                                });
        startLatch.await();
        assertThat(operationHandle).isNotNull();
        return Arrays.asList(sessionHandleId, operationHandle.getIdentifier().toString());
    }

    String getOperationStatus(List<String> ids) throws Exception {
        String sessionId = ids.get(0);
        String operationId = ids.get(1);
        OperationMessageParameters operationMessageParameters =
                new OperationMessageParameters(
                        new SessionHandle(UUID.fromString(sessionId)),
                        new OperationHandle(UUID.fromString(operationId)));
        CompletableFuture<OperationStatusResponseBody> future =
                sendRequest(
                        getOperationStatusHeaders, operationMessageParameters, emptyRequestBody);
        return future.get().getStatus();
    }

    String cancelOperation(List<String> ids) throws Exception {
        CompletableFuture<OperationStatusResponseBody> future =
                sendRequest(cancelOperationHeaders, getMessageParameters(ids), emptyRequestBody);
        return future.get().getStatus();
    }

    String closeOperation(List<String> ids) throws Exception {
        CompletableFuture<OperationStatusResponseBody> future =
                sendRequest(closeOperationHeaders, getMessageParameters(ids), emptyRequestBody);
        return future.get().getStatus();
    }

    OperationMessageParameters getMessageParameters(List<String> ids) {
        String sessionId = ids.get(0);
        String operationId = ids.get(1);
        return new OperationMessageParameters(
                new SessionHandle(UUID.fromString(sessionId)),
                new OperationHandle(UUID.fromString(operationId)));
    }
}
