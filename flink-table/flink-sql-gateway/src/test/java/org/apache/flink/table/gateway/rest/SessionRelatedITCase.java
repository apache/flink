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

import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.ConfigureSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.GetSessionConfigHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.TriggerSessionHeartbeatHeaders;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.ConfigureSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.GetSessionConfigResponseBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.service.session.Session;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.gateway.rest.handler.session.CloseSessionHandler.CLOSE_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in session
 * related cases.
 */
class SessionRelatedITCase extends RestAPIITCaseBase {

    private static final String SESSION_NAME = "test";
    private static final Map<String, String> properties = new HashMap<>();
    private static final int SESSION_NUMBER = 10;

    static {
        properties.put("k1", "v1");
        properties.put("k2", "v2");
    }

    private static final OpenSessionHeaders openSessionHeaders = OpenSessionHeaders.getInstance();
    private static final OpenSessionRequestBody openSessionRequestBody =
            new OpenSessionRequestBody(SESSION_NAME, properties);
    private static final EmptyMessageParameters emptyParameters =
            EmptyMessageParameters.getInstance();

    private static final CloseSessionHeaders closeSessionHeaders =
            CloseSessionHeaders.getInstance();
    private static final EmptyRequestBody emptyRequestBody = EmptyRequestBody.getInstance();

    private SessionHandle sessionHandle;

    private SessionMessageParameters sessionMessageParameters;

    @BeforeEach
    public void setUp() throws Exception {
        CompletableFuture<OpenSessionResponseBody> response =
                sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
        String sessionHandleId = response.get().getSessionHandle();
        assertThat(sessionHandleId).isNotNull();

        sessionHandle = new SessionHandle(UUID.fromString(sessionHandleId));
        assertThat(SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager().getSession(sessionHandle))
                .isNotNull();

        sessionMessageParameters = new SessionMessageParameters(sessionHandle);
    }

    @AfterEach
    public void cleanUp() throws Exception {
        CompletableFuture<CloseSessionResponseBody> response =
                sendRequest(closeSessionHeaders, sessionMessageParameters, emptyRequestBody);

        String status = response.get().getStatus();
        assertThat(status).isEqualTo(CLOSE_MESSAGE);
    }

    @Test
    void testCreateAndCloseSessions() throws Exception {
        List<SessionHandle> sessionHandles = new ArrayList<>();
        Set<String> sessionHandleIds = new HashSet<>();
        for (int num = 0; num < SESSION_NUMBER; ++num) {
            CompletableFuture<OpenSessionResponseBody> response =
                    sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
            String sessionHandleId = response.get().getSessionHandle();
            assertThat(sessionHandleId).isNotNull();
            sessionHandleIds.add(sessionHandleId);
            SessionHandle sessionHandle = new SessionHandle(UUID.fromString(sessionHandleId));
            assertThat(SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager().getSession(sessionHandle))
                    .isNotNull();
            sessionHandles.add(sessionHandle);
        }
        assertThat(sessionHandleIds).hasSize(SESSION_NUMBER);

        for (int num = 0; num < SESSION_NUMBER; ++num) {
            SessionHandle sessionHandle = sessionHandles.get(num);
            SessionMessageParameters sessionMessageParameters =
                    new SessionMessageParameters(sessionHandle);
            CompletableFuture<CloseSessionResponseBody> response =
                    sendRequest(closeSessionHeaders, sessionMessageParameters, emptyRequestBody);
            String status = response.get().getStatus();
            assertThat(status).isEqualTo(CLOSE_MESSAGE);
            assertThatThrownBy(
                            () ->
                                    SQL_GATEWAY_SERVICE_EXTENSION
                                            .getSessionManager()
                                            .getSession(sessionHandle))
                    .isInstanceOf(SqlGatewayException.class);
            // Test closing the closed session
            CompletableFuture<CloseSessionResponseBody> response2 =
                    sendRequest(closeSessionHeaders, sessionMessageParameters, emptyRequestBody);
            assertThatThrownBy(response2::get).isInstanceOf(ExecutionException.class);
        }
    }

    @Test
    void testGetSessionConfiguration() throws Exception {
        CompletableFuture<GetSessionConfigResponseBody> future =
                sendRequest(
                        GetSessionConfigHeaders.getInstance(),
                        sessionMessageParameters,
                        emptyRequestBody);
        Map<String, String> getProperties = future.get().getProperties();
        for (String key : properties.keySet()) {
            assertThat(properties).containsEntry(key, getProperties.get(key));
        }
    }

    @Test
    void testTouchSession() throws Exception {
        Session session =
                SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager().getSession(sessionHandle);
        assertThat(session).isNotNull();

        long lastAccessTime = session.getLastAccessTime();

        CompletableFuture<EmptyResponseBody> future =
                sendRequest(
                        TriggerSessionHeartbeatHeaders.getInstance(),
                        sessionMessageParameters,
                        emptyRequestBody);
        future.get();
        assertThat(session.getLastAccessTime()).isGreaterThan(lastAccessTime);
    }

    @Test
    void testConfigureSession() throws Exception {
        ConfigureSessionRequestBody configureSessionRequestBody =
                new ConfigureSessionRequestBody("set 'test' = 'configure';", -1L);

        CompletableFuture<EmptyResponseBody> response =
                sendRequest(
                        ConfigureSessionHeaders.getInstance(),
                        sessionMessageParameters,
                        configureSessionRequestBody);
        response.get();

        assertThat(
                        SQL_GATEWAY_SERVICE_EXTENSION
                                .getSessionManager()
                                .getSession(sessionHandle)
                                .getSessionConfig())
                .containsEntry("test", "configure");
    }
}
