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
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.statement.CompleteStatementHeaders;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.CompleteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.CompleteStatementResponseBody;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in statement
 * related cases.
 */
class StatementRelatedITCase extends RestAPIITCaseBase {

    private SessionHandle sessionHandle;
    private SessionMessageParameters sessionMessageParameters;

    @BeforeEach
    void setUp() throws Exception {
        CompletableFuture<OpenSessionResponseBody> response =
                sendRequest(
                        OpenSessionHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        new OpenSessionRequestBody(null, null));

        sessionHandle = new SessionHandle(UUID.fromString(response.get().getSessionHandle()));

        sessionMessageParameters = new SessionMessageParameters(sessionHandle);
    }

    @Test
    void testCompleteStatement() throws Exception {
        CompletableFuture<CompleteStatementResponseBody> completeStatementResponse =
                sendRequest(
                        CompleteStatementHeaders.getInstance(),
                        sessionMessageParameters,
                        new CompleteStatementRequestBody("CREATE TA", 9));

        assertThat(completeStatementResponse.get().getCandidates())
                .isEqualTo(Collections.singletonList("TABLE"));
    }
}
