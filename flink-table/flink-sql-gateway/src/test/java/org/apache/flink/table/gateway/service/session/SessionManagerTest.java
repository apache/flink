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

package org.apache.flink.table.gateway.service.session;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link SessionManager}. */
public class SessionManagerTest extends TestLogger {

    private SessionManager sessionManager;

    @BeforeEach
    public void setup() {
        Configuration conf = new Configuration();
        conf.set(
                SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_IDLE_TIMEOUT,
                Duration.ofSeconds(2));
        conf.set(
                SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_CHECK_INTERVAL,
                Duration.ofMillis(100));
        conf.set(SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_MAX_NUM, 3);
        sessionManager =
                new SessionManager(
                        new DefaultContext(conf, Collections.singletonList(new DefaultCLI())));
        sessionManager.start();
    }

    @AfterEach
    public void cleanUp() {
        if (sessionManager != null) {
            sessionManager.stop();
        }
    }

    @Test
    public void testIdleSessionCleanup() throws Exception {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .build();
        Session session = sessionManager.openSession(environment);
        SessionHandle sessionId = session.getSessionHandle();
        for (int i = 0; i < 3; i++) {
            // should success
            sessionManager.getSession(sessionId);
            Thread.sleep(1000);
        }
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));
        while (deadline.hasTimeLeft() && sessionManager.isSessionAlive(sessionId)) {
            //noinspection BusyWait
            Thread.sleep(1000);
        }
        assertFalse(sessionManager.isSessionAlive(sessionId));
    }

    @Test
    public void testSessionNumberLimit() {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .build();

        sessionManager.openSession(environment);
        sessionManager.openSession(environment);
        sessionManager.openSession(environment);

        assertEquals(3, sessionManager.currentSessionCount());
        assertThrows(
                SqlGatewayException.class,
                () -> sessionManager.openSession(environment),
                "Failed to create session, the count of active sessions exceeds the max count: 3");
    }
}
