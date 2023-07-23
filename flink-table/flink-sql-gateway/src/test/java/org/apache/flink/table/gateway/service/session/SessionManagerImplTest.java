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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.context.DefaultContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link SessionManagerImpl}. */
class SessionManagerImplTest {

    private SessionManagerImpl sessionManager;

    @BeforeEach
    void setup() {
        Configuration conf = new Configuration();
        conf.set(
                SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_IDLE_TIMEOUT,
                Duration.ofSeconds(2));
        conf.set(
                SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_CHECK_INTERVAL,
                Duration.ofMillis(100));
        conf.set(SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_MAX_NUM, 3);
        sessionManager = new SessionManagerImpl(new DefaultContext(conf, Collections.emptyList()));
        sessionManager.start();
    }

    @AfterEach
    void cleanUp() {
        if (sessionManager != null) {
            sessionManager.stop();
        }
    }

    @Test
    void testIdleSessionCleanup() throws Exception {
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
    void testSessionNumberLimit() {
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

    @Test
    void testRunWithCatalogStore() {
        Configuration conf = new Configuration();
        // Set a long idle timeout value in case the session is auto-closed.
        conf.set(
                SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_IDLE_TIMEOUT,
                Duration.ofSeconds(60 * 1000));
        conf.set(
                SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_CHECK_INTERVAL,
                Duration.ofMillis(100));
        conf.set(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND, "test-catalog-store");
        conf.set(SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_MAX_NUM, 3);
        sessionManager = new SessionManagerImpl(new DefaultContext(conf, Collections.emptyList()));
        sessionManager.start();

        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .build();
        // Open two sessions that will share all created catalogs.
        Session session1 = sessionManager.openSession(environment);
        Session session2 = sessionManager.openSession(environment);

        Configuration configuration = new Configuration();
        configuration.setString("type", "generic_in_memory");
        session1.createExecutor()
                .getTableEnvironment()
                .getCatalogManager()
                .createCatalog("cat1", CatalogDescriptor.of("cat1", configuration));
        session2.createExecutor()
                .getTableEnvironment()
                .getCatalogManager()
                .createCatalog("cat2", CatalogDescriptor.of("cat2", configuration));

        assertTrue(session1.createExecutor().listCatalogs().contains("cat1"));
        assertTrue(session1.createExecutor().listCatalogs().contains("cat2"));

        assertThatThrownBy(
                        () ->
                                session1.createExecutor()
                                        .getTableEnvironment()
                                        .createCatalog(
                                                "cat2",
                                                CatalogDescriptor.of("cat2", configuration)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Catalog cat2 already exists in catalog store.");

        sessionManager.stop();
    }

    @Test
    void testRunWithFileCatalogStore(@TempDir File tempFolder) {
        Configuration conf = new Configuration();
        // Set a long idle timeout value in case the session is auto-closed.
        conf.set(
                SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_IDLE_TIMEOUT,
                Duration.ofSeconds(60 * 1000));
        // Config file catalog store
        conf.set(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND, "file");
        conf.setString(
                CommonCatalogOptions.TABLE_CATALOG_STORE_OPTION_PREFIX + "file.path",
                tempFolder.getAbsolutePath());
        sessionManager = new SessionManagerImpl(new DefaultContext(conf, Collections.emptyList()));
        sessionManager.start();

        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .build();

        // catalog configuration
        Configuration configuration = new Configuration();
        configuration.setString("type", "generic_in_memory");

        // Open two sessions that will share all created catalogs.
        Session session1 = sessionManager.openSession(environment);

        session1.createExecutor()
                .getTableEnvironment()
                .getCatalogManager()
                .createCatalog("cat1", CatalogDescriptor.of("cat1", configuration));
        assertTrue(session1.createExecutor().listCatalogs().contains("cat1"));

        Session session2 = sessionManager.openSession(environment);

        session2.createExecutor()
                .getTableEnvironment()
                .getCatalogManager()
                .createCatalog("cat2", CatalogDescriptor.of("cat2", configuration));

        // session1 can access the catalog created by session2
        assertTrue(session1.createExecutor().listCatalogs().contains("cat1"));
        assertTrue(session1.createExecutor().listCatalogs().contains("cat2"));

        // session2 can access the catalog created by session1
        assertTrue(session2.createExecutor().listCatalogs().contains("cat2"));
        assertTrue(session2.createExecutor().listCatalogs().contains("cat1"));

        sessionManager.stop();
    }
}
