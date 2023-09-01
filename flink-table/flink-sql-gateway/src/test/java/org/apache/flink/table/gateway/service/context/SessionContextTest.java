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

package org.apache.flink.table.gateway.service.context;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.FileCatalogStore;
import org.apache.flink.table.catalog.listener.CatalogFactory1;
import org.apache.flink.table.catalog.listener.CatalogFactory2;
import org.apache.flink.table.catalog.listener.CatalogListener1;
import org.apache.flink.table.catalog.listener.CatalogListener2;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.ThreadUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_CATALOG_MODIFICATION_LISTENERS;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_CATALOG_NAME;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_KIND;
import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link SessionContext}. */
class SessionContextTest {

    private static final ExecutorService EXECUTOR_SERVICE =
            ThreadUtils.newThreadPool(5, 500, 60_0000, "session-context-test");
    private SessionContext sessionContext;

    @BeforeEach
    void setup() {
        sessionContext = createSessionContext();
    }

    @AfterEach
    void cleanUp() {
        sessionContext.close();
    }

    @AfterAll
    static void closeResources() {
        EXECUTOR_SERVICE.shutdown();
    }

    @Test
    void testSetAndResetOption() {
        // runtime config option
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");
        assertThat(sessionContext.getSessionConf().get(MAX_PARALLELISM)).isEqualTo(128);
        assertThat(sessionContext.getSessionConf().get(NAME)).isEqualTo("test");
        assertThat(sessionContext.getSessionConf().get(OBJECT_REUSE)).isFalse();

        sessionContext.reset();
        assertThat(sessionContext.getSessionConf().get(TABLE_SQL_DIALECT)).isEqualTo("default");
        assertThat(sessionContext.getSessionConf().get(NAME)).isNull();
        // The value of MAX_PARALLELISM in DEFAULTS_ENVIRONMENT_FILE is 16
        assertThat(sessionContext.getSessionConf().get(MAX_PARALLELISM)).isEqualTo(16);
        assertThat(sessionContext.getSessionConf().getOptional(NAME)).isEmpty();
        // The value of OBJECT_REUSE in origin configuration is true
        assertThat(sessionContext.getSessionConf().get(OBJECT_REUSE)).isTrue();
    }

    @Test
    void testSetAndResetKeyInConfigOptions() {
        // runtime config option
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");

        assertThat(sessionContext.getSessionConf().get(MAX_PARALLELISM)).isEqualTo(128);
        assertThat(sessionContext.getSessionConf().get(NAME)).isEqualTo("test");
        assertThat(sessionContext.getSessionConf().get(OBJECT_REUSE)).isFalse();

        sessionContext.reset(TABLE_SQL_DIALECT.key());
        assertThat(sessionContext.getSessionConf().get(TABLE_SQL_DIALECT)).isEqualTo("default");

        sessionContext.reset(MAX_PARALLELISM.key());
        assertThat(sessionContext.getSessionConf().get(MAX_PARALLELISM)).isEqualTo(16);

        sessionContext.reset(NAME.key());
        assertThat(sessionContext.getSessionConf().get(NAME)).isNull();

        sessionContext.reset(OBJECT_REUSE.key());
        assertThat(sessionContext.getSessionConf().get(OBJECT_REUSE)).isTrue();
    }

    @Test
    void testSetAndResetArbitraryKey() {
        // other property not in flink-conf
        sessionContext.set("aa", "11");
        sessionContext.set("bb", "22");

        ConfigOption<String> aa = ConfigOptions.key("aa").stringType().defaultValue("11");
        ConfigOption<String> bb = ConfigOptions.key("bb").stringType().defaultValue("22");

        assertThat(sessionContext.getSessionConf())
                .matches((conf) -> conf.contains(aa) && conf.contains(bb));

        sessionContext.reset("aa");
        assertThat(sessionContext.getSessionConf())
                .matches((conf) -> !conf.containsKey("aa") && conf.contains(bb));

        sessionContext.reset("bb");
        assertThat(sessionContext.getSessionConf())
                .matches((conf) -> !conf.containsKey("aa") && !conf.containsKey("bb"));
    }

    @Test
    void testCreateContextWithListeners() {
        assertThat(
                        sessionContext
                                .getSessionState()
                                .catalogManager
                                .getCatalogModificationListeners()
                                .isEmpty())
                .isTrue();

        Configuration flinkConfig = new Configuration();
        // Find and create listeners from `SET
        // 'table.catalog-modification.listeners'='factory1;factory2' for session
        SessionContext context1 =
                SessionContext.create(
                        new DefaultContext(flinkConfig, Collections.emptyList()),
                        SessionHandle.create(),
                        SessionEnvironment.newBuilder()
                                .addSessionConfig(
                                        Collections.singletonMap(
                                                TABLE_CATALOG_MODIFICATION_LISTENERS.key(),
                                                String.format(
                                                        "%s;%s",
                                                        CatalogFactory1.IDENTIFIER,
                                                        CatalogFactory2.IDENTIFIER)))
                                .setSessionEndpointVersion(MockedEndpointVersion.V1)
                                .build(),
                        EXECUTOR_SERVICE);
        assertThat(
                        context1.getSessionState().catalogManager.getCatalogModificationListeners()
                                .stream()
                                .map(l -> l.getClass().getName())
                                .collect(Collectors.toList()))
                .isEqualTo(
                        Arrays.asList(
                                CatalogListener1.class.getName(),
                                CatalogListener2.class.getName()));

        // Find and create listeners from flink-conf.yaml for session
        flinkConfig.set(
                TableConfigOptions.TABLE_CATALOG_MODIFICATION_LISTENERS,
                Arrays.asList(CatalogFactory1.IDENTIFIER, CatalogFactory2.IDENTIFIER));
        SessionContext context2 =
                SessionContext.create(
                        new DefaultContext(flinkConfig, Collections.emptyList()),
                        SessionHandle.create(),
                        SessionEnvironment.newBuilder()
                                .setSessionEndpointVersion(MockedEndpointVersion.V1)
                                .build(),
                        EXECUTOR_SERVICE);
        assertThat(
                        context2.getSessionState().catalogManager.getCatalogModificationListeners()
                                .stream()
                                .map(l -> l.getClass().getName())
                                .collect(Collectors.toList()))
                .isEqualTo(
                        Arrays.asList(
                                CatalogListener1.class.getName(),
                                CatalogListener2.class.getName()));
    }

    @Test
    void testCreateDefaultCatalogFromStore(@TempDir Path catalogFilePath) {
        CatalogStore catalogStore = new FileCatalogStore(catalogFilePath.toString());
        catalogStore.open();
        catalogStore.storeCatalog(
                "cat1",
                CatalogDescriptor.of(
                        "cat1",
                        Configuration.fromMap(
                                Collections.singletonMap("type", "generic_in_memory"))));
        catalogStore.storeCatalog(
                "cat2",
                CatalogDescriptor.of(
                        "cat2",
                        Configuration.fromMap(
                                Collections.singletonMap("type", "generic_in_memory"))));
        catalogStore.close();

        Configuration flinkConfig = new Configuration();
        flinkConfig.set(TABLE_CATALOG_STORE_KIND, "file");
        flinkConfig.setString("table.catalog-store.file.path", catalogFilePath.toString());
        SessionContext context1 = createSessionContext(flinkConfig);
        assertThat(context1.getSessionState().catalogManager.getCurrentCatalog())
                .isEqualTo("default_catalog");
        context1.close();

        flinkConfig.set(TABLE_CATALOG_NAME, "cat1");
        SessionContext context2 = createSessionContext(flinkConfig);
        assertThat(context2.getSessionState().catalogManager.getCurrentCatalog()).isEqualTo("cat1");
        context2.close();
    }

    // --------------------------------------------------------------------------------------------

    private SessionContext createSessionContext(Configuration flinkConfig) {
        DefaultContext defaultContext = new DefaultContext(flinkConfig, Collections.emptyList());
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .addSessionConfig(flinkConfig.toMap())
                        .build();
        return SessionContext.create(
                defaultContext, SessionHandle.create(), environment, EXECUTOR_SERVICE);
    }

    private SessionContext createSessionContext() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(OBJECT_REUSE, true);
        flinkConfig.set(MAX_PARALLELISM, 16);
        return createSessionContext(flinkConfig);
    }
}
