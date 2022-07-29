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

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.ThreadUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link SessionContext}. */
class SessionContextTest {

    private static final ExecutorService EXECUTOR_SERVICE =
            ThreadUtils.newThreadPool(5, 500, 60_0000, "session-context-test");
    private SessionContext sessionContext;

    @BeforeEach
    public void setup() {
        sessionContext = createSessionContext();
    }

    @AfterAll
    public static void cleanUp() {
        EXECUTOR_SERVICE.shutdown();
    }

    @Test
    public void testSetAndResetOption() {
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");
        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("hive");
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(128);
        assertThat(getConfiguration().get(NAME)).isEqualTo("test");
        assertThat(getConfiguration().get(OBJECT_REUSE)).isFalse();

        sessionContext.reset();
        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("default");
        assertThat(getConfiguration().get(NAME)).isNull();
        // The value of MAX_PARALLELISM in DEFAULTS_ENVIRONMENT_FILE is 16
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(16);
        assertThat(getConfiguration().getOptional(NAME)).isEmpty();
        // The value of OBJECT_REUSE in origin configuration is true
        assertThat(getConfiguration().get(OBJECT_REUSE)).isTrue();
    }

    @Test
    public void testSetAndResetKeyInConfigOptions() {
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");

        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("hive");
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(128);
        assertThat(getConfiguration().get(NAME)).isEqualTo("test");
        assertThat(getConfiguration().get(OBJECT_REUSE)).isFalse();

        sessionContext.reset(TABLE_SQL_DIALECT.key());
        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("default");

        sessionContext.reset(MAX_PARALLELISM.key());
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(16);

        sessionContext.reset(NAME.key());
        assertThat(getConfiguration().get(NAME)).isNull();

        sessionContext.reset(OBJECT_REUSE.key());
        assertThat(getConfiguration().get(OBJECT_REUSE)).isTrue();
    }

    @Test
    public void testSetAndResetArbitraryKey() {
        // other property not in flink-conf
        sessionContext.set("aa", "11");
        sessionContext.set("bb", "22");

        assertThat(sessionContext.getConfigMap().get("aa")).isEqualTo("11");
        assertThat(sessionContext.getConfigMap().get("bb")).isEqualTo("22");

        sessionContext.reset("aa");
        assertThat(sessionContext.getConfigMap().get("aa")).isNull();
        assertThat(sessionContext.getConfigMap().get("bb")).isEqualTo("22");

        sessionContext.reset("bb");
        assertThat(sessionContext.getConfigMap().get("bb")).isNull();
    }

    // --------------------------------------------------------------------------------------------

    private SessionContext createSessionContext() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(OBJECT_REUSE, true);
        flinkConfig.set(MAX_PARALLELISM, 16);
        DefaultContext defaultContext =
                new DefaultContext(flinkConfig, Collections.singletonList(new DefaultCLI()));
        return SessionContext.create(
                defaultContext,
                SessionHandle.create(),
                MockedEndpointVersion.V1,
                flinkConfig,
                EXECUTOR_SERVICE);
    }

    private ReadableConfig getConfiguration() {
        return Configuration.fromMap(sessionContext.getConfigMap());
    }
}
