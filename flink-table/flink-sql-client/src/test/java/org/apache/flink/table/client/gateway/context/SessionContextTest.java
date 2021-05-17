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

package org.apache.flink.table.client.gateway.context;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_PLANNER;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Test {@link SessionContext}. */
public class SessionContextTest {

    private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";

    @Test
    public void testSetAndResetYamlKey() throws Exception {
        SessionContext sessionContext = createSessionContext();
        sessionContext.set("execution.max-table-result-rows", "100000");

        assertEquals(
                "100000",
                getConfigurationMap(sessionContext).get("execution.max-table-result-rows"));

        sessionContext.reset();

        assertEquals(
                "100", getConfigurationMap(sessionContext).get("execution.max-table-result-rows"));
    }

    @Test
    public void testSetAndResetOption() throws Exception {
        SessionContext sessionContext = createSessionContext();
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option and Yaml specified value
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");
        assertEquals("hive", getConfiguration(sessionContext).getString(TABLE_SQL_DIALECT));
        assertEquals(128, getConfiguration(sessionContext).getInteger(MAX_PARALLELISM));
        assertEquals("test", getConfiguration(sessionContext).getString(NAME));
        assertFalse(getConfiguration(sessionContext).getBoolean(OBJECT_REUSE));

        sessionContext.reset();
        assertEquals("default", getConfiguration(sessionContext).getString(TABLE_SQL_DIALECT));
        assertNull(getConfiguration(sessionContext).get(NAME));
        // The value of MAX_PARALLELISM in DEFAULTS_ENVIRONMENT_FILE is 16
        assertEquals(16, getConfiguration(sessionContext).getInteger(MAX_PARALLELISM));
        assertNull(getConfiguration(sessionContext).getString(NAME, null));
        // The value of OBJECT_REUSE in origin configuration is true
        assertTrue(getConfiguration(sessionContext).getBoolean(OBJECT_REUSE));
    }

    @Test
    public void testSetAndResetKeyInYamlKey() throws Exception {
        SessionContext sessionContext = createSessionContext();
        sessionContext.set("execution.max-table-result-rows", "200000");
        sessionContext.set("execution.result-mode", "table");

        assertEquals(
                "200000",
                getConfigurationMap(sessionContext).get("execution.max-table-result-rows"));

        assertEquals("table", getConfigurationMap(sessionContext).get("execution.result-mode"));

        sessionContext.reset("execution.result-mode");
        assertEquals("changelog", getConfigurationMap(sessionContext).get("execution.result-mode"));
        // no reset this key execution.max-table-result-rows
        assertEquals(
                "200000",
                getConfigurationMap(sessionContext).get("execution.max-table-result-rows"));

        // reset option for deprecated key
        sessionContext.reset("sql-client.execution.max-table-result.rows");
        assertEquals(
                "100",
                getConfigurationMap(sessionContext)
                        .get("sql-client.execution.max-table-result.rows"));
        assertEquals(
                "100", getConfigurationMap(sessionContext).get("execution.max-table-result-rows"));
    }

    @Test
    public void testSetAndResetKeyInConfigOptions() throws Exception {
        SessionContext sessionContext = createSessionContext();
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option and Yaml specified value
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");

        assertEquals("hive", getConfiguration(sessionContext).getString(TABLE_SQL_DIALECT));
        assertEquals(128, getConfiguration(sessionContext).getInteger(MAX_PARALLELISM));
        assertEquals("test", getConfiguration(sessionContext).getString(NAME));
        assertFalse(getConfiguration(sessionContext).getBoolean(OBJECT_REUSE));

        sessionContext.reset(TABLE_SQL_DIALECT.key());
        assertEquals("default", getConfiguration(sessionContext).getString(TABLE_SQL_DIALECT));

        sessionContext.reset(MAX_PARALLELISM.key());
        assertEquals(16, getConfiguration(sessionContext).getInteger(MAX_PARALLELISM));

        sessionContext.reset(NAME.key());
        assertNull(getConfiguration(sessionContext).get(NAME));

        sessionContext.reset(OBJECT_REUSE.key());
        assertTrue(getConfiguration(sessionContext).getBoolean(OBJECT_REUSE));
    }

    @Test
    public void testSetWithConfigOptionAndResetWithYamlKey() throws Exception {
        SessionContext sessionContext = createSessionContext();
        // runtime config option and has deprecated key
        sessionContext.set(TABLE_PLANNER.key(), "blink");
        assertEquals(
                "blink", getConfiguration(sessionContext).get(TABLE_PLANNER).name().toLowerCase());

        sessionContext.reset(TABLE_PLANNER.key());
        assertEquals(
                "old", getConfiguration(sessionContext).get(TABLE_PLANNER).name().toLowerCase());
        assertEquals(
                "old", getConfigurationMap(sessionContext).get("execution.planner").toLowerCase());
    }

    @Test
    public void testSetAndResetKeyNotInYaml() throws Exception {
        SessionContext sessionContext = createSessionContext();
        // other property not in yaml and flink-conf
        sessionContext.set("aa", "11");
        sessionContext.set("bb", "22");

        assertEquals("11", getConfigurationMap(sessionContext).get("aa"));
        assertEquals("22", getConfigurationMap(sessionContext).get("bb"));

        sessionContext.reset("aa");
        assertNull(getConfigurationMap(sessionContext).get("aa"));
        assertEquals("22", getConfigurationMap(sessionContext).get("bb"));

        sessionContext.reset("bb");
        assertNull(getConfigurationMap(sessionContext).get("bb"));
    }

    // --------------------------------------------------------------------------------------------

    private SessionContext createSessionContext() throws Exception {

        Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", "old");
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");

        final Environment env =
                EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars);
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(OBJECT_REUSE, true);
        DefaultContext defaultContext =
                new DefaultContext(
                        env,
                        Collections.emptyList(),
                        flinkConfig,
                        Collections.singletonList(new DefaultCLI()));
        return SessionContext.create(defaultContext, "test-session");
    }

    private Map<String, String> getConfigurationMap(SessionContext context) {
        return context.getExecutionContext()
                .getTableEnvironment()
                .getConfig()
                .getConfiguration()
                .toMap();
    }

    private Configuration getConfiguration(SessionContext context) {
        return context.getExecutionContext().getTableEnvironment().getConfig().getConfiguration();
    }
}
