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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;

/** Test {@link SessionContext}. */
public class SessionContextTest {

    private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";

    @Test
    public void testSetAndResetYamlKey() throws Exception {
        SessionContext sessionContext = createSessionContext();
        sessionContext.set("execution.max-table-result-rows", "100000");

        Assert.assertEquals(
                "100000",
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .toMap()
                        .get("execution.max-table-result-rows"));

        sessionContext.reset();

        Assert.assertEquals(
                "100",
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .toMap()
                        .get("execution.max-table-result-rows"));
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
        Assert.assertEquals(
                "hive",
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getString(TABLE_SQL_DIALECT));
        Assert.assertEquals(
                128,
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getInteger(MAX_PARALLELISM));
        Assert.assertEquals(
                "test",
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getString(NAME));
        Assert.assertFalse(
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getBoolean(OBJECT_REUSE));

        sessionContext.reset();
        Assert.assertEquals(
                "default",
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getString(TABLE_SQL_DIALECT));
        Assert.assertNull(
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .get(NAME));
        // The value of MAX_PARALLELISM in DEFAULTS_ENVIRONMENT_FILE is 16
        Assert.assertEquals(
                16,
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getInteger(MAX_PARALLELISM));
        Assert.assertNull(
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getString(NAME, null));
        // The value of OBJECT_REUSE in origin configuration is true
        Assert.assertTrue(
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getBoolean(OBJECT_REUSE));
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
}
