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

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
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
                        .getSessionEnvironment()
                        .getExecution()
                        .asMap()
                        .get("max-table-result-rows"));

        sessionContext.reset();

        Assert.assertEquals(
                "100",
                sessionContext
                        .getSessionEnvironment()
                        .getExecution()
                        .asMap()
                        .get("max-table-result-rows"));
    }

    @Test
    public void testSetAndResetOption() throws Exception {
        SessionContext sessionContext = createSessionContext();
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option and has default value
        sessionContext.set(DEFAULT_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        Assert.assertEquals(
                "hive",
                sessionContext
                        .getSessionEnvironment()
                        .getConfiguration()
                        .asMap()
                        .get(TABLE_SQL_DIALECT.key()));
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
                        .getInteger(DEFAULT_PARALLELISM));
        Assert.assertEquals(
                "test",
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getString(NAME));

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
                        .getSessionEnvironment()
                        .getConfiguration()
                        .asMap()
                        .get("pipeline.name"));
        Assert.assertEquals(
                (long) DEFAULT_PARALLELISM.defaultValue(),
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getInteger(DEFAULT_PARALLELISM));
        Assert.assertNull(
                sessionContext
                        .getExecutionContext()
                        .getTableEnvironment()
                        .getConfig()
                        .getConfiguration()
                        .getString(NAME, null));
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
        DefaultContext defaultContext =
                new DefaultContext(
                        env,
                        Collections.emptyList(),
                        new Configuration(),
                        Collections.singletonList(new DefaultCLI()));
        return SessionContext.create(defaultContext, "test-session");
    }
}
