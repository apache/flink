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

package org.apache.flink.table.client.config;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_MAX_TABLE_RESULT_ROWS;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;

/** Test {@link ConfigurationUtils}. */
public class ConfigurationUtilsTest {

    private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";

    @Test
    public void testSetYamlKey() {
        Configuration configuration = new Configuration();
        ConfigurationUtils.setKeyToConfiguration(configuration, "execution.type", "batch");
        Assert.assertEquals("batch", configuration.getString("execution.type", null));
        Assert.assertEquals(RuntimeExecutionMode.BATCH, configuration.get(RUNTIME_MODE));
    }

    @Test
    public void testSetConfigOption() {
        Configuration configuration = new Configuration();
        ConfigurationUtils.setKeyToConfiguration(configuration, RUNTIME_MODE.key(), "batch");
        Assert.assertNull(configuration.getString("execution.type", null));
        Assert.assertEquals(RuntimeExecutionMode.BATCH, configuration.get(RUNTIME_MODE));
    }

    @Test
    public void testModifyYamlKeyWhenSetConfigOptionOnly() {
        Configuration configuration = new Configuration();
        // set config option
        ConfigurationUtils.setKeyToConfiguration(configuration, RUNTIME_MODE.key(), "batch");
        // modify yaml key
        ConfigurationUtils.setKeyToConfiguration(configuration, "execution.type", "streaming");

        Assert.assertEquals(RuntimeExecutionMode.STREAMING, configuration.get(RUNTIME_MODE));
        Assert.assertEquals("streaming", configuration.getString("execution.type", null));
    }

    @Test
    public void testModifyConfigOptionWhenSetYamlKey() {
        Configuration configuration = new Configuration();
        // set yaml key
        ConfigurationUtils.setKeyToConfiguration(configuration, "execution.type", "streaming");
        // modify config option
        ConfigurationUtils.setKeyToConfiguration(configuration, RUNTIME_MODE.key(), "batch");

        Assert.assertEquals(RuntimeExecutionMode.BATCH, configuration.get(RUNTIME_MODE));
        Assert.assertEquals("batch", configuration.getString("execution.type", null));
    }

    @Test
    public void testExecutionEntryToConfigOption() throws Exception {
        final Environment env = getEnvironment();

        Configuration configuration =
                ConfigurationUtils.convertExecutionEntryToConfiguration(env.getExecution());

        Assert.assertEquals(RuntimeExecutionMode.BATCH, configuration.get(RUNTIME_MODE));
        Assert.assertEquals(ResultMode.TABLE, configuration.get(EXECUTION_RESULT_MODE));
        Assert.assertEquals(100, configuration.getInteger(EXECUTION_MAX_TABLE_RESULT_ROWS));
        Assert.assertEquals("failure-rate", configuration.getString(RESTART_STRATEGY));

        List<String> items = ConfigurationUtils.getPropertiesInPretty(configuration.toMap());
        List<String> expectedItems =
                Arrays.asList(
                        "[DEPRECATED]execution.max-parallelism=16",
                        "[DEPRECATED]execution.max-table-result-rows=100",
                        "[DEPRECATED]execution.min-idle-state-retention=1000",
                        "[DEPRECATED]execution.parallelism=1",
                        "[DEPRECATED]execution.periodic-watermarks-interval=99",
                        "[DEPRECATED]execution.planner=old",
                        "[DEPRECATED]execution.restart-strategy.delay=1000",
                        "[DEPRECATED]execution.restart-strategy.failure-rate-interval=99000",
                        "[DEPRECATED]execution.restart-strategy.max-failures-per-interval=10",
                        "[DEPRECATED]execution.restart-strategy.type=failure-rate",
                        "[DEPRECATED]execution.result-mode=table",
                        "[DEPRECATED]execution.type=batch",
                        "execution.runtime-mode=batch",
                        "parallelism.default=1",
                        "pipeline.auto-watermark-interval=99",
                        "pipeline.max-parallelism=16",
                        "restart-strategy.failure-rate.delay=1000000000 ns",
                        "restart-strategy.failure-rate.failure-rate-interval=99000000000 ns",
                        "restart-strategy.failure-rate.max-failures-per-interval=10",
                        "restart-strategy.fixed-delay.delay=1000",
                        "restart-strategy=failure-rate",
                        "sql-client.execution.max-table-result.rows=100",
                        "sql-client.execution.result-mode=table",
                        "table.exec.state.ttl=1000",
                        "table.planner=old");
        Assert.assertEquals(expectedItems, items);
    }

    private Environment getEnvironment() throws Exception {
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", "old");
        replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
        replaceVars.put("$VAR_RESULT_MODE", "table");
        replaceVars.put("$VAR_UPDATE_MODE", "");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
        return EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars);
    }
}
