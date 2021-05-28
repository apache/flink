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
import org.apache.flink.table.client.gateway.utils.TestUserClassLoaderJar;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.PipelineOptions.JARS;
import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_PLANNER;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test {@link SessionContext}. */
public class SessionContextTest {

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
    private static File udfJar;

    private SessionContext sessionContext;

    @BeforeClass
    public static void prepare() throws Exception {
        udfJar =
                TestUserClassLoaderJar.createJarFile(
                        tempFolder.newFolder("test-jar"), "test-classloader-udf.jar");
    }

    @Before
    public void setup() throws Exception {
        sessionContext = createSessionContext();
    }

    @Test
    public void testSetAndResetYamlKey() {
        sessionContext.set("execution.max-table-result-rows", "100000");

        assertEquals("100000", getConfigurationMap().get("execution.max-table-result-rows"));

        sessionContext.reset();

        assertEquals("100", getConfigurationMap().get("execution.max-table-result-rows"));
    }

    @Test
    public void testSetAndResetOption() {
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option and Yaml specified value
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");
        assertEquals("hive", getConfiguration().getString(TABLE_SQL_DIALECT));
        assertEquals(128, getConfiguration().getInteger(MAX_PARALLELISM));
        assertEquals("test", getConfiguration().getString(NAME));
        assertFalse(getConfiguration().getBoolean(OBJECT_REUSE));

        sessionContext.reset();
        assertEquals("default", getConfiguration().getString(TABLE_SQL_DIALECT));
        assertNull(getConfiguration().get(NAME));
        // The value of MAX_PARALLELISM in DEFAULTS_ENVIRONMENT_FILE is 16
        assertEquals(16, getConfiguration().getInteger(MAX_PARALLELISM));
        assertNull(getConfiguration().getString(NAME, null));
        // The value of OBJECT_REUSE in origin configuration is true
        assertTrue(getConfiguration().getBoolean(OBJECT_REUSE));
    }

    @Test
    public void testSetAndResetKeyInYamlKey() {
        sessionContext.set("execution.max-table-result-rows", "200000");
        sessionContext.set("execution.result-mode", "table");

        assertEquals("200000", getConfigurationMap().get("execution.max-table-result-rows"));

        assertEquals("table", getConfigurationMap().get("execution.result-mode"));

        sessionContext.reset("execution.result-mode");
        assertEquals("changelog", getConfigurationMap().get("execution.result-mode"));
        // no reset this key execution.max-table-result-rows
        assertEquals("200000", getConfigurationMap().get("execution.max-table-result-rows"));

        // reset option for deprecated key
        sessionContext.reset("sql-client.execution.max-table-result.rows");
        assertEquals(
                "100", getConfigurationMap().get("sql-client.execution.max-table-result.rows"));
        assertEquals("100", getConfigurationMap().get("execution.max-table-result-rows"));
    }

    @Test
    public void testSetAndResetKeyInConfigOptions() {
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option and Yaml specified value
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");

        assertEquals("hive", getConfiguration().getString(TABLE_SQL_DIALECT));
        assertEquals(128, getConfiguration().getInteger(MAX_PARALLELISM));
        assertEquals("test", getConfiguration().getString(NAME));
        assertFalse(getConfiguration().getBoolean(OBJECT_REUSE));

        sessionContext.reset(TABLE_SQL_DIALECT.key());
        assertEquals("default", getConfiguration().getString(TABLE_SQL_DIALECT));

        sessionContext.reset(MAX_PARALLELISM.key());
        assertEquals(16, getConfiguration().getInteger(MAX_PARALLELISM));

        sessionContext.reset(NAME.key());
        assertNull(getConfiguration().get(NAME));

        sessionContext.reset(OBJECT_REUSE.key());
        assertTrue(getConfiguration().getBoolean(OBJECT_REUSE));
    }

    @Test
    public void testSetWithConfigOptionAndResetWithYamlKey() {
        // runtime config option and has deprecated key
        sessionContext.set(TABLE_PLANNER.key(), "blink");
        assertEquals("blink", getConfiguration().get(TABLE_PLANNER).name().toLowerCase());

        sessionContext.reset(TABLE_PLANNER.key());
        assertEquals("old", getConfiguration().get(TABLE_PLANNER).name().toLowerCase());
        assertEquals("old", getConfigurationMap().get("execution.planner").toLowerCase());
    }

    @Test
    public void testSetAndResetKeyNotInYaml() {
        // other property not in yaml and flink-conf
        sessionContext.set("aa", "11");
        sessionContext.set("bb", "22");

        assertEquals("11", getConfigurationMap().get("aa"));
        assertEquals("22", getConfigurationMap().get("bb"));

        sessionContext.reset("aa");
        assertNull(getConfigurationMap().get("aa"));
        assertEquals("22", getConfigurationMap().get("bb"));

        sessionContext.reset("bb");
        assertNull(getConfigurationMap().get("bb"));
    }

    @Test
    public void testAddJarWithFullPath() throws IOException {
        validateAddJar(udfJar.getPath());
    }

    @Test
    public void testAddJarWithRelativePath() throws IOException {
        validateAddJar(
                new File(".").getCanonicalFile().toPath().relativize(udfJar.toPath()).toString());
    }

    @Test
    public void testAddIllegalJar() {
        validateAddJarWithException("/path/to/illegal.jar", "JAR file does not exist");
    }

    @Test
    public void testRemoteJar() {
        validateAddJarWithException(
                "hdfs://remote:10080/remote.jar", "SQL Client only supports to add local jars.");
    }

    @Test
    public void testIllegalJarInConfig() {
        Configuration innerConfig = (Configuration) sessionContext.getReadableConfig();
        innerConfig.set(JARS, Collections.singletonList("/path/to/illegal.jar"));

        validateAddJarWithException(udfJar.getPath(), "no protocol: /path/to/illegal.jar");
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

    private Map<String, String> getConfigurationMap() {
        return sessionContext
                .getExecutionContext()
                .getTableEnvironment()
                .getConfig()
                .getConfiguration()
                .toMap();
    }

    private Configuration getConfiguration() {
        return sessionContext
                .getExecutionContext()
                .getTableEnvironment()
                .getConfig()
                .getConfiguration();
    }

    private void validateAddJar(String jarPath) throws IOException {
        sessionContext.addJar(jarPath);
        assertEquals(
                Collections.singletonList(udfJar.toURI().toURL().toString()),
                getConfiguration().get(JARS));
        // reset to the default
        sessionContext.reset();
        assertEquals(
                Collections.singletonList(udfJar.toURI().toURL().toString()),
                getConfiguration().get(JARS));
    }

    private void validateAddJarWithException(String jarPath, String errorMessages) {
        Set<URL> originDependencies = sessionContext.getDependencies();
        try {
            sessionContext.addJar(jarPath);
            fail("Should fail.");
        } catch (Exception e) {
            assertThat(e, containsMessage(errorMessages));
            // Keep dependencies as same as before if fail to add jar
            assertEquals(originDependencies, sessionContext.getDependencies());
        }
    }
}
