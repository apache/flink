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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.client.gateway.utils.UserDefinedFunctions;
import org.apache.flink.table.utils.TestUserClassLoaderJar;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.PipelineOptions.JARS;
import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Test {@link SessionContext}. */
public class SessionContextTest {

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static File udfJar;

    private SessionContext sessionContext;

    @BeforeClass
    public static void prepare() throws Exception {
        udfJar =
                TestUserClassLoaderJar.createJarFile(
                        tempFolder.newFolder("test-jar"),
                        "test-classloader-udf.jar",
                        UserDefinedFunctions.GENERATED_UDF_CLASS,
                        UserDefinedFunctions.GENERATED_UDF_CODE);
    }

    @Before
    public void setup() {
        sessionContext = createSessionContext();
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

        assertThat(getConfigurationMap().get("aa")).isEqualTo("11");
        assertThat(getConfigurationMap().get("bb")).isEqualTo("22");

        sessionContext.reset("aa");
        assertThat(getConfigurationMap().get("aa")).isNull();
        assertThat(getConfigurationMap().get("bb")).isEqualTo("22");

        sessionContext.reset("bb");
        assertThat(getConfigurationMap().get("bb")).isNull();
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
    public void testAddRemoteJar() {
        validateAddJarWithException(
                "hdfs://remote:10080/remote.jar", "SQL Client only supports to add local jars.");
    }

    @Test
    public void testAddIllegalJarInConfig() {
        Configuration innerConfig = (Configuration) sessionContext.getReadableConfig();
        innerConfig.set(JARS, Collections.singletonList("/path/to/illegal.jar"));

        validateAddJarWithException(udfJar.getPath(), "no protocol: /path/to/illegal.jar");
    }

    @Test
    public void testRemoveJarWithFullPath() {
        validateRemoveJar(udfJar.getPath());
    }

    @Test
    public void testRemoveJarWithRelativePath() throws IOException {
        validateRemoveJar(
                new File(".").getCanonicalFile().toPath().relativize(udfJar.toPath()).toString());
    }

    @Test
    public void testRemoveIllegalJar() {
        validateRemoveJarWithException("/path/to/illegal.jar", "JAR file does not exist");
    }

    @Test
    public void testRemoveRemoteJar() {
        Configuration innerConfig = (Configuration) sessionContext.getReadableConfig();
        innerConfig.set(JARS, Collections.singletonList("hdfs://remote:10080/remote.jar"));

        validateRemoveJarWithException(
                "hdfs://remote:10080/remote.jar", "SQL Client only supports to remove local jars.");
    }

    // --------------------------------------------------------------------------------------------

    private SessionContext createSessionContext() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(OBJECT_REUSE, true);
        flinkConfig.set(MAX_PARALLELISM, 16);
        DefaultContext defaultContext =
                new DefaultContext(
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

    private ReadableConfig getConfiguration() {
        return sessionContext.getExecutionContext().getTableEnvironment().getConfig();
    }

    private void validateAddJar(String jarPath) throws IOException {
        sessionContext.addJar(jarPath);
        assertThat(sessionContext.listJars()).containsExactly(udfJar.getPath());
        assertThat(getConfiguration().get(JARS)).containsExactly(udfJar.toURI().toURL().toString());
        // reset to the default
        sessionContext.reset();
        assertThat(sessionContext.listJars()).containsExactly(udfJar.getPath());
        assertThat(getConfiguration().get(JARS)).containsExactly(udfJar.toURI().toURL().toString());
    }

    private void validateRemoveJar(String jarPath) {
        sessionContext.addJar(jarPath);
        assertThat(sessionContext.listJars()).containsExactly(udfJar.getPath());

        sessionContext.removeJar(jarPath);
        assertThat(sessionContext.listJars()).isEmpty();
    }

    private void validateAddJarWithException(String jarPath, String errorMessages) {
        Set<URL> originDependencies = sessionContext.getDependencies();
        try {
            sessionContext.addJar(jarPath);
            fail("Should fail.");
        } catch (Exception e) {
            assertThat(e).satisfies(matching(containsMessage(errorMessages)));
            // Keep dependencies as same as before if fail to add jar
            assertThat(sessionContext.getDependencies()).isEqualTo(originDependencies);
        }
    }

    private void validateRemoveJarWithException(String jarPath, String errorMessages) {
        Set<URL> originDependencies = sessionContext.getDependencies();
        try {
            sessionContext.removeJar(jarPath);
            fail("Should fail.");
        } catch (Exception e) {
            assertThat(e).satisfies(matching(containsMessage(errorMessages)));
            // Keep dependencies as same as before if fail to remove jar
            assertThat(sessionContext.getDependencies()).isEqualTo(originDependencies);
        }
    }
}
