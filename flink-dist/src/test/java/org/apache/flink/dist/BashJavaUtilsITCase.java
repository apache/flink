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

package org.apache.flink.dist;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.util.bash.BashJavaUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for BashJavaUtils.
 *
 * <p>This test requires the distribution to be assembled and is hence marked as an IT case which
 * run after packaging.
 */
class BashJavaUtilsITCase extends JavaBashTestBase {

    private static final String RUN_BASH_JAVA_UTILS_CMD_SCRIPT =
            "src/test/bin/runBashJavaUtilsCmd.sh";
    private static final String RUN_EXTRACT_LOGGING_OUTPUTS_SCRIPT =
            "src/test/bin/runExtractLoggingOutputs.sh";

    @TempDir private Path tmpDir;

    @Test
    void testGetTmResourceParamsConfigs() throws Exception {
        int expectedResultLines = 2;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.GET_TM_RESOURCE_PARAMS.toString(),
            String.valueOf(expectedResultLines)
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
        ConfigurationUtils.parseJvmArgString(lines.get(0));
        ConfigurationUtils.parseTmResourceDynamicConfigs(lines.get(1));
    }

    @Test
    void testGetTmResourceParamsConfigsWithDynamicProperties() throws Exception {
        int expectedResultLines = 2;
        double cpuCores = 39.0;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.GET_TM_RESOURCE_PARAMS.toString(),
            String.valueOf(expectedResultLines),
            "-D" + TaskManagerOptions.CPU_CORES.key() + "=" + cpuCores
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
        Map<String, String> configs =
                ConfigurationUtils.parseTmResourceDynamicConfigs(lines.get(1));
        assertThat(Double.valueOf(configs.get(TaskManagerOptions.CPU_CORES.key())))
                .isEqualTo(cpuCores);
    }

    @Test
    void testGetJmResourceParams() throws Exception {
        int expectedResultLines = 2;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.GET_JM_RESOURCE_PARAMS.toString(),
            String.valueOf(expectedResultLines)
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);

        Map<String, String> jvmParams = ConfigurationUtils.parseJvmArgString(lines.get(0));
        Map<String, String> dynamicParams = parseAndAssertDynamicParameters(lines.get(1));
        assertJvmAndDynamicParametersMatch(jvmParams, dynamicParams);
    }

    @Test
    void testGetJmResourceParamsWithDynamicProperties() throws Exception {
        int expectedResultLines = 2;
        long metaspace = 123456789L;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.GET_JM_RESOURCE_PARAMS.toString(),
            String.valueOf(expectedResultLines),
            "-D" + JobManagerOptions.JVM_METASPACE.key() + "=" + metaspace + "b"
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);

        Map<String, String> jvmParams = ConfigurationUtils.parseJvmArgString(lines.get(0));
        Map<String, String> dynamicParams = parseAndAssertDynamicParameters(lines.get(1));
        assertJvmAndDynamicParametersMatch(jvmParams, dynamicParams);
        assertThat(Long.valueOf(jvmParams.get("-XX:MaxMetaspaceSize="))).isEqualTo(metaspace);
    }

    @Test
    void testGetConfiguration() throws Exception {
        int expectedResultLines = 25;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.UPDATE_AND_GET_FLINK_CONFIGURATION.toString(),
            String.valueOf(expectedResultLines)
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
    }

    @Test
    void testMigrateLegacyConfigToStandardYaml() throws Exception {
        int expectedResultLines = 31;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.MIGRATE_LEGACY_FLINK_CONFIGURATION_TO_STANDARD_YAML.toString(),
            String.valueOf(expectedResultLines)
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));
        assertThat(lines)
                .containsExactlyInAnyOrder(
                        "taskmanager:",
                        "  memory:",
                        "    process:",
                        "      size: 1728m",
                        "  bind-host: localhost",
                        "  host: localhost",
                        "  numberOfTaskSlots: '1'",
                        "jobmanager:",
                        "  execution:",
                        "    failover-strategy: region",
                        "  rpc:",
                        "    address: localhost",
                        "    port: '6123'",
                        "  memory:",
                        "    process:",
                        "      size: 1600m",
                        "  bind-host: localhost",
                        "rest:",
                        "  bind-address: localhost",
                        "  address: localhost",
                        "parallelism:",
                        "  default: '1'",
                        "env:",
                        "  java:",
                        "    opts:",
                        "      all: --add-exports=java.base/sun.net.util=ALL-UNNAMED "
                                + "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED "
                                + "--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED "
                                + "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED "
                                + "--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED "
                                + "--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED "
                                + "--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED "
                                + "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
                                + "--add-opens=java.base/java.lang=ALL-UNNAMED "
                                + "--add-opens=java.base/java.net=ALL-UNNAMED "
                                + "--add-opens=java.base/java.io=ALL-UNNAMED "
                                + "--add-opens=java.base/java.nio=ALL-UNNAMED "
                                + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                                + "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                                + "--add-opens=java.base/java.text=ALL-UNNAMED "
                                + "--add-opens=java.base/java.time=ALL-UNNAMED "
                                + "--add-opens=java.base/java.util=ALL-UNNAMED "
                                + "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                                + "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                                + "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
                        "list: a;b;c",
                        "map: a:b,c:d",
                        "listMap: a:b,c:d;e:f",
                        "escape:",
                        "  key: '*'");
    }

    @Test
    void testGetConfigurationRemoveKey() throws Exception {
        int expectedResultLines = 23;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.UPDATE_AND_GET_FLINK_CONFIGURATION.toString(),
            String.valueOf(expectedResultLines),
            "-rmKey",
            "parallelism.default"
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
        Configuration configuration = loadConfiguration(lines);
        assertThat(configuration.getOptional(CoreOptions.DEFAULT_PARALLELISM)).isEmpty();
    }

    @Test
    void testGetConfigurationRemoveKeyValue() throws Exception {
        int expectedResultLines = 23;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.UPDATE_AND_GET_FLINK_CONFIGURATION.toString(),
            String.valueOf(expectedResultLines),
            "-rmKV",
            "parallelism.default=1"
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
        Configuration configuration = loadConfiguration(lines);
        assertThat(configuration.getOptional(CoreOptions.DEFAULT_PARALLELISM)).isEmpty();
    }

    @Test
    void testGetConfigurationRemoveKeyValueNotMatchingValue() throws Exception {
        int expectedResultLines = 25;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.UPDATE_AND_GET_FLINK_CONFIGURATION.toString(),
            String.valueOf(expectedResultLines),
            "-rmKV",
            "parallelism.default=2"
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
        Configuration configuration = loadConfiguration(lines);
        assertThat(configuration.getOptional(CoreOptions.DEFAULT_PARALLELISM)).hasValue(1);
    }

    @Test
    void testGetConfigurationReplaceKeyValue() throws Exception {
        int expectedResultLines = 25;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.UPDATE_AND_GET_FLINK_CONFIGURATION.toString(),
            String.valueOf(expectedResultLines),
            "-repKV",
            "parallelism.default,1,2"
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
        Configuration configuration = loadConfiguration(lines);
        assertThat(configuration.getOptional(CoreOptions.DEFAULT_PARALLELISM)).hasValue(2);
    }

    @Test
    void testGetConfigurationReplaceKeyValueNotMatchingValue() throws Exception {
        int expectedResultLines = 25;
        String[] commands = {
            RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
            BashJavaUtils.Command.UPDATE_AND_GET_FLINK_CONFIGURATION.toString(),
            String.valueOf(expectedResultLines),
            "-repKV",
            "parallelism.default,2,3"
        };
        List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(lines).hasSize(expectedResultLines);
        Configuration configuration = loadConfiguration(lines);
        assertThat(configuration.getOptional(CoreOptions.DEFAULT_PARALLELISM)).hasValue(1);
    }

    private static Map<String, String> parseAndAssertDynamicParameters(
            String dynamicParametersStr) {
        Set<String> expectedDynamicParameters =
                Sets.newHashSet(
                        JobManagerOptions.JVM_HEAP_MEMORY.key(),
                        JobManagerOptions.OFF_HEAP_MEMORY.key(),
                        JobManagerOptions.JVM_METASPACE.key(),
                        JobManagerOptions.JVM_OVERHEAD_MIN.key(),
                        JobManagerOptions.JVM_OVERHEAD_MAX.key());

        Map<String, String> actualDynamicParameters = new HashMap<>();
        String[] dynamicParameterTokens = dynamicParametersStr.split(" ");
        assertThat(dynamicParameterTokens.length % 2).isZero();
        for (int i = 0; i < dynamicParameterTokens.length; ++i) {
            String parameterKeyValueStr = dynamicParameterTokens[i];
            if (i % 2 == 0) {
                assertThat(parameterKeyValueStr).isEqualTo("-D");
            } else {
                String[] parameterKeyValue = parameterKeyValueStr.split("=");
                assertThat(parameterKeyValue).hasSize(2);
                assertThat(parameterKeyValue[0]).isIn(expectedDynamicParameters);

                actualDynamicParameters.put(parameterKeyValue[0], parameterKeyValue[1]);
            }
        }

        return actualDynamicParameters;
    }

    private static void assertJvmAndDynamicParametersMatch(
            Map<String, String> jvmParams, Map<String, String> dynamicParams) {
        assertThat(jvmParams.get("-Xmx") + "b")
                .isEqualTo(dynamicParams.get(JobManagerOptions.JVM_HEAP_MEMORY.key()));
        assertThat(jvmParams.get("-Xms") + "b")
                .isEqualTo(dynamicParams.get(JobManagerOptions.JVM_HEAP_MEMORY.key()));
        assertThat(jvmParams.get("-XX:MaxMetaspaceSize=") + "b")
                .isEqualTo(dynamicParams.get(JobManagerOptions.JVM_METASPACE.key()));
    }

    @Test
    void testExtractLoggingOutputs() throws Exception {
        StringBuilder input = new StringBuilder();
        List<String> expectedOutput = new ArrayList<>();

        for (int i = 0; i < 5; ++i) {
            String line = "BashJavaUtils output line " + i + " `~!@#$%^&*()-_=+;:,.'\"\\\t/?";
            if (i % 2 == 0) {
                expectedOutput.add(line);
            } else {
                line = BashJavaUtils.EXECUTION_PREFIX + line;
            }
            input.append(line + "\n");
        }

        String[] commands = {RUN_EXTRACT_LOGGING_OUTPUTS_SCRIPT, input.toString()};
        List<String> actualOutput =
                Arrays.asList(executeScript(commands).split(System.lineSeparator()));

        assertThat(actualOutput).isEqualTo(expectedOutput);
    }

    private Configuration loadConfiguration(List<String> lines) throws IOException {
        File file =
                TempDirUtils.newFile(
                        tmpDir.toAbsolutePath(), GlobalConfiguration.FLINK_CONF_FILENAME);
        try (final PrintWriter pw = new PrintWriter(file)) {
            for (String line : lines) {
                pw.println(line);
            }
        }
        return GlobalConfiguration.loadConfiguration(tmpDir.toAbsolutePath().toString());
    }
}
