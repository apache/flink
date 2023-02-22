/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.cli;

import org.apache.flink.client.deployment.executors.LocalExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link CliFrontend}. */
class CliFrontendITCase {

    private PrintStream originalPrintStream;

    private ByteArrayOutputStream testOutputStream;

    @BeforeEach
    void before() {
        originalPrintStream = System.out;
        testOutputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(testOutputStream));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalPrintStream);
    }

    private String getStdoutString() {
        return testOutputStream.toString();
    }

    @Test
    void configurationIsForwarded() throws Exception {
        Configuration config = new Configuration();
        CustomCommandLine commandLine = new DefaultCLI();

        config.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(42L));

        CliFrontend cliFrontend = new CliFrontend(config, Collections.singletonList(commandLine));

        cliFrontend.parseAndRun(
                new String[] {
                    "run", "-c", TestingJob.class.getName(), CliFrontendTestUtils.getTestJarPath()
                });

        assertThat(getStdoutString()).contains("Watermark interval is 42");
    }

    @Test
    void commandlineOverridesConfiguration() throws Exception {
        Configuration config = new Configuration();

        // we use GenericCli because it allows specifying arbitrary options via "-Dfoo=bar" syntax
        CustomCommandLine commandLine = new GenericCLI(config, "/dev/null");

        config.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(42L));

        CliFrontend cliFrontend = new CliFrontend(config, Collections.singletonList(commandLine));

        cliFrontend.parseAndRun(
                new String[] {
                    "run",
                    "-t",
                    LocalExecutor.NAME,
                    "-c",
                    TestingJob.class.getName(),
                    "-D" + PipelineOptions.AUTO_WATERMARK_INTERVAL.key() + "=142",
                    CliFrontendTestUtils.getTestJarPath()
                });

        assertThat(getStdoutString()).contains("Watermark interval is 142");
    }

    @Test
    void mainShouldPrintHelpWithoutArgs(@TempDir Path tempFolder) throws Exception {
        Map<String, String> originalEnv = System.getenv();
        try {
            File confFolder = Files.createTempDirectory(tempFolder, "conf").toFile();
            File confYaml = new File(confFolder, "flink-conf.yaml");
            if (!confYaml.createNewFile()) {
                throw new IOException("Can't create testing flink-conf.yaml file.");
            }

            Map<String, String> map = new HashMap<>(System.getenv());
            map.put(ENV_FLINK_CONF_DIR, confFolder.getAbsolutePath());
            CommonTestUtils.setEnv(map);

            assertThat(CliFrontend.mainInternal(new String[0])).isEqualTo(1);
            assertThat(getStdoutString()).contains("The following actions are available");
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }
    }

    /**
     * Testing job that the watermark interval from the {@link
     * org.apache.flink.api.common.ExecutionConfig}.
     */
    public static class TestingJob {
        public static void main(String[] args) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            System.out.println(
                    "Watermark interval is " + env.getConfig().getAutoWatermarkInterval());
        }
    }
}
