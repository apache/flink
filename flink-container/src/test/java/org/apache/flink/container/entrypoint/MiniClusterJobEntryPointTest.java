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

package org.apache.flink.container.entrypoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MiniClusterJobEntryPoint}. */
class MiniClusterJobEntryPointTest {

    private static final CommandLineParser<StandaloneApplicationClusterConfiguration> PARSER =
            new CommandLineParser<>(new StandaloneApplicationClusterConfigurationParserFactory());

    private String confDirPath;

    @BeforeEach
    void createFlinkConfiguration(@TempDir Path tempFolder) throws IOException {
        confDirPath = tempFolder.toFile().getAbsolutePath();
        new File(tempFolder.toFile(), GlobalConfiguration.FLINK_CONF_FILENAME).createNewFile();
    }

    @Test
    void programArgsAfterDoubleDashNotReadAsLauncherFlags() throws FlinkParseException {
        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                PARSER.parse(
                        new String[] {
                            "--configDir",
                            confDirPath,
                            "--jars",
                            "local:///opt/flink/usrlib/my-job.jar",
                            "--job-classname",
                            "com.acme.MyJob",
                            "--",
                            "--job-classname",
                            "not-a-flag",
                            "--fromSavepoint",
                            "nor-this"
                        });

        assertThat(clusterConfiguration.getJobClassName()).isEqualTo("com.acme.MyJob");
        assertThat(clusterConfiguration.getArgs())
                .containsExactly("--job-classname", "not-a-flag", "--fromSavepoint", "nor-this");
        assertThat(
                        SavepointRestoreSettings.fromConfiguration(loadFrom(clusterConfiguration))
                                .restoreSavepoint())
                .isFalse();
    }

    @Test
    void explicitJobIdTakesPrecedenceOverConfigFile(@TempDir Path tempFolder)
            throws IOException, FlinkParseException {
        final JobID fromConfigFile = JobID.generate();
        final JobID fromCommandLine = JobID.generate();
        final String otherConfDir = tempFolder.toFile().getAbsolutePath();
        Files.writeString(
                tempFolder.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key()
                        + ": "
                        + fromConfigFile.toHexString()
                        + System.lineSeparator());

        final StandaloneApplicationClusterConfiguration otherConfig =
                PARSER.parse(new String[] {"--configDir", otherConfDir});
        assertThat(loadFrom(otherConfig).get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID))
                .as("with no flag, the config file's value stands")
                .isEqualTo(fromConfigFile.toHexString());

        final StandaloneApplicationClusterConfiguration overrideConfig =
                PARSER.parse(
                        new String[] {
                            "--configDir", otherConfDir, "--job-id", fromCommandLine.toHexString()
                        });
        assertThat(loadFrom(overrideConfig).get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID))
                .as("an explicit --job-id overrides it")
                .isEqualTo(fromCommandLine.toHexString());
    }

    @Test
    void savepointRestoreFromCommandLine() throws FlinkParseException {
        final Configuration configuration =
                loadFrom(
                        PARSER.parse(
                                new String[] {
                                    "--configDir",
                                    confDirPath,
                                    "--fromSavepoint",
                                    "/some/savepoint",
                                    "--allowNonRestoredState"
                                }));

        final SavepointRestoreSettings settings =
                SavepointRestoreSettings.fromConfiguration(configuration);
        assertThat(settings.getRestorePath()).isEqualTo("/some/savepoint");
        assertThat(settings.allowNonRestoredState()).isTrue();
    }

    @Test
    void clusterLeftRunningAfterApplicationFinishesWhenConfigured() throws FlinkParseException {
        final StandaloneApplicationClusterConfiguration defaultConfig =
                PARSER.parse(new String[] {"--configDir", confDirPath});
        assertThat(loadFrom(defaultConfig).get(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH))
                .as("by default, cluster exits when the job finishes")
                .isTrue();

        final StandaloneApplicationClusterConfiguration customConfig =
                PARSER.parse(
                        new String[] {
                            "--configDir",
                            confDirPath,
                            "-D" + DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH.key() + "=false"
                        });
        assertThat(loadFrom(customConfig).get(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH))
                .as("if requested, cluster remains running when the job finishes")
                .isFalse();
    }

    private static Configuration loadFrom(
            StandaloneApplicationClusterConfiguration clusterConfiguration) {
        return ApplicationClusterEntryPointUtils.loadConfiguration(clusterConfiguration);
    }
}
