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
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

/** Tests for the {@link StandaloneApplicationClusterConfigurationParserFactory}. */
class StandaloneApplicationClusterConfigurationParserFactoryTest {

    private File confFile;
    private String confDirPath;

    @BeforeEach
    void createEmptyFlinkConfiguration(@TempDir Path tempFolder) throws IOException {
        confDirPath = tempFolder.toFile().getAbsolutePath();
        confFile = new File(tempFolder.toFile(), GlobalConfiguration.FLINK_CONF_FILENAME);
        confFile.createNewFile();
    }

    private static final CommandLineParser<StandaloneApplicationClusterConfiguration>
            commandLineParser =
                    new CommandLineParser<>(
                            new StandaloneApplicationClusterConfigurationParserFactory());
    private static final String JOB_CLASS_NAME = "foobar";

    @Test
    void testEntrypointClusterConfigurationToConfigurationParsing() throws FlinkParseException {
        final JobID jobID = JobID.generate();
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath("/test/savepoint/path", true);
        final String key = DeploymentOptions.TARGET.key();
        final String value = "testDynamicExecutorConfig";
        final int restPort = 1234;
        final String arg1 = "arg1";
        final String arg2 = "arg2";
        final String[] args = {
            "--configDir",
            confDirPath,
            "--job-id",
            jobID.toHexString(),
            "--fromSavepoint",
            savepointRestoreSettings.getRestorePath(),
            "--allowNonRestoredState",
            "--webui-port",
            String.valueOf(restPort),
            "--job-classname",
            JOB_CLASS_NAME,
            String.format("-D%s=%s", key, value),
            arg1,
            arg2
        };

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                commandLineParser.parse(args);
        assertThat(clusterConfiguration.getJobClassName()).isEqualTo(JOB_CLASS_NAME);
        assertThat(clusterConfiguration.getArgs()).contains(arg1, arg2);

        final Configuration configuration =
                StandaloneApplicationClusterEntryPoint.loadConfigurationFromClusterConfig(
                        clusterConfiguration);

        final String strJobId = configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);
        assertThat(JobID.fromHexString(strJobId)).isEqualTo(jobID);
        assertThat(SavepointRestoreSettings.fromConfiguration(configuration))
                .isEqualTo(savepointRestoreSettings);

        assertThat(configuration.get(RestOptions.PORT)).isEqualTo(restPort);
        assertThat(configuration.get(DeploymentOptions.TARGET)).isEqualTo(value);
    }

    @Test
    void testEntrypointClusterConfigWOSavepointSettingsToConfigurationParsing()
            throws FlinkParseException {
        final JobID jobID = JobID.generate();
        final String[] args = {"-c", confDirPath, "--job-id", jobID.toHexString()};

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                commandLineParser.parse(args);
        final Configuration configuration =
                StandaloneApplicationClusterEntryPoint.loadConfigurationFromClusterConfig(
                        clusterConfiguration);

        final String strJobId = configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);
        assertThat(JobID.fromHexString(strJobId)).isEqualTo(jobID);
        assertThat(SavepointRestoreSettings.fromConfiguration(configuration))
                .isEqualTo(SavepointRestoreSettings.none());
    }

    @Test
    void testEntrypointClusterConfigurationParsing() throws FlinkParseException {
        final String key = "key";
        final String value = "value";
        final int restPort = 1234;
        final String arg1 = "arg1";
        final String arg2 = "arg2";
        final String[] args = {
            "--configDir",
            confDirPath,
            "--webui-port",
            String.valueOf(restPort),
            "--job-classname",
            JOB_CLASS_NAME,
            String.format("-D%s=%s", key, value),
            arg1,
            arg2
        };

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                commandLineParser.parse(args);

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(confDirPath);
        assertThat(clusterConfiguration.getJobClassName()).isEqualTo(JOB_CLASS_NAME);
        assertThat(clusterConfiguration.getRestPort()).isEqualTo(restPort);
        final Properties dynamicProperties = clusterConfiguration.getDynamicProperties();

        assertThat(dynamicProperties).containsEntry(key, value);

        assertThat(clusterConfiguration.getArgs()).contains(arg1, arg2);

        assertThat(clusterConfiguration.getSavepointRestoreSettings())
                .isEqualTo(SavepointRestoreSettings.none());

        assertThat(clusterConfiguration.getJobId()).isNull();
    }

    @Test
    void testOnlyRequiredArguments() throws FlinkParseException {
        final String[] args = {"--configDir", confDirPath};

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                commandLineParser.parse(args);

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(confDirPath);
        assertThat(clusterConfiguration.getDynamicProperties()).isEqualTo(new Properties());
        assertThat(clusterConfiguration.getArgs()).isEqualTo(new String[0]);
        assertThat(clusterConfiguration.getRestPort()).isEqualTo(-1);
        assertThat(clusterConfiguration.getHostname()).isNull();
        assertThat(clusterConfiguration.getSavepointRestoreSettings())
                .isEqualTo(SavepointRestoreSettings.none());
        assertThat(clusterConfiguration.getJobId()).isNull();
        assertThat(clusterConfiguration.getJobClassName()).isNull();
    }

    @Test
    void testMissingRequiredArgument() {
        final String[] args = {};
        assertThatThrownBy(() -> commandLineParser.parse(args))
                .isInstanceOf(FlinkParseException.class);
    }

    @Test
    void testSavepointRestoreSettingsParsing() throws FlinkParseException {
        final String restorePath = "foobar";
        final String[] args = {"-c", confDirPath, "-j", JOB_CLASS_NAME, "-s", restorePath, "-n"};
        final StandaloneApplicationClusterConfiguration standaloneApplicationClusterConfiguration =
                commandLineParser.parse(args);

        final SavepointRestoreSettings savepointRestoreSettings =
                standaloneApplicationClusterConfiguration.getSavepointRestoreSettings();

        assertThat(savepointRestoreSettings.restoreSavepoint()).isTrue();
        assertThat(savepointRestoreSettings.getRestorePath()).isEqualTo(restorePath);
        assertThat(savepointRestoreSettings.allowNonRestoredState()).isTrue();
    }

    @Test
    void testSetJobIdManually() throws FlinkParseException {
        final JobID jobId = new JobID();
        final String[] args = {
            "--configDir", confDirPath, "--job-classname", "foobar", "--job-id", jobId.toString()
        };

        final StandaloneApplicationClusterConfiguration standaloneApplicationClusterConfiguration =
                commandLineParser.parse(args);

        assertThat(standaloneApplicationClusterConfiguration.getJobId()).isEqualTo(jobId);
    }

    @Test
    void testInvalidJobIdThrows() {
        final String invalidJobId = "0xINVALID";
        final String[] args = {
            "--configDir", confDirPath, "--job-classname", "foobar", "--job-id", invalidJobId
        };

        try {
            commandLineParser.parse(args);
            fail("Did not throw expected FlinkParseException");
        } catch (FlinkParseException e) {
            Optional<IllegalArgumentException> cause =
                    ExceptionUtils.findThrowable(e, IllegalArgumentException.class);
            assertThat(cause).isPresent();
            assertThat(cause.get().getMessage()).containsSequence(invalidJobId);
        }
    }

    @Test
    void testShortOptions() throws FlinkParseException {
        final String jobClassName = "foobar";
        final JobID jobId = new JobID();
        final String savepointRestorePath = "s3://foo/bar";

        final String[] args = {
            "-c", confDirPath,
            "-j", jobClassName,
            "-jid", jobId.toString(),
            "-s", savepointRestorePath,
            "-n"
        };

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                commandLineParser.parse(args);

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(confDirPath);
        assertThat(clusterConfiguration.getJobClassName()).isEqualTo(jobClassName);
        assertThat(clusterConfiguration.getJobId()).isEqualTo(jobId);

        final SavepointRestoreSettings savepointRestoreSettings =
                clusterConfiguration.getSavepointRestoreSettings();
        assertThat(savepointRestoreSettings.restoreSavepoint()).isTrue();
        assertThat(savepointRestoreSettings.getRestorePath()).isEqualTo(savepointRestorePath);
        assertThat(savepointRestoreSettings.allowNonRestoredState()).isTrue();
    }

    @Test
    void testHostOption() throws FlinkParseException {
        final String hostName = "user-specified-hostname";
        final String[] args = {
            "--configDir", confDirPath, "--job-classname", "foobar", "--host", hostName
        };
        final StandaloneApplicationClusterConfiguration applicationClusterConfiguration =
                commandLineParser.parse(args);
        assertThat(applicationClusterConfiguration.getHostname()).isEqualTo(hostName);
    }
}
