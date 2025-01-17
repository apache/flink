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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests to deploy script into mini yarn cluster. */
public class SqlYARNApplicationITCase extends YarnTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlYARNApplicationITCase.class);

    private static final Duration yarnAppTerminateTimeout = Duration.ofSeconds(30);
    private static final int sleepIntervalInMS = 100;
    private static @TempDir Path workDir;
    private static File script;

    @BeforeAll
    static void setup() throws Exception {
        YARN_CONFIGURATION.set(
                YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-sql-yarn-test-application");
        startYARNWithConfig(YARN_CONFIGURATION, true);
        script = workDir.resolve("script.sql").toFile();
        assertThat(script.createNewFile()).isTrue();
        FileUtils.writeFileUtf8(
                script,
                "CREATE TEMPORARY TABLE sink(\n"
                        + "  a INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'blackhole'\n"
                        + ");\n"
                        + "INSERT INTO sink VALUES (1), (2), (3);");
    }

    @Test
    void testDeployScriptViaSqlClient() throws Exception {
        runTest(this::runSqlClient);
    }

    private void runSqlClient() throws Exception {
        File sqlClientScript =
                new File(
                        flinkLibFolder
                                .getParentFile()
                                .toPath()
                                .resolve("bin")
                                .resolve("sql-client.sh")
                                .toUri());
        if (!sqlClientScript.exists()) {
            throw new RuntimeException();
        } else {
            // make sure the subprocess has permission to execute the file.
            Runtime.getRuntime().exec("chmod +x " + sqlClientScript.getCanonicalPath()).waitFor();
        }

        List<String> parameters = new ArrayList<>();
        // command line parameters: sql-client.sh -Dkey=value -f <path-to-script>
        parameters.add(sqlClientScript.getCanonicalPath());
        parameters.add(
                getSqlClientParameter(JobManagerOptions.TOTAL_PROCESS_MEMORY.key(), "768MB"));
        parameters.add(getSqlClientParameter(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), "1g"));
        parameters.add(getSqlClientParameter(RpcOptions.ASK_TIMEOUT_DURATION.key(), "30s"));
        parameters.add(
                getSqlClientParameter(
                        DeploymentOptions.TARGET.key(),
                        YarnDeploymentTarget.APPLICATION.getName()));
        parameters.add(
                getSqlClientParameter(
                        CLASSPATH_INCLUDE_USER_JAR.key(),
                        YarnConfigOptions.UserJarInclusion.LAST.name()));
        parameters.add("-f");
        parameters.add(script.getAbsolutePath());

        LOG.info("Running process with parameters: {}", parameters);

        ProcessBuilder builder = new ProcessBuilder(parameters);
        // prepare environment
        builder.environment().put("HADOOP_CLASSPATH", getYarnClasspath());
        builder.environment().putAll(env);
        Process process = builder.start();

        // start to deploy script
        StringBuilder output = new StringBuilder();
        consumeOutput(process.getErrorStream(), line -> output.append(line).append("\n"));
        consumeOutput(process.getInputStream(), line -> output.append(line).append("\n"));

        process.waitFor(120, TimeUnit.SECONDS);

        // validate results
        assertThat(output).contains("Deploy script in application mode:");
        assertThat(output).contains("Cluster ID:");

        Pattern pattern = Pattern.compile("Cluster ID: (application_\\w+_\\w+)");
        Matcher matcher = pattern.matcher(output.toString());
        assertThat(matcher.find()).isTrue();
        ApplicationId applicationId = ApplicationId.fromString(matcher.group(1));

        try (final YarnClusterDescriptor yarnClusterDescriptor =
                createYarnClusterDescriptor(
                        Configuration.fromMap(
                                Collections.singletonMap(
                                        DeploymentOptions.TARGET.key(),
                                        YarnDeploymentTarget.APPLICATION.getName())))) {
            waitApplicationFinishedElseKillIt(
                    applicationId,
                    yarnAppTerminateTimeout,
                    yarnClusterDescriptor,
                    sleepIntervalInMS);
        }
    }

    private String getSqlClientParameter(String key, String value) {
        return String.format("-D%s=%s", key, value);
    }

    private static void consumeOutput(
            final InputStream stream, final Consumer<String> streamConsumer) {
        new Thread(
                        () -> {
                            try (BufferedReader bufferedReader =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    stream, StandardCharsets.UTF_8))) {
                                String line;
                                while ((line = bufferedReader.readLine()) != null) {
                                    streamConsumer.accept(line);
                                }
                            } catch (IOException e) {
                                LOG.error("Failure while processing process stdout/stderr.", e);
                            }
                        })
                .start();
    }
}
