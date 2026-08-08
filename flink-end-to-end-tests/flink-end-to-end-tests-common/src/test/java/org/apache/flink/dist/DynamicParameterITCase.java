/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.test.util.FileUtils;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.FlinkDistribution;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicParameterITCase {

    private static final Pattern ENTRYPOINT_LOG_PATTERN =
            Pattern.compile(".*ClusterEntrypoint +\\[] - +(.*)");
    private static final Pattern ENTRYPOINT_CLASSPATH_LOG_PATTERN =
            Pattern.compile(".*ClusterEntrypoint +\\[] - +Classpath:.*");

    private static final Duration LOG_TIMEOUT = Duration.ofMinutes(1);

    private static final String HOST = "test_host";
    private static final int PORT = 8082;

    private static final String DYNAMIC_KEY = "hello";
    private static final String DYNAMIC_VALUE = "world";
    private static final String DYNAMIC_PROPERTY = DYNAMIC_KEY + "=" + DYNAMIC_VALUE;

    private static final Path originalDist = FileUtils.findFlinkDist();

    private FlinkDistribution dist;

    @BeforeEach
    void setup(@TempDir Path tmp) throws IOException {
        TestUtils.copyDirectory(originalDist, tmp);
        dist = new FlinkDistribution(tmp);
    }

    @AfterEach
    void cleanup() throws IOException {
        if (dist != null) {
            dist.stopFlinkCluster();
        }
    }

    @Test
    void testWithoutAnyParameter() throws Exception {
        assertParameterPassing(dist, false, false, false);
    }

    @Test
    void testWithHost() throws Exception {
        assertParameterPassing(dist, true, false, false);
    }

    @Test
    void testWithHostAndPort() throws Exception {
        assertParameterPassing(dist, true, true, false);
    }

    @Test
    void testWithDynamicParameter() throws Exception {
        assertParameterPassing(dist, false, false, true);
    }

    @Test
    void testWithDynamicParameterAndHost() throws Exception {
        assertParameterPassing(dist, true, false, true);
    }

    @Test
    void testWithDynamicParameterAndHostAndPort() throws Exception {
        assertParameterPassing(dist, true, true, true);
    }

    private static void assertParameterPassing(
            FlinkDistribution dist, boolean withHost, boolean withPort, boolean withDynamicProperty)
            throws Exception {

        final List<String> args = new ArrayList<>();
        args.add("start");

        if (withHost) {
            args.add("-D");
            args.add("jobmanager.rpc.address=" + HOST);
        }
        if (withPort) {
            Preconditions.checkState(withHost, "port may only be supplied with a host");
            args.add("-D");
            args.add("rest.port=" + PORT);
        }
        if (withDynamicProperty) {
            args.add("-D");
            args.add(DYNAMIC_PROPERTY);
        }

        dist.callJobManagerScript(args.toArray(new String[0]));

        // The backgrounded JobManager writes its log asynchronously; wait until the complete
        // program arguments block has been flushed, otherwise the parser may observe a
        // partially-written block or never observe it at all.
        final String[] programArguments = waitForProgramArguments(dist);

        try {
            final EntrypointClusterConfiguration entrypointConfig =
                    ClusterEntrypoint.parseArguments(programArguments);

            final Configuration configuration = loadConfiguration(entrypointConfig);

            assertThat(configuration.get(JobManagerOptions.ADDRESS))
                    .isEqualTo(withHost ? HOST : "localhost");
            assertThat(configuration.get(RestOptions.PORT)).isEqualTo(withPort ? PORT : 8081);

            if (withDynamicProperty) {
                assertThat(entrypointConfig.getDynamicProperties())
                        .containsEntry(DYNAMIC_KEY, DYNAMIC_VALUE);
            } else {
                assertThat(entrypointConfig.getDynamicProperties())
                        .doesNotContainEntry(DYNAMIC_KEY, DYNAMIC_VALUE);
            }
        } catch (FlinkParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static String[] waitForProgramArguments(FlinkDistribution dist) throws Exception {
        final List<String[]> captured = new ArrayList<>(1);
        CommonTestUtils.waitUtil(
                () -> {
                    // The "Classpath:" line is logged after the program arguments, so its presence
                    // means the complete arguments block has been flushed.
                    if (!allProgramArgumentsLogged(dist)) {
                        return false;
                    }
                    captured.clear();
                    captured.add(readProgramArguments(dist));
                    return true;
                },
                LOG_TIMEOUT,
                Duration.ofMillis(500),
                "The JobManager did not log its complete program arguments in time.");
        return captured.get(0);
    }

    private static String[] readProgramArguments(FlinkDistribution dist) {
        try (Stream<String> lines =
                dist.searchAllLogs(ENTRYPOINT_LOG_PATTERN, matcher -> matcher.group(1), true)) {
            return lines.filter(new ProgramArgumentsFilter()).toArray(String[]::new);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Configuration loadConfiguration(
            EntrypointClusterConfiguration entrypointClusterConfiguration) {
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(
                        entrypointClusterConfiguration.getDynamicProperties());
        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

        return configuration;
    }

    private static boolean allProgramArgumentsLogged(FlinkDistribution dist) {
        // the classpath is logged after the program arguments
        try (Stream<String> lines =
                dist.searchAllLogs(
                        ENTRYPOINT_CLASSPATH_LOG_PATTERN, matcher -> matcher.group(0), true)) {
            return lines.iterator().hasNext();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class ProgramArgumentsFilter implements Predicate<String> {

        private boolean inProgramArguments = false;

        @Override
        public boolean test(String s) {
            if (s.contains("Program Arguments:")) {
                inProgramArguments = true;
                return false;
            }
            if (s.contains("Classpath:")) {
                inProgramArguments = false;
            }
            return inProgramArguments;
        }
    }
}
