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
import java.nio.file.Path;
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

    private static final String HOST = "localhost";
    private static final int PORT = 8081;

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
            args.add(HOST);
        }
        if (withPort) {
            Preconditions.checkState(withHost, "port may only be supplied with a host");
            args.add(String.valueOf(PORT));
        }
        if (withDynamicProperty) {
            args.add("-D");
            args.add(DYNAMIC_PROPERTY);
        }

        dist.callJobManagerScript(args.toArray(new String[0]));

        while (!allProgramArgumentsLogged(dist)) {
            Thread.sleep(500);
        }

        try (Stream<String> lines =
                dist.searchAllLogs(ENTRYPOINT_LOG_PATTERN, matcher -> matcher.group(1))) {

            final EntrypointClusterConfiguration entrypointConfig =
                    ClusterEntrypoint.parseArguments(
                            lines.filter(new ProgramArgumentsFilter()).toArray(String[]::new));

            assertThat(entrypointConfig.getHostname()).isEqualTo(withHost ? HOST : null);
            assertThat(entrypointConfig.getRestPort()).isEqualTo(withPort ? PORT : -1);

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

    private static boolean allProgramArgumentsLogged(FlinkDistribution dist) throws IOException {
        // the classpath is logged after the program arguments
        try (Stream<String> lines =
                dist.searchAllLogs(ENTRYPOINT_CLASSPATH_LOG_PATTERN, matcher -> matcher.group(0))) {
            return lines.iterator().hasNext();
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
