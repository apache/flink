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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the "info" command. */
class CliFrontendInfoTest extends CliFrontendTestBase {

    private static PrintStream stdOut;
    private static PrintStream capture;
    private static ByteArrayOutputStream buffer;

    @Test
    void testMissingOption() {
        assertThatThrownBy(
                        () -> {
                            String[] parameters = {};
                            Configuration configuration = getConfiguration();
                            CliFrontend testFrontend =
                                    new CliFrontend(
                                            configuration, Collections.singletonList(getCli()));
                            testFrontend.info(parameters);
                        })
                .isInstanceOf(CliArgsException.class);
    }

    @Test
    void testUnrecognizedOption() {
        assertThatThrownBy(
                        () -> {
                            String[] parameters = {"-v", "-l"};
                            Configuration configuration = getConfiguration();
                            CliFrontend testFrontend =
                                    new CliFrontend(
                                            configuration, Collections.singletonList(getCli()));
                            testFrontend.info(parameters);
                        })
                .isInstanceOf(CliArgsException.class);
    }

    @Test
    void testShowExecutionPlan() throws Exception {
        replaceStdOut();
        try {

            String[] parameters =
                    new String[] {
                        CliFrontendTestUtils.getTestJarPath(), "-f", "true", "--arg", "suffix"
                    };
            Configuration configuration = getConfiguration();
            CliFrontend testFrontend =
                    new CliFrontend(configuration, Collections.singletonList(getCli()));
            testFrontend.info(parameters);
            assertThat(buffer.toString()).contains("\"parallelism\" : 4");
        } finally {
            restoreStdOut();
        }
    }

    @Test
    void testShowExecutionPlanWithCliOptionParallelism() throws Exception {
        final String[] parameters = {
            "-p", "17", CliFrontendTestUtils.getTestJarPath(), "--arg", "suffix"
        };
        testShowExecutionPlanWithParallelism(parameters, 17);
    }

    @Test
    void testShowExecutionPlanWithDynamicPropertiesParallelism() throws Exception {
        final String[] parameters = {
            "-Dparallelism.default=15", CliFrontendTestUtils.getTestJarPath(), "--arg", "suffix"
        };
        testShowExecutionPlanWithParallelism(parameters, 15);
    }

    void testShowExecutionPlanWithParallelism(String[] parameters, int expectedParallelism) {
        replaceStdOut();
        try {
            Configuration configuration = getConfiguration();
            CliFrontend testFrontend =
                    new CliFrontend(configuration, Collections.singletonList(getCli()));
            testFrontend.info(parameters);
            assertThat(buffer.toString()).contains("\"parallelism\" : " + expectedParallelism);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Program caused an exception: " + e.getMessage());
        } finally {
            restoreStdOut();
        }
    }

    private static void replaceStdOut() {
        stdOut = System.out;
        buffer = new ByteArrayOutputStream();
        capture = new PrintStream(buffer);
        System.setOut(capture);
    }

    private static void restoreStdOut() {
        System.setOut(stdOut);
    }
}
