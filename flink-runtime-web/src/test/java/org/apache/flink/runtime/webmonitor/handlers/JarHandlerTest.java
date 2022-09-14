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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JarRunHandler} and {@link JarPlanHandler}. */
class JarHandlerTest {

    private static final String JAR_NAME = "output-test-program.jar";

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testPlanJar(@TempDir File tmp1, @TempDir File tmp2) throws Exception {
        final TestingDispatcherGateway restfulGateway =
                TestingDispatcherGateway.newBuilder().build();

        final JarHandlers handlers =
                new JarHandlers(tmp1.toPath(), restfulGateway, EXECUTOR_EXTENSION.getExecutor());

        final Path originalJar = Paths.get(System.getProperty("targetDir")).resolve(JAR_NAME);
        final Path jar = Files.copy(originalJar, tmp2.toPath().resolve(JAR_NAME));

        final String storedJarPath =
                JarHandlers.uploadJar(handlers.uploadHandler, jar, restfulGateway);
        final String storedJarName = Paths.get(storedJarPath).getFileName().toString();

        assertThatThrownBy(
                        () ->
                                JarHandlers.showPlan(
                                        handlers.planHandler, storedJarName, restfulGateway))
                .satisfies(
                        e -> {
                            assertThat(
                                            ExceptionUtils.findThrowable(
                                                    e, ProgramInvocationException.class))
                                    .map(Exception::getMessage)
                                    .hasValueSatisfying(
                                            message -> {
                                                assertThat(message)
                                                        // original cause is preserved in stack
                                                        // trace
                                                        .contains(
                                                                "The program plan could not be fetched - the program aborted pre-maturely")
                                                        // implies the jar was registered for the
                                                        // job graph
                                                        // (otherwise the jar name would
                                                        // not occur in the exception)
                                                        .contains(JAR_NAME)
                                                        // ensure that no stdout/stderr has been
                                                        // captured
                                                        .contains("System.out: " + "hello out!")
                                                        .contains("System.err: " + "hello err!");
                                            });
                        });
    }
}
