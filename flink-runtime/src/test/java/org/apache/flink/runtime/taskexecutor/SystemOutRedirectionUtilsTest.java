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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions.SystemOutMode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Queue;

import static org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_SYSTEM_OUT_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SystemOutRedirectionUtils} */
class SystemOutRedirectionUtilsTest {

    private PrintStream originalOut;
    private PrintStream originalErr;

    private Queue<String> outCollector;
    private Queue<String> errCollector;

    @BeforeEach
    void beforeEach() {
        originalOut = System.out;
        originalErr = System.err;

        outCollector = new LinkedList<>();
        errCollector = new LinkedList<>();
    }

    @AfterEach
    void afterEach() {
        // Recover the original out and err.
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    void testDefaultSystemOutAndErr() {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outStream));
        System.setErr(new PrintStream(errStream));
        SystemOutRedirectionUtils.redirectSystemOutAndError(new Configuration());

        String logContext = "This is log context!";
        System.out.print(logContext);
        assertThat(outStream.toString()).isEqualTo(logContext);

        System.err.print(logContext);
        assertThat(errStream.toString()).isEqualTo(logContext);
    }

    @ParameterizedTest
    @EnumSource(
            value = SystemOutMode.class,
            names = {"IGNORE", "LOG"})
    void testSystemOutAndErrAreRedirected(SystemOutMode systemOutMode) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outStream));
        System.setErr(new PrintStream(errStream));

        Configuration conf = new Configuration();
        conf.set(TASK_MANAGER_SYSTEM_OUT_MODE, systemOutMode);
        SystemOutRedirectionUtils.redirectSystemOutAndError(conf);

        String logContext = "This is log context!";
        System.out.print(logContext);
        assertThat(outStream.toByteArray()).isEmpty();

        System.err.print(logContext);
        assertThat(errStream.toByteArray()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLogThreadName(boolean logThreadName) {
        SystemOutRedirectionUtils.redirectToLoggingRedirector(
                outCollector::add, errCollector::add, 100000, logThreadName);

        // Test for the whole context.
        String logContext = "This is" + System.lineSeparator() + " log context!";
        System.out.println(logContext);
        System.err.println(logContext);
        assertThat(outCollector.poll())
                .isEqualTo(generateExpectedContext(logContext, logThreadName));
        assertThat(errCollector.poll())
                .isEqualTo(generateExpectedContext(logContext, logThreadName));

        // Test for print with println
        String part1 = "Log " + System.lineSeparator() + "context part 1.";
        String part2 = "Log context part 2.";
        System.out.print(part1);
        System.err.print(part1);
        assertThat(outCollector).isEmpty();
        assertThat(errCollector).isEmpty();

        System.out.println(part2);
        System.err.println(part2);
        assertThat(outCollector.poll())
                .isEqualTo(generateExpectedContext(part1 + part2, logThreadName));
        assertThat(errCollector.poll())
                .isEqualTo(generateExpectedContext(part1 + part2, logThreadName));

        // Test for print with print(System.lineSeparator())
        System.out.print(part1);
        System.err.print(part1);
        assertThat(outCollector).isEmpty();
        assertThat(errCollector).isEmpty();

        System.out.print(System.lineSeparator());
        System.err.print(System.lineSeparator());
        assertThat(outCollector.poll()).isEqualTo(generateExpectedContext(part1, logThreadName));
        assertThat(errCollector.poll()).isEqualTo(generateExpectedContext(part1, logThreadName));
    }

    private String generateExpectedContext(String originalLogContext, boolean logThreadName) {
        if (!logThreadName) {
            return originalLogContext;
        }
        return String.format(
                "Thread Name: %s , log context: %s",
                Thread.currentThread().getName(), originalLogContext);
    }

    @Test
    void testByteLimitEachLine() {
        int byteLimitEachLine = 100;
        SystemOutRedirectionUtils.redirectToLoggingRedirector(
                outCollector::add, errCollector::add, byteLimitEachLine, false);

        StringBuilder expectedContext = new StringBuilder();
        for (int i = 0; i < byteLimitEachLine; i++) {
            assertThat(outCollector).isEmpty();
            assertThat(errCollector).isEmpty();
            System.out.print('a');
            System.err.print('a');
            expectedContext.append('a');
        }
        assertThat(outCollector.poll()).isEqualTo(expectedContext.toString());
    }
}
