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
package org.apache.beam.runners.fnexecution.environment;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the exit event logged by Flink's copy of Beam's process manager. */
@Isolated
@DisabledOnOs(OS.WINDOWS)
class ProcessManagerTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 23})
    void logsCompletedProcess(int exitCode) throws Exception {
        assertExitLogged("exit " + exitCode, false, exitCode);
    }

    @Test
    void logsForciblyKilledProcess() throws Exception {
        assertExitLogged("exec sleep 60", true, 137);
    }

    private void assertExitLogged(String command, boolean kill, int exitCode) throws Exception {
        final String workerId = "exit-log-worker";
        final CompletableFuture<LogEvent> exitEvent = new CompletableFuture<>();
        final Logger logger = (Logger) LogManager.getLogger(ProcessManager.class);
        final Level previousLevel = logger.getLevel();
        final AbstractAppender appender =
                new AbstractAppender("process-exit-test", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        if (event.getMessage()
                                .getFormattedMessage()
                                .startsWith("Process for worker " + workerId + " exited:")) {
                            exitEvent.complete(event.toImmutable());
                        }
                    }
                };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        final ProcessManager manager = ProcessManager.create();
        try {
            final Process process =
                    manager.startProcess(
                                    workerId,
                                    "/bin/sh",
                                    Arrays.asList("-c", command),
                                    Collections.emptyMap())
                            .getUnderlyingProcess();
            if (kill) {
                process.destroyForcibly();
            }
            final LogEvent event = exitEvent.get(5, TimeUnit.SECONDS);
            assertThat(event.getLevel()).isEqualTo(Level.INFO);
            assertThat(event.getMessage().getFormattedMessage())
                    .isEqualTo(
                            "Process for worker "
                                    + workerId
                                    + " exited: pid="
                                    + process.pid()
                                    + ", exitCode="
                                    + exitCode);
        } finally {
            try {
                manager.stopProcess(workerId);
            } finally {
                logger.removeAppender(appender);
                appender.stop();
                logger.setLevel(previousLevel);
            }
        }
    }
}
