/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Test;

import static org.apache.flink.configuration.ClusterOptions.ThreadDumpLogLevel;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CheckpointExpiredThreadDumper}. */
public class CheckpointExpiredThreadDumperTest {

    @Test
    public void testRepeatedlyAbortOneCheckpoint() {
        CheckpointExpiredThreadDumper dumper = new CheckpointExpiredThreadDumper();
        JobID id1 = new JobID(1L, 1L);
        JobID id2 = new JobID(2L, 2L);

        String original = setLoggerLevel(CheckpointExpiredThreadDumper.class.getName(), "INFO");

        // If set logger level valid, do the test.
        if (!"".equals(original)) {
            assertThat(dumper.threadDumpIfNeeded(id1, 1L, ThreadDumpLogLevel.DEBUG, 100)).isFalse();
            assertThat(dumper.threadDumpIfNeeded(id1, 2L, ThreadDumpLogLevel.INFO, 100)).isTrue();
            assertThat(dumper.threadDumpIfNeeded(id1, 2L, ThreadDumpLogLevel.INFO, 100)).isFalse();
            assertThat(dumper.threadDumpIfNeeded(id2, 2L, ThreadDumpLogLevel.INFO, 100)).isTrue();
            dumper.removeCheckpointExpiredThreadDumpRecordForJob(id1);
            assertThat(dumper.threadDumpIfNeeded(id1, 2L, ThreadDumpLogLevel.INFO, 100)).isTrue();
            setLoggerLevel(CheckpointExpiredThreadDumper.class.getName(), original);
        }
    }

    /**
     * Dynamically set the logger level to a specified logger name. Set to root if not exist.
     *
     * @param loggerName the logger name to set to.
     * @param newLogLevel the new logger level.
     * @return the original logger level name. Empty string if apply failed.
     */
    private String setLoggerLevel(String loggerName, String newLogLevel) {
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration config = context.getConfiguration();

        final LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);
        String original = "";

        if (loggerConfig != null) {
            original = loggerConfig.getLevel().name();
            loggerConfig.setLevel(org.apache.logging.log4j.Level.valueOf(newLogLevel));
            context.updateLoggers();
        }
        return original;
    }
}
