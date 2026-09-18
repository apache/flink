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

package org.apache.flink.util;

import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.event.Level;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JobIDLoggingUtil}. */
class JobIDLoggingUtilTest {
    private static final Logger LOG = LoggerFactory.getLogger(JobIDLoggingUtilTest.class);
    private static final String KEY = "flink-job-id";

    @RegisterExtension
    final LoggerAuditingExtension logging =
            new LoggerAuditingExtension(JobIDLoggingUtilTest.class, Level.DEBUG);

    @Test
    void ignorePatternsSkipEventsWithADifferentKeyValue() {
        logWithMdc("job-a", "Received task test-task.");
        logWithMdc("job-b", "Freeing inactive slots for job job-b.");

        JobIDLoggingUtil.assertKeyPresent(
                KEY,
                "job-a",
                logging,
                Collections.singletonList("Received task .*"),
                "Freeing inactive slots.*");
    }

    @Test
    void eventsWithADifferentKeyValueFailWhenNotIgnored() {
        logWithMdc("job-a", "Received task test-task.");
        logWithMdc("job-b", "Freeing inactive slots for job job-b.");

        assertThatThrownBy(
                        () ->
                                JobIDLoggingUtil.assertKeyPresent(
                                        KEY,
                                        "job-a",
                                        logging,
                                        Collections.singletonList("Received task .*")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("events with a wrong value");
    }

    @Test
    void ignorePatternsStillSkipEventsWithAMissingKey() {
        logWithMdc("job-a", "Received task test-task.");
        LOG.debug("Successful registration at resource manager.");

        JobIDLoggingUtil.assertKeyPresent(
                KEY,
                "job-a",
                logging,
                Collections.singletonList("Received task .*"),
                "Successful registration.*");
    }

    private static void logWithMdc(String jobId, String message) {
        MDC.put(KEY, jobId);
        try {
            LOG.debug(message);
        } finally {
            MDC.remove(KEY);
        }
    }
}
