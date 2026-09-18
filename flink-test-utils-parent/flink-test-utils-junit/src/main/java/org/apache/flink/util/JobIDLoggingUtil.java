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

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utility class for verifying the presence and correctness of a specific key-value pair in the
 * logging context of log events.
 */
public class JobIDLoggingUtil {
    private static final Logger logger = LoggerFactory.getLogger(JobIDLoggingUtil.class);

    /**
     * Asserts that the specified key is present in the log events with the expected values.
     *
     * @param key the key to look for
     * @param expectedValues the expected values of the key
     * @param ext the LoggerAuditingExtension instance
     * @param expectedPatterns the list of expected patterns
     * @param ignoredPatterns the array of ignore patterns
     */
    public static void assertKeyPresent(
            String key,
            Set<String> expectedValues,
            LoggerAuditingExtension ext,
            List<String> expectedPatterns,
            String... ignoredPatterns) {
        final List<LogEvent> eventsWithMissingKey = new ArrayList<>();
        final List<LogEvent> eventsWithWrongValue = new ArrayList<>();
        final List<LogEvent> ignoredEvents = new ArrayList<>();
        final List<Pattern> expected =
                expectedPatterns.stream().map(Pattern::compile).collect(toList());
        final List<Pattern> ignorePatterns =
                Arrays.stream(ignoredPatterns).map(Pattern::compile).collect(toList());

        for (LogEvent e : ext.getEvents()) {
            ReadOnlyStringMap context = e.getContextData();
            if (context.containsKey(key)) {
                if (expectedValues.contains(context.getValue(key))) {
                    expected.removeIf(
                            pattern ->
                                    pattern.matcher(e.getMessage().getFormattedMessage())
                                            .matches());
                } else {
                    eventsWithWrongValue.add(e);
                }
            } else if (matchesAny(ignorePatterns, e.getMessage().getFormattedMessage())) {
                ignoredEvents.add(e);
            } else {
                eventsWithMissingKey.add(e);
            }
        }

        logger.debug(
                "checked events for {}:\n  {};\n  ignored: {},\n  wrong value: {},\n missing key: {}",
                ext.getLoggerName(),
                ext.getEvents(),
                ignoredEvents,
                eventsWithWrongValue,
                eventsWithMissingKey);
        assertThat(eventsWithWrongValue).as("events with a wrong value").isEmpty();
        assertThat(expected)
                .as(
                        "not all expected events logged by %s, logged:\n%s",
                        ext.getLoggerName(), ext.getEvents())
                .isEmpty();
        assertThat(eventsWithMissingKey)
                .as("too many events without key logged by %s", ext.getLoggerName())
                .isEmpty();
    }

    /**
     * Asserts that the specified key is present in the log events with the expected value.
     *
     * @param key the key to look for
     * @param expectedValue the expected value of the key
     * @param ext the LoggerAuditingExtension instance
     * @param expectedPatterns the list of expected patterns
     * @param ignoredPatterns the array of ignore patterns
     */
    public static void assertKeyPresent(
            String key,
            String expectedValue,
            LoggerAuditingExtension ext,
            List<String> expectedPatterns,
            String... ignoredPatterns) {
        assertKeyPresent(
                key, Collections.singleton(expectedValue), ext, expectedPatterns, ignoredPatterns);
    }

    private static boolean matchesAny(List<Pattern> patterns, String message) {
        return patterns.stream().anyMatch(pattern -> pattern.matcher(message).matches());
    }
}
