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

package org.apache.flink.runtime.webmonitor.utils;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_JOBMANAGER_LOG_URL_PATTERN;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_TASKMANAGER_LOG_URL_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogUrlUtil}. */
class LogUrlUtilTest {

    private static final String PATTERN_WITHOUT_SCHEME =
            "my.testing-url?job=<jobid>&taskmanager=<tmid>";

    @Test
    void testGetValidLogUrlPatternHttp() {
        String pattern = "http://" + PATTERN_WITHOUT_SCHEME;
        testGetValidLogUrlPattern(pattern, Optional.of(pattern));
    }

    @Test
    void testGetValidLogUrlPatternHttps() {
        String pattern = "https://" + PATTERN_WITHOUT_SCHEME;
        testGetValidLogUrlPattern(pattern, Optional.of(pattern));
    }

    @Test
    void testGetValidLogUrlPatternNoScheme() {
        testGetValidLogUrlPattern(
                PATTERN_WITHOUT_SCHEME, Optional.of("http://" + PATTERN_WITHOUT_SCHEME));
    }

    @Test
    void testGetValidLogUrlPatternUnsupportedScheme() {
        testGetValidLogUrlPattern("file://" + PATTERN_WITHOUT_SCHEME, Optional.empty());
    }

    @Test
    void testGetValidLogUrlPatternNotConfigured() {
        Configuration config = new Configuration();
        assertThat(
                        LogUrlUtil.getValidLogUrlPattern(
                                config, HISTORY_SERVER_JOBMANAGER_LOG_URL_PATTERN))
                .isNotPresent();
        assertThat(
                        LogUrlUtil.getValidLogUrlPattern(
                                config, HISTORY_SERVER_TASKMANAGER_LOG_URL_PATTERN))
                .isNotPresent();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void testGetValidLogUrlPattern(
            String configuredPattern, Optional<String> expectedPattern) {
        Configuration config = new Configuration();
        config.set(HISTORY_SERVER_JOBMANAGER_LOG_URL_PATTERN, configuredPattern);
        config.set(HISTORY_SERVER_TASKMANAGER_LOG_URL_PATTERN, configuredPattern);

        assertThat(
                        LogUrlUtil.getValidLogUrlPattern(
                                config, HISTORY_SERVER_JOBMANAGER_LOG_URL_PATTERN))
                .isEqualTo(expectedPattern);
        assertThat(
                        LogUrlUtil.getValidLogUrlPattern(
                                config, HISTORY_SERVER_TASKMANAGER_LOG_URL_PATTERN))
                .isEqualTo(expectedPattern);
    }
}
