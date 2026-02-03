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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.WebOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RestHandlerConfiguration}. */
class RestHandlerConfigurationTest {

    @Test
    void testWebSubmitFeatureFlagEnabled() {
        testWebSubmitFeatureFlag(true);
    }

    @Test
    void testWebSubmitFeatureFlagDisabled() {
        testWebSubmitFeatureFlag(false);
    }

    @Test
    void testWebCancelFeatureFlagEnabled() {
        testWebCancelFeatureFlag(true);
    }

    @Test
    void testWebCancelFeatureFlagDisabled() {
        testWebCancelFeatureFlag(false);
    }

    @ParameterizedTest
    @CsvSource({
        "true,true,true,false",
        "true,true,false,true",
        "true,false,true,false",
        "true,false,false,false",
        "false,true,true,false",
        "false,true,false,false",
        "false,false,true,false",
        "false,false,false,false",
    })
    void testWebRescaleFeatureFlagWithReactiveMode(
            boolean webRescaleEnabled,
            boolean adaptiveScheduler,
            boolean reactiveMode,
            boolean expectedResult) {
        final Configuration config = new Configuration();
        config.set(WebOptions.RESCALE_ENABLE, webRescaleEnabled);
        if (adaptiveScheduler) {
            config.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        } else {
            config.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default);
        }
        if (reactiveMode) {
            config.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        }
        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);
        assertThat(restHandlerConfiguration.isWebRescaleEnabled()).isEqualTo(expectedResult);
    }

    private static void testWebSubmitFeatureFlag(boolean webSubmitEnabled) {
        final Configuration config = new Configuration();
        config.set(WebOptions.SUBMIT_ENABLE, webSubmitEnabled);

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);
        assertThat(restHandlerConfiguration.isWebSubmitEnabled()).isEqualTo(webSubmitEnabled);
    }

    private static void testWebCancelFeatureFlag(boolean webCancelEnabled) {
        final Configuration config = new Configuration();
        config.set(WebOptions.CANCEL_ENABLE, webCancelEnabled);

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);
        assertThat(restHandlerConfiguration.isWebCancelEnabled()).isEqualTo(webCancelEnabled);
    }

    @Test
    void testCheckpointCacheExpireAfterWrite() {
        final Configuration config = new Configuration();
        final Duration defaultDuration = Duration.ofMillis(3000L);
        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);
        assertThat(restHandlerConfiguration.getCheckpointCacheExpireAfterWrite())
                .isEqualTo(defaultDuration);

        final Duration testDuration = Duration.ofMillis(100L);
        config.set(RestOptions.CACHE_CHECKPOINT_STATISTICS_TIMEOUT, testDuration);
        restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(config);
        assertThat(restHandlerConfiguration.getCheckpointCacheExpireAfterWrite())
                .isEqualTo(testDuration);
    }

    @Test
    void testCheckpointCacheExpiryFallbackToRefreshInterval() {
        final long refreshInterval = 1000L;
        final Configuration config = new Configuration();
        config.set(WebOptions.REFRESH_INTERVAL, Duration.ofMillis(refreshInterval));

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);
        assertThat(restHandlerConfiguration.getCheckpointCacheExpireAfterWrite())
                .isEqualTo(Duration.ofMillis(1000L));
    }

    @Test
    void testCheckpointCacheSize() {
        final int testCacheSize = 50;
        final Configuration config = new Configuration();
        config.set(RestOptions.CACHE_CHECKPOINT_STATISTICS_SIZE, testCacheSize);

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);
        assertThat(restHandlerConfiguration.getCheckpointCacheSize()).isEqualTo(testCacheSize);
    }

    /** Tests that ExecutionGraph cache TTL defaults to refresh interval when not set. */
    @Test
    void testExecutionGraphCacheTTLDefault() {
        final Duration refreshInterval = Duration.ofMillis(5000L);
        final Configuration config = new Configuration();
        config.set(WebOptions.REFRESH_INTERVAL, refreshInterval);

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);

        // When EXECUTION_GRAPH_CACHE_TTL is not set, it should default to REFRESH_INTERVAL
        assertThat(restHandlerConfiguration.getExecutionGraphCacheTTL()).isEqualTo(refreshInterval);
    }

    /** Tests that ExecutionGraph cache TTL can be configured independently. */
    @Test
    void testExecutionGraphCacheTTLCustomValue() {
        final Duration refreshInterval = Duration.ofMillis(5000L);
        final Duration cacheTTL = Duration.ofMillis(10000L);
        final Configuration config = new Configuration();
        config.set(WebOptions.REFRESH_INTERVAL, refreshInterval);
        config.set(WebOptions.EXECUTION_GRAPH_CACHE_TTL, cacheTTL);

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);

        // EXECUTION_GRAPH_CACHE_TTL should be independent of REFRESH_INTERVAL
        assertThat(restHandlerConfiguration.getRefreshInterval())
                .isEqualTo(refreshInterval.toMillis());
        assertThat(restHandlerConfiguration.getExecutionGraphCacheTTL()).isEqualTo(cacheTTL);
    }

    /**
     * Tests that ExecutionGraph cache TTL can be set to zero for real-time state synchronization.
     */
    @Test
    void testExecutionGraphCacheTTLZeroValue() {
        final Duration zeroCacheTTL = Duration.ZERO;
        final Configuration config = new Configuration();
        config.set(WebOptions.EXECUTION_GRAPH_CACHE_TTL, zeroCacheTTL);

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);

        // Setting to zero should disable caching
        assertThat(restHandlerConfiguration.getExecutionGraphCacheTTL()).isEqualTo(zeroCacheTTL);
    }

    /** Tests that negative ExecutionGraph cache TTL throws IllegalArgumentException. */
    @Test
    void testExecutionGraphCacheTTLNegativeValue() {
        final Duration negativeCacheTTL = Duration.ofMillis(-1000L);
        final Configuration config = new Configuration();
        config.set(WebOptions.EXECUTION_GRAPH_CACHE_TTL, negativeCacheTTL);

        assertThatThrownBy(() -> RestHandlerConfiguration.fromConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ExecutionGraph cache TTL should not be negative");
    }
}
