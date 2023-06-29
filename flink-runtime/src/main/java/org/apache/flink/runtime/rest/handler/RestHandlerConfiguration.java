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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.time.Duration;

/** Configuration object containing values for the rest handler configuration. */
public class RestHandlerConfiguration {

    private final long refreshInterval;

    private final int checkpointHistorySize;

    private final Duration checkpointCacheExpireAfterWrite;

    private final int checkpointCacheSize;

    private final Time timeout;

    private final File webUiDir;

    private final boolean webSubmitEnabled;

    private final boolean webCancelEnabled;

    private final boolean webRescaleEnabled;

    public RestHandlerConfiguration(
            long refreshInterval,
            int checkpointHistorySize,
            Duration checkpointCacheExpireAfterWrite,
            int checkpointCacheSize,
            Time timeout,
            File webUiDir,
            boolean webSubmitEnabled,
            boolean webCancelEnabled,
            boolean webRescaleEnabled) {
        Preconditions.checkArgument(
                refreshInterval > 0L, "The refresh interval (ms) should be larger than 0.");
        this.refreshInterval = refreshInterval;

        this.checkpointHistorySize = checkpointHistorySize;
        this.checkpointCacheExpireAfterWrite = checkpointCacheExpireAfterWrite;
        this.checkpointCacheSize = checkpointCacheSize;

        this.timeout = Preconditions.checkNotNull(timeout);
        this.webUiDir = Preconditions.checkNotNull(webUiDir);
        this.webSubmitEnabled = webSubmitEnabled;
        this.webCancelEnabled = webCancelEnabled;
        this.webRescaleEnabled = webRescaleEnabled;
    }

    public long getRefreshInterval() {
        return refreshInterval;
    }

    public int getCheckpointHistorySize() {
        return checkpointHistorySize;
    }

    public Duration getCheckpointCacheExpireAfterWrite() {
        return checkpointCacheExpireAfterWrite;
    }

    public int getCheckpointCacheSize() {
        return checkpointCacheSize;
    }

    public Time getTimeout() {
        return timeout;
    }

    public File getWebUiDir() {
        return webUiDir;
    }

    public boolean isWebSubmitEnabled() {
        return webSubmitEnabled;
    }

    public boolean isWebCancelEnabled() {
        return webCancelEnabled;
    }

    public boolean isWebRescaleEnabled() {
        return webRescaleEnabled;
    }

    public static RestHandlerConfiguration fromConfiguration(Configuration configuration) {
        final long refreshInterval = configuration.getLong(WebOptions.REFRESH_INTERVAL);

        final int checkpointHistorySize =
                configuration.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);
        final Duration checkpointStatsSnapshotCacheExpireAfterWrite =
                configuration
                        .getOptional(RestOptions.CACHE_CHECKPOINT_STATISTICS_TIMEOUT)
                        .orElse(Duration.ofMillis(refreshInterval));
        final int checkpointStatsSnapshotCacheSize =
                configuration.get(RestOptions.CACHE_CHECKPOINT_STATISTICS_SIZE);

        final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

        final String rootDir = "flink-web-ui";
        final File webUiDir = new File(configuration.getString(WebOptions.TMP_DIR), rootDir);

        final boolean webSubmitEnabled = configuration.getBoolean(WebOptions.SUBMIT_ENABLE);
        final boolean webCancelEnabled = configuration.getBoolean(WebOptions.CANCEL_ENABLE);
        final boolean webRescaleSupported =
                ClusterOptions.isAdaptiveSchedulerEnabled(configuration)
                        && !ClusterOptions.isReactiveModeEnabled(configuration);
        final boolean webRescaleEnabled =
                webRescaleSupported && configuration.getBoolean(WebOptions.RESCALE_ENABLE);

        return new RestHandlerConfiguration(
                refreshInterval,
                checkpointHistorySize,
                checkpointStatsSnapshotCacheExpireAfterWrite,
                checkpointStatsSnapshotCacheSize,
                timeout,
                webUiDir,
                webSubmitEnabled,
                webCancelEnabled,
                webRescaleEnabled);
    }
}
