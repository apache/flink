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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

import javax.annotation.Nonnull;

/** A metric group for {@link CheckpointMetrics} related to the async phase. */
public class AsyncCheckpointMetricsGroup extends ProxyMetricGroup<MetricGroup> {

    private volatile CheckpointMetrics checkpointMetrics;

    public AsyncCheckpointMetricsGroup(MetricGroup parent) {
        super(parent);
        this.checkpointMetrics = new CheckpointMetrics();
        registerMetrics();
    }

    @VisibleForTesting
    public static final String BYTES_PERSISTED_DURING_ALIGNMENT =
            "checkpointBytesPersistedDuringAlignment";

    @VisibleForTesting
    public static final String ASYNC_DURATION_MILLIS = "checkpointAsyncDurationMillis";

    @VisibleForTesting
    public static final String BYTES_PERSISTED_OF_THIS_CHECKPOINT =
            "checkpointBytesPersistedOfThisCheckpoint";

    @VisibleForTesting
    public static final String TOTAL_BYTES_PERSISTED = "checkpointTotalBytesPersisted";

    /**
     * Report newest CheckpointMetrics to metric group.
     *
     * @param checkpointMetrics newest checkpoint metrics.
     */
    public void reportCheckpointMetrics(@Nonnull CheckpointMetrics checkpointMetrics) {
        this.checkpointMetrics = checkpointMetrics;
    }

    /** Register all exposed metrics. */
    private void registerMetrics() {
        gauge(
                BYTES_PERSISTED_DURING_ALIGNMENT,
                () -> checkpointMetrics.getBytesPersistedDuringAlignment());
        gauge(ASYNC_DURATION_MILLIS, () -> checkpointMetrics.getAsyncDurationMillis());
        gauge(
                BYTES_PERSISTED_OF_THIS_CHECKPOINT,
                () -> checkpointMetrics.getBytesPersistedOfThisCheckpoint());
        gauge(TOTAL_BYTES_PERSISTED, () -> checkpointMetrics.getTotalBytesPersisted());
    }
}
