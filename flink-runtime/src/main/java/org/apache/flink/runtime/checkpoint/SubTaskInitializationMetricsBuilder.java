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

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;

/** A builder for {@link SubTaskInitializationMetrics}. */
@NotThreadSafe
public class SubTaskInitializationMetricsBuilder {
    private final long initializationStartTs;
    private final Map<String, Long> durationMetrics = new HashMap<>();
    private InitializationStatus status = InitializationStatus.FAILED;

    public SubTaskInitializationMetricsBuilder(long initializationStartTs) {
        this.initializationStartTs = initializationStartTs;
    }

    public long getInitializationStartTs() {
        return initializationStartTs;
    }

    /**
     * This adds a custom "duration" type metric, handled and aggregated by the {@link
     * JobInitializationMetricsBuilder}. If a metric with the given name already exists the old and
     * the new values will be added together.
     */
    public SubTaskInitializationMetricsBuilder addDurationMetric(String name, long value) {
        durationMetrics.compute(
                name, (key, oldValue) -> oldValue == null ? value : value + oldValue);
        return this;
    }

    public SubTaskInitializationMetricsBuilder setStatus(InitializationStatus status) {
        this.status = status;
        return this;
    }

    public SubTaskInitializationMetrics build() {
        return build(System.currentTimeMillis());
    }

    @VisibleForTesting
    public SubTaskInitializationMetrics build(long endTs) {
        return new SubTaskInitializationMetrics(
                initializationStartTs, endTs, durationMetrics, status);
    }
}
