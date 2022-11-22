/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

/** Metrics related to the materialization part of Changelog. */
@Internal
public class ChangelogMaterializationMetricGroup extends ProxyMetricGroup<MetricGroup> {

    private static final String PREFIX = "ChangelogMaterialization";

    @VisibleForTesting
    public static final String STARTED_MATERIALIZATION = PREFIX + ".startedMaterialization";

    @VisibleForTesting
    public static final String COMPLETED_MATERIALIZATION = PREFIX + ".completedMaterialization";

    @VisibleForTesting
    public static final String FAILED_MATERIALIZATION = PREFIX + ".failedMaterialization";

    @VisibleForTesting
    public static final String LAST_DURATION_OF_MATERIALIZATION =
            PREFIX + ".lastDurationOfMaterialization";

    private final Counter startedMaterializationCounter;
    private final Counter completedMaterializationCounter;
    private final Counter failedMaterializationCounter;

    private volatile long lastDuration = -1;

    public ChangelogMaterializationMetricGroup(MetricGroup parentMetricGroup) {
        super(parentMetricGroup);
        this.startedMaterializationCounter =
                counter(STARTED_MATERIALIZATION, new ThreadSafeSimpleCounter());
        this.completedMaterializationCounter =
                counter(COMPLETED_MATERIALIZATION, new ThreadSafeSimpleCounter());
        this.failedMaterializationCounter =
                counter(FAILED_MATERIALIZATION, new ThreadSafeSimpleCounter());

        gauge(LAST_DURATION_OF_MATERIALIZATION, () -> lastDuration);
    }

    void reportStartedMaterialization() {
        startedMaterializationCounter.inc();
    }

    void reportCompletedMaterialization(long duration) {
        completedMaterializationCounter.inc();
        lastDuration = duration;
    }

    void reportFailedMaterialization() {
        failedMaterializationCounter.inc();
    }
}
