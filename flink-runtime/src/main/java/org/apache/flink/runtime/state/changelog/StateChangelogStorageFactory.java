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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.LocalRecoveryConfig;

import java.io.IOException;

import static org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG;
import static org.apache.flink.configuration.StateChangelogOptions.MATERIALIZATION_MAX_FAILURES_ALLOWED;
import static org.apache.flink.configuration.StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL;
import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;

/**
 * A factory for {@link StateChangelogStorage}. Please use {@link StateChangelogStorageLoader} to
 * create {@link StateChangelogStorage}.
 */
@Internal
public interface StateChangelogStorageFactory {
    /** Get the identifier for user to use this changelog storage factory. */
    String getIdentifier();

    /** Create the storage based on a configuration. */
    StateChangelogStorage<?> createStorage(
            JobID jobID,
            Configuration configuration,
            TaskManagerJobMetricGroup metricGroup,
            LocalRecoveryConfig localRecoveryConfig)
            throws IOException;

    /** Create the storage for recovery. */
    StateChangelogStorageView<?> createStorageView(Configuration configuration) throws IOException;

    /** Extract the relevant to this factory portion of the configuration. */
    default Configuration extractConfiguration(ReadableConfig src) {
        Configuration dst = new Configuration();
        src.getOptional(PERIODIC_MATERIALIZATION_INTERVAL)
                .ifPresent(value -> dst.set(PERIODIC_MATERIALIZATION_INTERVAL, value));
        src.getOptional(MATERIALIZATION_MAX_FAILURES_ALLOWED)
                .ifPresent(value -> dst.set(MATERIALIZATION_MAX_FAILURES_ALLOWED, value));
        src.getOptional(ENABLE_STATE_CHANGE_LOG)
                .ifPresent(value -> dst.set(ENABLE_STATE_CHANGE_LOG, value));
        src.getOptional(STATE_CHANGE_LOG_STORAGE)
                .ifPresent(value -> dst.set(STATE_CHANGE_LOG_STORAGE, value));
        return dst;
    }
}
