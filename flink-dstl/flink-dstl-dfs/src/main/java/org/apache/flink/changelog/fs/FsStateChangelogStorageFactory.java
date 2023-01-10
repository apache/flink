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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BASE_PATH;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.CACHE_IDLE_TIMEOUT;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.COMPRESSION_ENABLED;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.IN_FLIGHT_DATA_LIMIT;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.NUM_DISCARD_THREADS;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.NUM_UPLOAD_THREADS;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_DELAY;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_SIZE_THRESHOLD;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PREEMPTIVE_PERSIST_THRESHOLD;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.RETRY_DELAY_AFTER_FAILURE;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.RETRY_MAX_ATTEMPTS;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.RETRY_POLICY;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.UPLOAD_BUFFER_SIZE;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.UPLOAD_TIMEOUT;
import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;

/** {@link FsStateChangelogStorage} factory. */
@Internal
public class FsStateChangelogStorageFactory implements StateChangelogStorageFactory {

    public static final String IDENTIFIER = "filesystem";

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public StateChangelogStorage<?> createStorage(
            JobID jobID,
            Configuration configuration,
            TaskManagerJobMetricGroup metricGroup,
            LocalRecoveryConfig localRecoveryConfig)
            throws IOException {
        return new FsStateChangelogStorage(jobID, configuration, metricGroup, localRecoveryConfig);
    }

    @Override
    public StateChangelogStorageView<?> createStorageView(Configuration configuration) {
        return new FsStateChangelogStorageForRecovery(
                new ChangelogStreamHandleReaderWithCache(configuration));
    }

    @Override
    public Configuration extractConfiguration(ReadableConfig src) {
        Configuration dst = StateChangelogStorageFactory.super.extractConfiguration(src);
        src.getOptional(BASE_PATH).ifPresent(value -> dst.set(BASE_PATH, value));
        src.getOptional(COMPRESSION_ENABLED)
                .ifPresent(value -> dst.set(COMPRESSION_ENABLED, value));
        src.getOptional(PREEMPTIVE_PERSIST_THRESHOLD)
                .ifPresent(value -> dst.set(PREEMPTIVE_PERSIST_THRESHOLD, value));
        src.getOptional(PERSIST_DELAY).ifPresent(value -> dst.set(PERSIST_DELAY, value));
        src.getOptional(PERSIST_SIZE_THRESHOLD)
                .ifPresent(value -> dst.set(PERSIST_SIZE_THRESHOLD, value));
        src.getOptional(UPLOAD_BUFFER_SIZE).ifPresent(value -> dst.set(UPLOAD_BUFFER_SIZE, value));
        src.getOptional(NUM_UPLOAD_THREADS).ifPresent(value -> dst.set(NUM_UPLOAD_THREADS, value));
        src.getOptional(NUM_DISCARD_THREADS)
                .ifPresent(value -> dst.set(NUM_DISCARD_THREADS, value));
        src.getOptional(IN_FLIGHT_DATA_LIMIT)
                .ifPresent(value -> dst.set(IN_FLIGHT_DATA_LIMIT, value));
        src.getOptional(RETRY_POLICY).ifPresent(value -> dst.set(RETRY_POLICY, value));
        src.getOptional(UPLOAD_TIMEOUT).ifPresent(value -> dst.set(UPLOAD_TIMEOUT, value));
        src.getOptional(RETRY_MAX_ATTEMPTS).ifPresent(value -> dst.set(RETRY_MAX_ATTEMPTS, value));
        src.getOptional(RETRY_DELAY_AFTER_FAILURE)
                .ifPresent(value -> dst.set(RETRY_DELAY_AFTER_FAILURE, value));
        src.getOptional(CACHE_IDLE_TIMEOUT).ifPresent(value -> dst.set(CACHE_IDLE_TIMEOUT, value));
        return dst;
    }

    public static void configure(
            Configuration configuration,
            File newFolder,
            Duration uploadTimeout,
            int maxUploadAttempts) {
        configuration.setString(STATE_CHANGE_LOG_STORAGE, IDENTIFIER);
        configuration.setString(BASE_PATH, newFolder.getAbsolutePath());
        configuration.set(UPLOAD_TIMEOUT, uploadTimeout);
        configuration.set(RETRY_MAX_ATTEMPTS, maxUploadAttempts);
    }
}
