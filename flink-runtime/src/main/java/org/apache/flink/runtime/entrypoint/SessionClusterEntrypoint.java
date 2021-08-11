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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.FileExecutionGraphInfoStore;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava30.com.google.common.base.Ticker;

import java.io.File;
import java.io.IOException;

/** Base class for session cluster entry points. */
public abstract class SessionClusterEntrypoint extends ClusterEntrypoint {

    public SessionClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
            Configuration configuration, ScheduledExecutor scheduledExecutor) throws IOException {
        final File tmpDir = new File(ConfigurationUtils.parseTempDirectories(configuration)[0]);

        final Time expirationTime =
                Time.seconds(configuration.getLong(JobManagerOptions.JOB_STORE_EXPIRATION_TIME));
        final int maximumCapacity =
                configuration.getInteger(JobManagerOptions.JOB_STORE_MAX_CAPACITY);
        final long maximumCacheSizeBytes =
                configuration.getLong(JobManagerOptions.JOB_STORE_CACHE_SIZE);

        return new FileExecutionGraphInfoStore(
                tmpDir,
                expirationTime,
                maximumCapacity,
                maximumCacheSizeBytes,
                scheduledExecutor,
                Ticker.systemTicker());
    }
}
