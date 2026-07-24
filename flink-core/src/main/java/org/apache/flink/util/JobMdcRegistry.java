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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-wide registry mapping {@link JobID} to enriched MDC context, populated where the job
 * {@link Configuration} is available and consulted by {@link MdcUtils#asContextData(JobID)}.
 */
@Internal
@ThreadSafe
public final class JobMdcRegistry {

    private static final Map<JobID, Map<String, String>> REGISTRY = new ConcurrentHashMap<>();

    private JobMdcRegistry() {}

    /**
     * Registers enriched MDC context if the configuration carries any MDC key mappings; clears any
     * stale entry otherwise. Equivalent to {@link #unregister} when the config is unenriched.
     */
    public static void registerOrClear(final JobID jobID, final Configuration jobConfiguration) {
        final Map<String, String> context = MdcUtils.asContextData(jobID, jobConfiguration);
        if (context.size() > 1) {
            REGISTRY.put(jobID, context);
        } else {
            unregister(jobID);
        }
    }

    /** Remove the registered context for the job. */
    public static void unregister(final JobID jobID) {
        REGISTRY.remove(jobID);
    }

    /** Return the registered context for the job, or {@code null} if none. */
    @Nullable
    public static Map<String, String> lookup(final JobID jobID) {
        return REGISTRY.get(jobID);
    }

    @VisibleForTesting
    public static void clear() {
        REGISTRY.clear();
    }
}
