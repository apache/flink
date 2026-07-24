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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MdcOptions;

import org.slf4j.MDC;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Utility class to manage common Flink attributes in {@link MDC}. */
public class MdcUtils {

    public static final String JOB_ID = "flink-job-id";

    /**
     * Replace MDC contents with the provided one and return a closeable object that can be used to
     * restore the original MDC.
     *
     * @param context to put into MDC
     */
    public static MdcCloseable withContext(Map<String, String> context) {
        final Map<String, String> orig = MDC.getCopyOfContextMap();
        MDC.setContextMap(context);
        return () -> {
            if (orig != null) {
                MDC.setContextMap(orig);
            } else {
                MDC.clear();
            }
        };
    }

    /** {@link AutoCloseable } that restores the {@link MDC} contents on close. */
    public interface MdcCloseable extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Wrap the given {@link Runnable} so that the given data is added to {@link MDC} before its
     * execution and removed afterward.
     */
    public static Runnable wrapRunnable(Map<String, String> contextData, Runnable command) {
        return () -> {
            try (MdcCloseable ctx = withContext(contextData)) {
                command.run();
            }
        };
    }

    /**
     * Wrap the given {@link Callable} so that the given data is added to {@link MDC} before its
     * execution and removed afterward.
     */
    public static <T> Callable<T> wrapCallable(
            Map<String, String> contextData, Callable<T> command) {
        return () -> {
            try (MdcCloseable ctx = withContext(contextData)) {
                return command.call();
            }
        };
    }

    /**
     * Wrap the given {@link Executor} so that the given {@link JobID} is added before it executes
     * any submitted commands and removed afterward.
     */
    public static Executor scopeToJob(JobID jobID, Executor executor) {
        checkArgument(!(executor instanceof MdcAwareExecutor));
        return new MdcAwareExecutor<>(executor, asContextData(jobID));
    }

    /**
     * Wrap the given {@link ExecutorService} so that the given {@link JobID} is added before it
     * executes any submitted commands and removed afterward.
     */
    public static ExecutorService scopeToJob(JobID jobID, ExecutorService delegate) {
        checkArgument(!(delegate instanceof MdcAwareExecutorService));
        return new MdcAwareExecutorService<>(delegate, asContextData(jobID));
    }

    /** Wraps the given {@link ExecutorService} into one with a copy of the current context. */
    public static ExecutorService propagate(ExecutorService delegate) {
        checkArgument(!(delegate instanceof MdcAwareExecutorService));
        final Map<String, String> context = MDC.getCopyOfContextMap();
        return new MdcAwareExecutorService<>(
                delegate, context != null ? context : Collections.emptyMap());
    }

    /**
     * Wrap the given {@link ScheduledExecutorService} so that the given {@link JobID} is added
     * before it executes any submitted commands and removed afterward.
     */
    public static ScheduledExecutorService scopeToJob(JobID jobID, ScheduledExecutorService ses) {
        checkArgument(!(ses instanceof MdcAwareScheduledExecutorService));
        return new MdcAwareScheduledExecutorService(ses, asContextData(jobID));
    }

    /**
     * Build MDC context for a job. Consults the {@link JobMdcRegistry} for enriched context
     * registered where the job {@link Configuration} is available; falls back to the plain job ID
     * entry.
     */
    public static Map<String, String> asContextData(JobID jobID) {
        final Map<String, String> registered = JobMdcRegistry.lookup(jobID);
        if (registered != null) {
            return registered;
        }
        return Collections.singletonMap(JOB_ID, jobID.toHexString());
    }

    /**
     * Build MDC context from a job ID and job configuration, enriching with context entries
     * configured via {@link MdcOptions#JOB_CONFIGURATION_TO_MDC_KEYS}.
     */
    public static Map<String, String> asContextData(
            final JobID jobID, final Configuration jobConfiguration) {
        final Map<String, String> mdcKeyMapping =
                jobConfiguration.get(MdcOptions.JOB_CONFIGURATION_TO_MDC_KEYS);
        final Map<String, String> context = new HashMap<>();
        for (Map.Entry<String, String> entry : mdcKeyMapping.entrySet()) {
            final String value = jobConfiguration.getString(entry.getKey(), null);
            if (value != null && !value.isBlank()) {
                context.put(entry.getValue(), value);
            }
        }
        if (context.isEmpty()) {
            return Collections.singletonMap(JOB_ID, jobID.toHexString());
        }
        context.put(JOB_ID, jobID.toHexString());
        return Collections.unmodifiableMap(context);
    }
}
