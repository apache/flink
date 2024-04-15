/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.util.profiler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Create and keep profiling requests with rolling policy. */
public class ProfilingService implements Closeable {

    protected static final Logger LOG = LoggerFactory.getLogger(ProfilingService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
    private static volatile ProfilingService instance;
    private final Map<String, ArrayDeque<ProfilingInfo>> profilingMap;
    private final String profilingResultDir;
    private final int historySizeLimit;
    private final ScheduledExecutorService scheduledExecutor;
    private ProfilingFuture profilingFuture;

    private ProfilingService(Configuration configs) {
        this.profilingMap = new HashMap<>();
        this.historySizeLimit = configs.get(RestOptions.MAX_PROFILING_HISTORY_SIZE);
        Preconditions.checkArgument(
                historySizeLimit > 0,
                String.format(
                        "Configured %s must be positive.",
                        RestOptions.MAX_PROFILING_HISTORY_SIZE.key()));
        this.profilingResultDir = configs.get(RestOptions.PROFILING_RESULT_DIR);
        this.scheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory.Builder()
                                .setPoolName("flink-profiling-service")
                                .build());
    }

    public static ProfilingService getInstance(Configuration configs) {
        if (instance == null) {
            synchronized (ProfilingService.class) {
                if (instance == null) {
                    instance = new ProfilingService(configs);
                }
            }
        }
        return instance;
    }

    public CompletableFuture<ProfilingInfo> requestProfiling(
            String resourceID, long duration, ProfilingInfo.ProfilingMode mode) {
        if (profilingFuture != null && !profilingFuture.isDone()) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(resourceID + " is still under profiling."));
        }
        ProfilingInfo profilingInfo = ProfilingInfo.create(duration, mode);
        profilingMap.putIfAbsent(resourceID, new ArrayDeque<>());
        profilingMap.get(resourceID).addFirst(profilingInfo);
        AsyncProfiler profiler = AsyncProfiler.getInstance();
        try {
            String response =
                    profiler.execute(
                            ProfilerConstants.COMMAND_START.msg
                                    + profilingInfo.getProfilingMode().getCode());
            if (StringUtils.isNullOrWhitespaceOnly(response)
                    || !response.startsWith(ProfilerConstants.PROFILER_STARTED_SUCCESS.msg)) {
                return CompletableFuture.completedFuture(
                        profilingInfo.fail("Start profiler failed. " + response));
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                    profilingInfo.fail("Start profiler failed. " + e));
        }

        this.profilingFuture = new ProfilingFuture(duration, () -> stopProfiling(resourceID));
        return CompletableFuture.completedFuture(profilingInfo);
    }

    private void stopProfiling(String resourceID) {
        AsyncProfiler profiler = AsyncProfiler.getInstance();
        ArrayDeque<ProfilingInfo> profilingList = profilingMap.get(resourceID);
        Preconditions.checkState(!CollectionUtil.isNullOrEmpty(profilingList));
        ProfilingInfo info = profilingList.getFirst();
        try {
            String fileName = formatOutputFileName(resourceID, info);
            String outputPath = new File(profilingResultDir, fileName).getPath();
            String response = profiler.execute(ProfilerConstants.COMMAND_STOP.msg + outputPath);
            if (!StringUtils.isNullOrWhitespaceOnly(response)
                    && response.startsWith(ProfilerConstants.PROFILER_STOPPED_SUCCESS.msg)) {
                info.success(fileName);
            } else {
                info.fail("Stop profiler failed. " + response);
            }
            rollingClearing(profilingList);
        } catch (Throwable e) {
            info.fail("Stop profiler failed. " + e);
        }
    }

    private void rollingClearing(ArrayDeque<ProfilingInfo> profilingList) {
        while (profilingList.size() > historySizeLimit) {
            ProfilingInfo info = profilingList.pollLast();
            String outputFile = info != null ? info.getOutputFile() : "";
            if (StringUtils.isNullOrWhitespaceOnly(outputFile)) {
                continue;
            }
            try {
                Files.deleteIfExists(Paths.get(profilingResultDir, outputFile));
            } catch (Exception e) {
                LOG.error(String.format("Clearing file for %s failed. Skipped.", info), e);
            }
        }
    }

    private String formatOutputFileName(String resourceID, ProfilingInfo info) {
        return String.format(
                "%s_%s_%s.html", resourceID, info.getProfilingMode(), sdf.format(new Date()));
    }

    @Override
    public void close() throws IOException {
        try {
            if (profilingFuture != null && !profilingFuture.isDone()) {
                profilingFuture.cancel();
            }
            if (!scheduledExecutor.isShutdown()) {
                scheduledExecutor.shutdownNow();
            }
        } catch (Exception e) {
            LOG.error("Exception thrown during stopping profiling service. ", e);
        } finally {
            instance = null;
        }
    }

    public CompletableFuture<Collection<ProfilingInfo>> getProfilingList(String resourceID) {
        return CompletableFuture.completedFuture(
                profilingMap.getOrDefault(resourceID, new ArrayDeque<>()));
    }

    public String getProfilingResultDir() {
        return profilingResultDir;
    }

    @VisibleForTesting
    ArrayDeque<ProfilingInfo> getProfilingListForTest(String resourceID) {
        return profilingMap.getOrDefault(resourceID, new ArrayDeque<>());
    }

    @VisibleForTesting
    int getHistorySizeLimit() {
        return historySizeLimit;
    }

    @VisibleForTesting
    ProfilingFuture getProfilingFuture() {
        return profilingFuture;
    }

    enum ProfilerConstants {
        PROFILER_STARTED_SUCCESS("Profiling started"),
        PROFILER_STOPPED_SUCCESS("OK"),
        COMMAND_START("start,event="),
        COMMAND_STOP("stop,file=");

        private final String msg;

        ProfilerConstants(String msg) {
            this.msg = msg;
        }
    }

    /**
     * ProfilingFuture takes a handler and trigger duration, which will call the handler when
     * timeout or cancelled.
     */
    class ProfilingFuture {
        private final ScheduledFuture<?> future;
        private final Runnable handler;

        public ProfilingFuture(long duration, Runnable handler) {
            this.handler = handler;
            this.future = scheduledExecutor.schedule(handler, duration, TimeUnit.SECONDS);
        }

        /**
         * Verify the profiling request was done or not.
         *
         * @return true represents the request was finished.
         */
        public boolean isDone() {
            return this.future == null || this.future.isDone();
        }

        /**
         * Try to cancel the Profiling Request. Note that we'll trigger the handler once cancelled
         * successfully.
         *
         * @return true if cancelled, false for failed.
         */
        public boolean cancel() {
            if (isDone()) {
                return true;
            }
            if (!future.cancel(true)) {
                return false;
            }
            // If cancelled, we have to trigger handler immediately for stopping profiler.
            handler.run();
            return true;
        }
    }
}
