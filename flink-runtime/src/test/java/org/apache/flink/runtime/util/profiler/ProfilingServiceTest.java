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

package org.apache.flink.runtime.util.profiler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Unit tests for {@link ProfilingService}. */
public class ProfilingServiceTest extends TestLogger {
    private static final String NO_ACCESS_TO_PERF_EVENTS = "No access to perf events.";
    private static final String NO_ALLOC_SYMBOL_FOUND = "No AllocTracer symbols found.";
    private static final String RESOURCE_ID = "TestJobManager";
    private static final long DEFAULT_PROFILING_DURATION = 3L;
    private static final int HISTORY_SIZE_LIMIT = 2;

    private ProfilingService profilingService;
    private final Configuration configs = new Configuration();

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        configs.set(RestOptions.MAX_PROFILING_HISTORY_SIZE, HISTORY_SIZE_LIMIT);
        configs.set(RestOptions.PROFILING_RESULT_DIR, tempDir.toString());
        profilingService = ProfilingService.getInstance(configs);
    }

    @AfterEach
    void tearDown() throws IOException {
        profilingService.close();
    }

    @Test
    public void testSingleton() throws IOException {
        try (ProfilingService testService = ProfilingService.getInstance(configs)) {
            Assertions.assertEquals(profilingService, testService);
        }
    }

    @Test
    void testProfilingConfigurationWorkingAsExpected() throws IOException {
        try (ProfilingService testService = ProfilingService.getInstance(configs)) {
            Assertions.assertEquals(
                    configs.get(RestOptions.PROFILING_RESULT_DIR),
                    testService.getProfilingResultDir());
            Assertions.assertEquals(
                    configs.get(RestOptions.MAX_PROFILING_HISTORY_SIZE),
                    testService.getHistorySizeLimit());
        }
    }

    @Test
    void testFailedRequestSinceStillUnderProfiling()
            throws ExecutionException, InterruptedException {
        requestSingleProfiling(ProfilingInfo.ProfilingMode.ITIMER, 10L, false);
        try {
            // request for another profiling right now, it should fail.
            requestSingleProfiling(ProfilingInfo.ProfilingMode.ITIMER, 10L, false);
            Assertions.fail("Duplicate profiling request should throw with IllegalStateException.");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testAllProfilingMode() throws ExecutionException, InterruptedException {
        for (ProfilingInfo.ProfilingMode mode : ProfilingInfo.ProfilingMode.values()) {
            requestSingleProfiling(mode, DEFAULT_PROFILING_DURATION, true);
        }
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testRollingDeletion() throws ExecutionException, InterruptedException {
        // trigger 3 times ITIMER mode profiling
        for (int i = 0; i < 3; i++) {
            requestSingleProfiling(
                    ProfilingInfo.ProfilingMode.ITIMER, DEFAULT_PROFILING_DURATION, true);
        }
        // Due to the configuration of MAX_PROFILING_HISTORY_SIZE=2,
        // the profiling result directory shouldn't contain more than 2 files.
        verifyRollingDeletionWorks();
    }

    /**
     * Trigger a specific profiling instance for testing.
     *
     * @param mode target profiling mode
     * @param duration target profiling duration
     * @param waitUntilFinished flag of waiting for profiling finished or not
     */
    private void requestSingleProfiling(
            ProfilingInfo.ProfilingMode mode, Long duration, Boolean waitUntilFinished)
            throws InterruptedException, ExecutionException {
        ProfilingInfo profilingInfo =
                profilingService.requestProfiling(RESOURCE_ID, duration, mode).get();
        if (isNoPermissionOrAllocateSymbol(profilingInfo)) {
            log.warn(
                    "Ignoring failed profiling instance in {} mode, which caused by {}.",
                    profilingInfo.getProfilingMode(),
                    profilingInfo.getMessage());
            return;
        }
        Assertions.assertEquals(
                ProfilingInfo.ProfilingStatus.RUNNING,
                profilingInfo.getStatus(),
                String.format(
                        "Submitting profiling request should be started successfully or failed by no permission, but got errorMsg=%s",
                        profilingInfo.getMessage()));
        if (waitUntilFinished) {
            waitForProfilingFinished();
            Assertions.assertEquals(
                    ProfilingInfo.ProfilingStatus.FINISHED,
                    profilingInfo.getStatus(),
                    String.format(
                            "Profiling request should complete successful, but got errorMsg=%s",
                            profilingInfo.getMessage()));
        }
    }

    private void verifyRollingDeletionWorks() {
        ArrayDeque<ProfilingInfo> profilingList =
                profilingService.getProfilingListForTest(RESOURCE_ID);
        // Profiling History shouldn't exceed history size limit.
        Assertions.assertTrue(profilingList.size() <= profilingService.getHistorySizeLimit());
        // Profiling History files should be rolling deleted.
        Set<String> resultFileNames = new HashSet<>();
        File configuredDir = new File(profilingService.getProfilingResultDir());
        for (File f : Objects.requireNonNull(configuredDir.listFiles())) {
            if (f.getName().startsWith(RESOURCE_ID)) {
                resultFileNames.add(f.getName());
            }
        }
        if (profilingList.size() != resultFileNames.size()) {
            log.error(
                    "Found unexpected profiling file size: profilingList={},resultFileNames={}",
                    profilingList,
                    resultFileNames);
        }
        Assertions.assertEquals(profilingList.size(), resultFileNames.size());
        for (ProfilingInfo profilingInfo : profilingList) {
            String outputFile = profilingInfo.getOutputFile();
            Assertions.assertTrue(resultFileNames.contains(outputFile));
        }
    }

    private void waitForProfilingFinished() throws InterruptedException {
        while (!profilingService.getProfilingFuture().isDone()) {
            Thread.sleep(1000);
        }
    }

    /**
     * Check profiling instance failed caused by no permission to perf_events or missing of JDK
     * debug symbols.
     *
     * @return true if no permission to access perf_events or no AllocTracer symbols found.
     */
    private boolean isNoPermissionOrAllocateSymbol(ProfilingInfo profilingInfo) {
        boolean isNoPermission =
                profilingInfo.getStatus() == ProfilingInfo.ProfilingStatus.FAILED
                        && !StringUtils.isNullOrWhitespaceOnly(profilingInfo.getMessage())
                        && profilingInfo.getMessage().contains(NO_ACCESS_TO_PERF_EVENTS);
        boolean isNoAllocateSymbol =
                profilingInfo.getStatus() == ProfilingInfo.ProfilingStatus.FAILED
                        && !StringUtils.isNullOrWhitespaceOnly(profilingInfo.getMessage())
                        && profilingInfo.getMessage().contains(NO_ALLOC_SYMBOL_FOUND);
        return isNoPermission || isNoAllocateSymbol;
    }
}
