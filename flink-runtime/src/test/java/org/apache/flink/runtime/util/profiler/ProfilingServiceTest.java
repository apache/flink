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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ProfilingServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ProfilingServiceTest.class);
    private static final Configuration configs = new Configuration();
    private static final String NO_ACCESS_TO_PERF_EVENTS = "No access to perf events.";
    private static final String NO_ALLOC_SYMBOL_FOUND = "No AllocTracer symbols found.";
    private static final String resourceID = "TestJobManager";
    private static final long profilingDuration = 3L;
    private static final int historySizeLimit = 3;

    private ProfilingService profilingService;

    @BeforeAll
    static void beforeAll() {
        configs.set(RestOptions.MAX_PROFILING_HISTORY_SIZE, historySizeLimit);
    }

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        configs.set(RestOptions.PROFILING_RESULT_DIR, tempDir.toString());
        profilingService = ProfilingService.getInstance(configs);
        verifyConfigsWorks(profilingService, tempDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        profilingService.close();
    }

    @Test
    public void testSingleInstance() throws IOException {
        ProfilingService instance = ProfilingService.getInstance(configs);
        Assertions.assertEquals(profilingService, instance);
        instance.close();
    }

    @Test
    void testFailedRequestUnderProfiling() throws ExecutionException, InterruptedException {
        ProfilingInfo profilingInfo =
                profilingService
                        .requestProfiling(resourceID, 10, ProfilingInfo.ProfilingMode.ITIMER)
                        .get();
        Assertions.assertEquals(ProfilingInfo.ProfilingStatus.RUNNING, profilingInfo.getStatus());
        try {
            profilingService
                    .requestProfiling(
                            resourceID, profilingDuration, ProfilingInfo.ProfilingMode.ITIMER)
                    .get();
            Assertions.fail("Duplicate profiling request should throw with IllegalStateException.");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testAllProfilingMode() throws ExecutionException, InterruptedException {
        for (ProfilingInfo.ProfilingMode mode : ProfilingInfo.ProfilingMode.values()) {
            ProfilingInfo profilingInfo =
                    profilingService.requestProfiling(resourceID, profilingDuration, mode).get();
            if (isNoPermissionOrAllocateSymbol(profilingInfo)) {
                LOG.warn(
                        "Ignoring failed profiling instance in {} mode, which caused by no permission.",
                        profilingInfo.getProfilingMode());
                continue;
            }
            Assertions.assertEquals(
                    ProfilingInfo.ProfilingStatus.RUNNING,
                    profilingInfo.getStatus(),
                    String.format(
                            "Submitting profiling request should be succeed or no permission, but got errorMsg=%s",
                            profilingInfo.getMessage()));
            waitForProfilingFinished(profilingService);
            Assertions.assertEquals(
                    ProfilingInfo.ProfilingStatus.FINISHED,
                    profilingInfo.getStatus(),
                    String.format(
                            "Profiling request should complete successful, but got errorMsg=%s",
                            profilingInfo.getMessage()));
        }

        verifyRollingDeletion(profilingService);
    }

    private void verifyConfigsWorks(ProfilingService profilingService, Path configuredDir) {
        Assertions.assertEquals(configuredDir.toString(), profilingService.getProfilingResultDir());
        Assertions.assertEquals(historySizeLimit, profilingService.getHistorySizeLimit());
    }

    private void verifyRollingDeletion(ProfilingService profilingService) {
        ArrayDeque<ProfilingInfo> profilingList =
                profilingService.getProfilingListForTest(resourceID);
        // Profiling History shouldn't exceed history size limit.
        Assertions.assertEquals(historySizeLimit, profilingList.size());
        // Profiling History files should be rolling deleted.
        Set<String> resultFileNames = new HashSet<>();
        File configuredDir = new File(profilingService.getProfilingResultDir());
        for (File f : Objects.requireNonNull(configuredDir.listFiles())) {
            resultFileNames.add(f.getName());
        }
        Assertions.assertEquals(profilingList.size(), resultFileNames.size());
        for (ProfilingInfo profilingInfo : profilingList) {
            String outputFile = profilingInfo.getOutputFile();
            Assertions.assertTrue(resultFileNames.contains(outputFile));
        }
    }

    private void waitForProfilingFinished(ProfilingService profilingService) {
        while (!profilingService.getProfilingFuture().isDone()) {}
    }

    /**
     * Check profiling instance failed caused by no permission to perf_events or missing of JDK
     * debug symbols.
     *
     * @return true if no permission to access perf_events.
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
