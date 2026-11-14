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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@code Process.*} metrics of {@link SystemResourcesCounter} and {@link
 * SystemResourcesMetricsInitializer}, read from a fake procfs directory.
 */
class SystemResourcesProcessMetricsTest {

    private static final String STATUS = "Name:\tjava\nVmRSS:\t    2048 kB\nThreads:\t42\n";
    private static final String IO =
            "rchar: 9000\nwchar: 8000\nread_bytes: 4096\nwrite_bytes: 1024\n"
                    + "cancelled_write_bytes: 512\n";

    @TempDir private Path procSelfDir;

    @Test
    void testReadsProcessUsageFromProcFs() throws IOException {
        writeProcFile("status", STATUS);
        writeProcFile("io", IO);
        final SystemResourcesCounter counter = createCounter();

        counter.calculateProcessUsage();

        assertThat(counter.getProcessMemoryRSS()).isEqualTo(2048L * 1024);
        assertThat(counter.getProcessBytesRead()).isEqualTo(4096L);
        assertThat(counter.getProcessBytesWritten()).isEqualTo(1024L);
    }

    @Test
    void testRegistersProcessUsageGauges() throws IOException {
        writeProcFile("status", STATUS);
        writeProcFile("io", IO);
        final SystemResourcesCounter counter = createCounter();
        counter.calculateProcessUsage();

        final Map<String, InterceptingOperatorMetricGroup> addedGroups = new HashMap<>();
        final InterceptingOperatorMetricGroup processGroup =
                new InterceptingOperatorMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return addedGroups.computeIfAbsent(
                                name, k -> new InterceptingOperatorMetricGroup());
                    }
                };
        SystemResourcesMetricsInitializer.instantiateProcessMetrics(processGroup, counter);

        assertThat(addedGroups).containsOnlyKeys("Memory", "IO");
        assertThat(((Gauge<?>) addedGroups.get("Memory").get("RSS")).getValue())
                .isEqualTo(2048L * 1024);
        assertThat(((Gauge<?>) addedGroups.get("IO").get("Read")).getValue()).isEqualTo(4096L);
        assertThat(((Gauge<?>) addedGroups.get("IO").get("Write")).getValue()).isEqualTo(1024L);
    }

    @Test
    void testReportsMinusOneUntilProcFsFileIsReadable() throws IOException {
        writeProcFile("status", STATUS);
        final SystemResourcesCounter counter = createCounter();

        counter.calculateProcessUsage();

        assertThat(counter.getProcessMemoryRSS()).isEqualTo(2048L * 1024);
        assertThat(counter.getProcessBytesRead()).isEqualTo(-1L);
        assertThat(counter.getProcessBytesWritten()).isEqualTo(-1L);

        writeProcFile("io", IO);
        counter.calculateProcessUsage();

        assertThat(counter.getProcessBytesRead()).isEqualTo(4096L);
        assertThat(counter.getProcessBytesWritten()).isEqualTo(1024L);
    }

    @Test
    void testReportsMinusOneForMalformedProcFsFiles() throws IOException {
        writeProcFile("status", "VmRSS:\tunknown kB\n");
        writeProcFile("io", "read_bytes: 4096\n");
        final SystemResourcesCounter counter = createCounter();

        counter.calculateProcessUsage();

        assertThat(counter.getProcessMemoryRSS()).isEqualTo(-1L);
        assertThat(counter.getProcessBytesRead()).isEqualTo(-1L);
        assertThat(counter.getProcessBytesWritten()).isEqualTo(-1L);
    }

    private SystemResourcesCounter createCounter() {
        return new SystemResourcesCounter(Duration.ofSeconds(1), procSelfDir);
    }

    private void writeProcFile(String name, String content) throws IOException {
        Files.write(procSelfDir.resolve(name), content.getBytes(StandardCharsets.UTF_8));
    }
}
