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

package org.apache.flink.runtime.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.within;

class HardwareTest {

    @Test
    void testCpuCores() {
        try {
            assertThat(Hardware.getNumberCPUCores()).isGreaterThanOrEqualTo(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCpuCoresAsDoubleIsPositive() {
        assertThat(Hardware.getNumberCPUCoresAsDouble()).isGreaterThan(0.0);
    }

    @Test
    void testPhysicalMemory() {
        try {
            long physMem = Hardware.getSizeOfPhysicalMemory();
            assertThat(physMem).isGreaterThanOrEqualTo(-1);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCgroupV2FractionalLimit(@TempDir Path tempDir) throws IOException {
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "50000 100000");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isEqualTo(0.5);
    }

    @Test
    void testCgroupV2SubCoreLimit(@TempDir Path tempDir) throws IOException {
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "30000 100000");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isEqualTo(0.3);
    }

    @Test
    void testCgroupV2RepeatingFractionalLimit(@TempDir Path tempDir) throws IOException {
        // 33333 / 100000 = 0.33333 (one third, truncated at microsecond granularity)
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "33333 100000");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isCloseTo(1.0 / 3.0, within(1.0e-4));
    }

    @Test
    void testCgroupV2IntegerLimit(@TempDir Path tempDir) throws IOException {
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "200000 100000");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isEqualTo(2.0);
    }

    @Test
    void testCgroupV2Unlimited(@TempDir Path tempDir) throws IOException {
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "max 100000");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isEqualTo(-1.0);
    }

    @Test
    void testCgroupV2MissingFile(@TempDir Path tempDir) {
        assertThat(Hardware.getCpuLimitFromCgroupV2(tempDir.resolve("does_not_exist")))
                .isEqualTo(-1.0);
    }

    @Test
    void testCgroupV2MalformedContent(@TempDir Path tempDir) throws IOException {
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "garbage");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isEqualTo(-1.0);
    }

    @Test
    void testCgroupV2NonNumericValues(@TempDir Path tempDir) throws IOException {
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "abc def");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isEqualTo(-1.0);
    }

    @Test
    void testCgroupV2ZeroPeriod(@TempDir Path tempDir) throws IOException {
        Path cpuMax = writeFile(tempDir.resolve("cpu.max"), "50000 0");
        assertThat(Hardware.getCpuLimitFromCgroupV2(cpuMax)).isEqualTo(-1.0);
    }

    @Test
    void testCgroupV1FractionalLimit(@TempDir Path tempDir) throws IOException {
        Path quota = writeFile(tempDir.resolve("cpu.cfs_quota_us"), "50000");
        Path period = writeFile(tempDir.resolve("cpu.cfs_period_us"), "100000");
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period)).isEqualTo(0.5);
    }

    @Test
    void testCgroupV1OneThirdCpuLimit(@TempDir Path tempDir) throws IOException {
        Path quota = writeFile(tempDir.resolve("cpu.cfs_quota_us"), "33333");
        Path period = writeFile(tempDir.resolve("cpu.cfs_period_us"), "100000");
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period)).isEqualTo(0.33333);
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period))
                .isCloseTo(1.0 / 3.0, within(1.0e-4));
    }

    @Test
    void testCgroupV1IntegerLimit(@TempDir Path tempDir) throws IOException {
        Path quota = writeFile(tempDir.resolve("cpu.cfs_quota_us"), "150000");
        Path period = writeFile(tempDir.resolve("cpu.cfs_period_us"), "100000");
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period)).isEqualTo(1.5);
    }

    @Test
    void testCgroupV1Unlimited(@TempDir Path tempDir) throws IOException {
        Path quota = writeFile(tempDir.resolve("cpu.cfs_quota_us"), "-1");
        Path period = writeFile(tempDir.resolve("cpu.cfs_period_us"), "100000");
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period)).isEqualTo(-1.0);
    }

    @Test
    void testCgroupV1ZeroPeriod(@TempDir Path tempDir) throws IOException {
        Path quota = writeFile(tempDir.resolve("cpu.cfs_quota_us"), "50000");
        Path period = writeFile(tempDir.resolve("cpu.cfs_period_us"), "0");
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period)).isEqualTo(-1.0);
    }

    @Test
    void testCgroupV1MissingFiles(@TempDir Path tempDir) {
        Path quota = tempDir.resolve("missing_quota");
        Path period = tempDir.resolve("missing_period");
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period)).isEqualTo(-1.0);
    }

    @Test
    void testCgroupV1NonNumericQuota(@TempDir Path tempDir) throws IOException {
        Path quota = writeFile(tempDir.resolve("cpu.cfs_quota_us"), "not-a-number");
        Path period = writeFile(tempDir.resolve("cpu.cfs_period_us"), "100000");
        assertThat(Hardware.getCpuLimitFromCgroupV1(quota, period)).isEqualTo(-1.0);
    }

    private static Path writeFile(Path path, String content) throws IOException {
        Files.writeString(path, content);
        return path;
    }
}
