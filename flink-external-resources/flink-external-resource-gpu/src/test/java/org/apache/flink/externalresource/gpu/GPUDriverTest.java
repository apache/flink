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

package org.apache.flink.externalresource.gpu;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GPUDriver}. */
class GPUDriverTest {

    private static final String TESTING_DISCOVERY_SCRIPT_PATH =
            "src/test/resources/testing-gpu-discovery.sh";

    @Test
    void testGPUDriverWithTestScript() throws Exception {
        final int gpuAmount = 2;
        final Configuration config = new Configuration();
        config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, TESTING_DISCOVERY_SCRIPT_PATH);

        final GPUDriver gpuDriver = new GPUDriver(config);
        final Set<GPUInfo> gpuResource = gpuDriver.retrieveResourceInfo(gpuAmount);

        assertThat(gpuResource).hasSize(gpuAmount);
    }

    @Test
    void testGPUDriverWithInvalidAmount() throws Exception {
        final int gpuAmount = -1;
        final Configuration config = new Configuration();
        config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, TESTING_DISCOVERY_SCRIPT_PATH);

        final GPUDriver gpuDriver = new GPUDriver(config);
        assertThatThrownBy(() -> gpuDriver.retrieveResourceInfo(gpuAmount))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGPUDriverWithIllegalConfigTestScript() {
        final Configuration config = new Configuration();
        config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, " ");

        assertThatThrownBy(() -> new GPUDriver(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testGPUDriverWithTestScriptDoNotExist() throws Exception {
        final Configuration config = new Configuration();
        config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, "invalid/path");
        assertThatThrownBy(() -> new GPUDriver(config)).isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testGPUDriverWithInexecutableScript(@TempDir Path tempDir) throws Exception {
        final Configuration config = new Configuration();

        Path tempFile = Files.createTempFile(tempDir, UUID.randomUUID().toString(), "");
        final File inExecutableFile = tempFile.toFile();
        assertThat(inExecutableFile.setExecutable(false)).isTrue();

        config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, inExecutableFile.getAbsolutePath());

        assertThatThrownBy(() -> new GPUDriver(config)).isInstanceOf(FlinkException.class);
    }

    @Test
    void testGPUDriverWithTestScriptExitWithNonZero() throws Exception {
        final Configuration config = new Configuration();
        config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, TESTING_DISCOVERY_SCRIPT_PATH);
        config.setString(GPUDriver.DISCOVERY_SCRIPT_ARG, "--exit-non-zero");

        final GPUDriver gpuDriver = new GPUDriver(config);
        assertThatThrownBy(() -> gpuDriver.retrieveResourceInfo(1))
                .isInstanceOf(FlinkException.class);
    }
}
