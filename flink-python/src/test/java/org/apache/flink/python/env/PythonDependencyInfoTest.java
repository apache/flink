/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.env;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.util.PythonDependencyUtils;
import org.apache.flink.util.OperatingSystem;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.apache.flink.python.PythonOptions.PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO;
import static org.apache.flink.python.PythonOptions.PYTHON_FILES_DISTRIBUTED_CACHE_INFO;
import static org.apache.flink.python.PythonOptions.PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for {@link PythonDependencyInfo}. */
class PythonDependencyInfoTest {

    private DistributedCache distributedCache;

    public PythonDependencyInfoTest() {
        Map<String, Future<Path>> distributeCachedFiles = new HashMap<>();
        distributeCachedFiles.put(
                "python_file_{SHA256_0}",
                CompletableFuture.completedFuture(new Path("/distributed_cache/file0")));
        distributeCachedFiles.put(
                "python_file_{SHA256_1}",
                CompletableFuture.completedFuture(new Path("/distributed_cache/file1")));
        distributeCachedFiles.put(
                "python_requirements_file_{SHA256}",
                CompletableFuture.completedFuture(new Path("/distributed_cache/file2")));
        distributeCachedFiles.put(
                "python_requirements_cache_{SHA256}",
                CompletableFuture.completedFuture(new Path("/distributed_cache/file3")));
        distributeCachedFiles.put(
                "python_archive_{SHA256_0}",
                CompletableFuture.completedFuture(new Path("/distributed_cache/file4")));
        distributeCachedFiles.put(
                "python_archive_{SHA256_1}",
                CompletableFuture.completedFuture(new Path("/distributed_cache/file5")));
        distributedCache = new DistributedCache(distributeCachedFiles);
    }

    @Test
    void testParsePythonFiles() {
        // Skip this test on Windows as we can not control the Window Driver letters.
        assumeThat(OperatingSystem.isWindows()).isFalse();

        Configuration config = new Configuration();
        Map<String, String> pythonFiles = new HashMap<>();
        pythonFiles.put("python_file_{SHA256_0}", "test_file1.py");
        pythonFiles.put("python_file_{SHA256_1}", "test_file2.py");
        config.set(PYTHON_FILES_DISTRIBUTED_CACHE_INFO, pythonFiles);
        PythonDependencyInfo dependencyInfo = PythonDependencyInfo.create(config, distributedCache);

        Map<String, String> expected = new HashMap<>();
        expected.put("/distributed_cache/file0", "test_file1.py");
        expected.put("/distributed_cache/file1", "test_file2.py");
        assertThat(dependencyInfo.getPythonFiles()).isEqualTo(expected);
    }

    @Test
    void testParsePythonRequirements() throws IOException {
        // Skip this test on Windows as we can not control the Window Driver letters.
        assumeThat(OperatingSystem.isWindows()).isFalse();

        Configuration config = new Configuration();
        config.set(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO, new HashMap<>());
        config.get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)
                .put(PythonDependencyUtils.FILE, "python_requirements_file_{SHA256}");
        PythonDependencyInfo dependencyInfo = PythonDependencyInfo.create(config, distributedCache);

        assertThat(dependencyInfo.getRequirementsFilePath().get())
                .isEqualTo("/distributed_cache/file2");
        assertThat(dependencyInfo.getRequirementsCacheDir()).isNotPresent();

        config.get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)
                .put(PythonDependencyUtils.CACHE, "python_requirements_cache_{SHA256}");
        dependencyInfo = PythonDependencyInfo.create(config, distributedCache);

        assertThat(dependencyInfo.getRequirementsFilePath().get())
                .isEqualTo("/distributed_cache/file2");
        assertThat(dependencyInfo.getRequirementsCacheDir().get())
                .isEqualTo("/distributed_cache/file3");
    }

    @Test
    void testParsePythonArchives() {
        // Skip this test on Windows as we can not control the Window Driver letters.
        assumeThat(OperatingSystem.isWindows()).isFalse();

        Configuration config = new Configuration();
        Map<String, String> pythonArchives = new HashMap<>();
        pythonArchives.put("python_archive_{SHA256_0}", "py27.zip");
        pythonArchives.put("python_archive_{SHA256_1}", "py37");
        config.set(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO, pythonArchives);
        PythonDependencyInfo dependencyInfo = PythonDependencyInfo.create(config, distributedCache);

        Map<String, String> expected = new HashMap<>();
        expected.put("/distributed_cache/file4", "py27.zip");
        expected.put("/distributed_cache/file5", "py37");
        assertThat(dependencyInfo.getArchives()).isEqualTo(expected);
    }

    @Test
    void testParsePythonExec() {
        Configuration config = new Configuration();
        config.set(PythonOptions.PYTHON_EXECUTABLE, "/usr/bin/python3");
        PythonDependencyInfo dependencyInfo = PythonDependencyInfo.create(config, distributedCache);

        assertThat(dependencyInfo.getPythonExec()).isEqualTo("/usr/bin/python3");
    }
}
