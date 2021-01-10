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

package org.apache.flink.python;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.util.PythonDependencyUtils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link PythonConfig}. */
public class PythonConfigTest {

    @Test
    public void testDefaultConfigure() {
        PythonConfig pythonConfig = new PythonConfig(new Configuration());
        assertThat(
                pythonConfig.getMaxBundleSize(),
                is(equalTo(PythonOptions.MAX_BUNDLE_SIZE.defaultValue())));
        assertThat(
                pythonConfig.getMaxBundleTimeMills(),
                is(equalTo(PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue())));
        assertThat(
                pythonConfig.getMaxArrowBatchSize(),
                is(equalTo(PythonOptions.MAX_ARROW_BATCH_SIZE.defaultValue())));
        assertThat(pythonConfig.getPythonFilesInfo().isEmpty(), is(true));
        assertThat(pythonConfig.getPythonRequirementsFileInfo().isPresent(), is(false));
        assertThat(pythonConfig.getPythonRequirementsCacheDirInfo().isPresent(), is(false));
        assertThat(pythonConfig.getPythonArchivesInfo().isEmpty(), is(true));
        assertThat(pythonConfig.getPythonExec(), is("python"));
        assertThat(
                pythonConfig.isUsingManagedMemory(),
                is(equalTo(PythonOptions.USE_MANAGED_MEMORY.defaultValue())));
    }

    @Test
    public void testMaxBundleSize() {
        Configuration config = new Configuration();
        config.set(PythonOptions.MAX_BUNDLE_SIZE, 10);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(pythonConfig.getMaxBundleSize(), is(equalTo(10)));
    }

    @Test
    public void testMaxBundleTimeMills() {
        Configuration config = new Configuration();
        config.set(PythonOptions.MAX_BUNDLE_TIME_MILLS, 10L);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(pythonConfig.getMaxBundleTimeMills(), is(equalTo(10L)));
    }

    @Test
    public void testMaxArrowBatchSize() {
        Configuration config = new Configuration();
        config.set(PythonOptions.MAX_ARROW_BATCH_SIZE, 10);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(pythonConfig.getMaxArrowBatchSize(), is(equalTo(10)));
    }

    @Test
    public void testPythonFilesInfo() {
        Configuration config = new Configuration();
        Map<String, String> pythonFiles = new HashMap<>();
        pythonFiles.put("python_file_{SHA256}", "file0.py");
        config.set(PythonDependencyUtils.PYTHON_FILES, pythonFiles);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(pythonConfig.getPythonFilesInfo(), is(equalTo(pythonFiles)));
    }

    @Test
    public void testPythonRequirementsFileInfo() {
        Configuration config = new Configuration();
        Map<String, String> pythonRequirementsFile =
                config.getOptional(PythonDependencyUtils.PYTHON_REQUIREMENTS_FILE)
                        .orElse(new HashMap<>());
        pythonRequirementsFile.put(PythonDependencyUtils.FILE, "python_requirements_file_{SHA256}");
        config.set(PythonDependencyUtils.PYTHON_REQUIREMENTS_FILE, pythonRequirementsFile);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(
                pythonConfig.getPythonRequirementsFileInfo().get(),
                is(equalTo("python_requirements_file_{SHA256}")));
    }

    @Test
    public void testPythonRequirementsCacheDirInfo() {
        Configuration config = new Configuration();
        Map<String, String> pythonRequirementsFile =
                config.getOptional(PythonDependencyUtils.PYTHON_REQUIREMENTS_FILE)
                        .orElse(new HashMap<>());
        pythonRequirementsFile.put(
                PythonDependencyUtils.CACHE, "python_requirements_cache_{SHA256}");
        config.set(PythonDependencyUtils.PYTHON_REQUIREMENTS_FILE, pythonRequirementsFile);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(
                pythonConfig.getPythonRequirementsCacheDirInfo().get(),
                is(equalTo("python_requirements_cache_{SHA256}")));
    }

    @Test
    public void testPythonArchivesInfo() {
        Configuration config = new Configuration();
        Map<String, String> pythonArchives = new HashMap<>();
        pythonArchives.put("python_archive_{SHA256}", "file0.zip");
        config.set(PythonDependencyUtils.PYTHON_ARCHIVES, pythonArchives);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(pythonConfig.getPythonArchivesInfo(), is(equalTo(pythonArchives)));
    }

    @Test
    public void testPythonExec() {
        Configuration config = new Configuration();
        config.set(PythonOptions.PYTHON_EXECUTABLE, "/usr/local/bin/python3");
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(pythonConfig.getPythonExec(), is(equalTo("/usr/local/bin/python3")));
    }

    @Test
    public void testManagedMemory() {
        Configuration config = new Configuration();
        config.set(PythonOptions.USE_MANAGED_MEMORY, true);
        PythonConfig pythonConfig = new PythonConfig(config);
        assertThat(pythonConfig.isUsingManagedMemory(), is(equalTo(true)));
    }
}
