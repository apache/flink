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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test all configurations can be set using configuration. */
class PythonOptionsTest {

    @Test
    void testBundleSize() {
        final Configuration configuration = new Configuration();
        final int defaultBundleSize = configuration.getInteger(PythonOptions.MAX_BUNDLE_SIZE);
        assertThat(defaultBundleSize).isEqualTo(PythonOptions.MAX_BUNDLE_SIZE.defaultValue());

        final int expectedBundleSize = 100;
        configuration.setInteger(PythonOptions.MAX_BUNDLE_SIZE, expectedBundleSize);

        final int actualBundleSize = configuration.getInteger(PythonOptions.MAX_BUNDLE_SIZE);
        assertThat(actualBundleSize).isEqualTo(expectedBundleSize);
    }

    @Test
    void testBundleTime() {
        final Configuration configuration = new Configuration();
        final long defaultBundleTime = configuration.getLong(PythonOptions.MAX_BUNDLE_TIME_MILLS);
        assertThat(defaultBundleTime).isEqualTo(PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue());

        final long expectedBundleTime = 100;
        configuration.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, expectedBundleTime);

        final long actualBundleSize = configuration.getLong(PythonOptions.MAX_BUNDLE_TIME_MILLS);
        assertThat(actualBundleSize).isEqualTo(expectedBundleTime);
    }

    @Test
    void testArrowBatchSize() {
        final Configuration configuration = new Configuration();
        final int defaultArrowBatchSize =
                configuration.getInteger(PythonOptions.MAX_ARROW_BATCH_SIZE);
        assertThat(defaultArrowBatchSize)
                .isEqualTo(PythonOptions.MAX_ARROW_BATCH_SIZE.defaultValue());

        final int expectedArrowBatchSize = 100;
        configuration.setInteger(PythonOptions.MAX_ARROW_BATCH_SIZE, expectedArrowBatchSize);

        final int actualArrowBatchSize =
                configuration.getInteger(PythonOptions.MAX_ARROW_BATCH_SIZE);
        assertThat(actualArrowBatchSize).isEqualTo(expectedArrowBatchSize);
    }

    @Test
    void testPythonMetricEnabled() {
        final Configuration configuration = new Configuration();
        final boolean isMetricEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
        assertThat(isMetricEnabled).isEqualTo(PythonOptions.PYTHON_METRIC_ENABLED.defaultValue());

        final boolean expectedIsMetricEnabled = false;
        configuration.setBoolean(PythonOptions.PYTHON_METRIC_ENABLED, false);

        final boolean actualIsMetricEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
        assertThat(actualIsMetricEnabled).isEqualTo(expectedIsMetricEnabled);
    }

    @Test
    void testPythonProfileEnabled() {
        final Configuration configuration = new Configuration();
        final boolean isProfileEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_PROFILE_ENABLED);
        assertThat(isProfileEnabled).isEqualTo(PythonOptions.PYTHON_PROFILE_ENABLED.defaultValue());

        final boolean expectedIsProfileEnabled = true;
        configuration.setBoolean(PythonOptions.PYTHON_PROFILE_ENABLED, true);

        final boolean actualIsProfileEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_PROFILE_ENABLED);
        assertThat(actualIsProfileEnabled).isEqualTo(expectedIsProfileEnabled);
    }

    @Test
    void testPythonFiles() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonFiles =
                configuration.getOptional(PythonOptions.PYTHON_FILES);
        assertThat(defaultPythonFiles).isEmpty();

        final String expectedPythonFiles = "tmp_dir/test1.py,tmp_dir/test2.py";
        configuration.set(PythonOptions.PYTHON_FILES, expectedPythonFiles);

        final String actualPythonFiles = configuration.get(PythonOptions.PYTHON_FILES);
        assertThat(actualPythonFiles).isEqualTo(expectedPythonFiles);
    }

    @Test
    void testPythonRequirements() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonRequirements =
                configuration.getOptional(PythonOptions.PYTHON_REQUIREMENTS);
        assertThat(defaultPythonRequirements).isEmpty();

        final String expectedPythonRequirements = "tmp_dir/requirements.txt#tmp_dir/cache";
        configuration.set(PythonOptions.PYTHON_REQUIREMENTS, expectedPythonRequirements);

        final String actualPythonRequirements =
                configuration.get(PythonOptions.PYTHON_REQUIREMENTS);
        assertThat(actualPythonRequirements).isEqualTo(expectedPythonRequirements);
    }

    @Test
    void testPythonArchives() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonArchives =
                configuration.getOptional(PythonOptions.PYTHON_ARCHIVES);
        assertThat(defaultPythonArchives).isEmpty();

        final String expectedPythonArchives = "tmp_dir/py37.zip#venv,tmp_dir/data.zip";
        configuration.set(PythonOptions.PYTHON_ARCHIVES, expectedPythonArchives);

        final String actualPythonArchives = configuration.get(PythonOptions.PYTHON_ARCHIVES);
        assertThat(actualPythonArchives).isEqualTo(expectedPythonArchives);
    }

    @Test
    void testPythonExecutable() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonExecutable =
                configuration.getOptional(PythonOptions.PYTHON_EXECUTABLE);
        assertThat(defaultPythonExecutable).isEmpty();

        final String expectedPythonExecutable = "venv/py37/bin/python";
        configuration.set(PythonOptions.PYTHON_EXECUTABLE, expectedPythonExecutable);

        final String actualPythonExecutable = configuration.get(PythonOptions.PYTHON_EXECUTABLE);
        assertThat(actualPythonExecutable).isEqualTo(expectedPythonExecutable);
    }

    @Test
    void testPythonClientExecutable() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonClientExecutable =
                configuration.getOptional(PythonOptions.PYTHON_CLIENT_EXECUTABLE);
        assertThat(defaultPythonClientExecutable).isEmpty();

        final String expectedPythonClientExecutable = "tmp_dir/test1.py,tmp_dir/test2.py";
        configuration.set(PythonOptions.PYTHON_CLIENT_EXECUTABLE, expectedPythonClientExecutable);

        final String actualPythonClientExecutable =
                configuration.get(PythonOptions.PYTHON_CLIENT_EXECUTABLE);
        assertThat(actualPythonClientExecutable).isEqualTo(expectedPythonClientExecutable);
    }

    @Test
    void testPythonSystemEnvEnabled() {
        final Configuration configuration = new Configuration();
        final boolean isSystemEnvEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_SYSTEMENV_ENABLED);
        assertThat(isSystemEnvEnabled)
                .isEqualTo(PythonOptions.PYTHON_SYSTEMENV_ENABLED.defaultValue());

        final boolean expectedIsSystemEnvEnabled = false;
        configuration.setBoolean(PythonOptions.PYTHON_SYSTEMENV_ENABLED, false);

        final boolean actualIsSystemEnvEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_SYSTEMENV_ENABLED);
        assertThat(actualIsSystemEnvEnabled).isEqualTo(expectedIsSystemEnvEnabled);
    }

    @Test
    void testPythonPath() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonPath =
                configuration.getOptional(PythonOptions.PYTHON_PATH);
        assertThat(defaultPythonPath).isEmpty();

        final String expectedPythonPath = "venv/py37/bin/python";
        configuration.set(PythonOptions.PYTHON_PATH, expectedPythonPath);

        final String actualPythonPath = configuration.get(PythonOptions.PYTHON_PATH);
        assertThat(actualPythonPath).isEqualTo(expectedPythonPath);
    }
}
