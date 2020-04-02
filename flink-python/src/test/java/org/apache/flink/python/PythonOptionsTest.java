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

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test all configurations can be set using configuration.
 */
public class PythonOptionsTest {

	@Test
	public void testBundleSize() {
		final Configuration configuration = new Configuration();
		final int defaultBundleSize = configuration.getInteger(PythonOptions.MAX_BUNDLE_SIZE);
		assertThat(defaultBundleSize, is(equalTo(PythonOptions.MAX_BUNDLE_SIZE.defaultValue())));

		final int expectedBundleSize = 100;
		configuration.setInteger(PythonOptions.MAX_BUNDLE_SIZE, expectedBundleSize);

		final int actualBundleSize = configuration.getInteger(PythonOptions.MAX_BUNDLE_SIZE);
		assertThat(actualBundleSize, is(equalTo(expectedBundleSize)));
	}

	@Test
	public void testBundleTime() {
		final Configuration configuration = new Configuration();
		final long defaultBundleTime = configuration.getLong(PythonOptions.MAX_BUNDLE_TIME_MILLS);
		assertThat(defaultBundleTime, is(equalTo(PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue())));

		final long expectedBundleTime = 100;
		configuration.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, expectedBundleTime);

		final long actualBundleSize = configuration.getLong(PythonOptions.MAX_BUNDLE_TIME_MILLS);
		assertThat(actualBundleSize, is(equalTo(expectedBundleTime)));
	}

	@Test
	public void testPythonFrameworkMemorySize() {
		final Configuration configuration = new Configuration();
		final String defaultPythonFrameworkMemorySize = configuration.getString(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE);
		assertThat(defaultPythonFrameworkMemorySize, is(equalTo(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE.defaultValue())));

		final String expectedPythonFrameworkMemorySize = "100mb";
		configuration.setString(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE, expectedPythonFrameworkMemorySize);

		final String actualPythonFrameworkMemorySize = configuration.getString(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE);
		assertThat(actualPythonFrameworkMemorySize, is(equalTo(expectedPythonFrameworkMemorySize)));
	}

	@Test
	public void testPythonBufferMemorySize() {
		final Configuration configuration = new Configuration();
		final String defaultPythonBufferMemorySize = configuration.getString(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE);
		assertThat(defaultPythonBufferMemorySize, is(equalTo(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE.defaultValue())));

		final String expectedPythonBufferMemorySize = "100mb";
		configuration.setString(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE, expectedPythonBufferMemorySize);

		final String actualPythonBufferMemorySize = configuration.getString(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE);
		assertThat(actualPythonBufferMemorySize, is(equalTo(expectedPythonBufferMemorySize)));
	}

	@Test
	public void testArrowBatchSize() {
		final Configuration configuration = new Configuration();
		final int defaultArrowBatchSize = configuration.getInteger(PythonOptions.MAX_ARROW_BATCH_SIZE);
		assertThat(defaultArrowBatchSize, is(equalTo(PythonOptions.MAX_ARROW_BATCH_SIZE.defaultValue())));

		final int expectedArrowBatchSize = 100;
		configuration.setInteger(PythonOptions.MAX_ARROW_BATCH_SIZE, expectedArrowBatchSize);

		final int actualArrowBatchSize = configuration.getInteger(PythonOptions.MAX_ARROW_BATCH_SIZE);
		assertThat(actualArrowBatchSize, is(equalTo(expectedArrowBatchSize)));
	}

	@Test
	public void testPythonMetricEnabled() {
		final Configuration configuration = new Configuration();
		final boolean isMetricEnabled = configuration.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
		assertThat(isMetricEnabled, is(equalTo(PythonOptions.PYTHON_METRIC_ENABLED.defaultValue())));

		final boolean expectedIsMetricEnabled = false;
		configuration.setBoolean(PythonOptions.PYTHON_METRIC_ENABLED, false);

		final boolean actualIsMetricEnabled = configuration.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
		assertThat(actualIsMetricEnabled, is(equalTo(expectedIsMetricEnabled)));
	}
}
