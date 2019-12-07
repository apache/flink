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
 * Tests for {@link PythonConfig}.
 */
public class PythonConfigTest {

	@Test
	public void testDefaultConfigure() {
		PythonConfig pythonConfig = new PythonConfig(new Configuration());
		assertThat(pythonConfig.getMaxBundleSize(),
			is(equalTo(PythonOptions.MAX_BUNDLE_SIZE.defaultValue())));
		assertThat(pythonConfig.getMaxBundleTimeMills(),
			is(equalTo(PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue())));
		assertThat(pythonConfig.getPythonFrameworkMemorySize(),
			is(equalTo(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE.defaultValue())));
		assertThat(pythonConfig.getPythonDataBufferMemorySize(),
			is(equalTo(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE.defaultValue())));
		assertThat(pythonConfig.getPythonFilesInfo().isPresent(), is(false));
		assertThat(pythonConfig.getPythonRequirementsFileInfo().isPresent(), is(false));
		assertThat(pythonConfig.getPythonRequirementsCacheDirInfo().isPresent(), is(false));
		assertThat(pythonConfig.getPythonArchivesInfo().isPresent(), is(false));
		assertThat(pythonConfig.getPythonExec().isPresent(), is(false));
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
	public void testPythonFrameworkMemorySize() {
		Configuration config = new Configuration();
		config.set(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE, "100mb");
		PythonConfig pythonConfig = new PythonConfig(config);
		assertThat(pythonConfig.getPythonFrameworkMemorySize(), is(equalTo("100mb")));
	}

	@Test
	public void testPythonDataBufferMemorySize() {
		Configuration config = new Configuration();
		config.set(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE, "100mb");
		PythonConfig pythonConfig = new PythonConfig(config);
		assertThat(pythonConfig.getPythonDataBufferMemorySize(), is(equalTo("100mb")));
	}

	@Test
	public void testPythonFilesInfo() {
		Configuration config = new Configuration();
		config.setString(PythonConfig.PYTHON_FILES, "{\"python_file_0\" : \"file0.py\"}");
		PythonConfig pythonConfig = new PythonConfig(config);
		assertThat(pythonConfig.getPythonFilesInfo().get(), is(equalTo("{\"python_file_0\" : \"file0.py\"}")));
	}

	@Test
	public void testPythonRequirementsFileInfo() {
		Configuration config = new Configuration();
		config.setString(PythonConfig.PYTHON_REQUIREMENTS_FILE, "python_requirements_file_0");
		PythonConfig pythonConfig = new PythonConfig(config);
		assertThat(pythonConfig.getPythonRequirementsFileInfo().get(), is(equalTo("python_requirements_file_0")));
	}

	@Test
	public void testPythonRequirementsCacheDirInfo() {
		Configuration config = new Configuration();
		config.setString(PythonConfig.PYTHON_REQUIREMENTS_CACHE, "python_requirements_cache_1");
		PythonConfig pythonConfig = new PythonConfig(config);
		assertThat(pythonConfig.getPythonRequirementsCacheDirInfo().get(), is(equalTo("python_requirements_cache_1")));
	}

	@Test
	public void testPythonArchivesInfo() {
		Configuration config = new Configuration();
		config.setString(PythonConfig.PYTHON_ARCHIVES, "{\"python_archive_0\" : \"file0.zip\"}");
		PythonConfig pythonConfig = new PythonConfig(config);
		assertThat(pythonConfig.getPythonArchivesInfo().get(), is(equalTo("{\"python_archive_0\" : \"file0.zip\"}")));
	}

	@Test
	public void testPythonExec() {
		Configuration config = new Configuration();
		config.setString(PythonConfig.PYTHON_EXEC, "/usr/local/bin/python3");
		PythonConfig pythonConfig = new PythonConfig(config);
		assertThat(pythonConfig.getPythonExec().get(), is(equalTo("/usr/local/bin/python3")));
	}

}
