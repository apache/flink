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

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.util.PythonDependencyUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Configurations for the Python job which are used at run time.
 */
@Internal
public class PythonConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Max number of elements to include in a bundle.
	 */
	private final int maxBundleSize;

	/**
	 * Max duration of a bundle.
	 */
	private final long maxBundleTimeMills;

	/**
	 * Max number of elements to include in an arrow batch.
	 */
	private final int maxArrowBatchSize;

	/**
	 * The python files uploaded by pyflink.table.TableEnvironment#add_python_file() or command line
	 * option "-pyfs". The key is the file key in distribute cache and the value is the corresponding
	 * origin file name.
	 */
	private final Map<String, String> pythonFilesInfo;

	/**
	 * The file key of the requirements file in distribute cache. It is specified by
	 * pyflink.table.TableEnvironment#set_python_requirements() or command line option "-pyreq".
	 */
	@Nullable
	private final String pythonRequirementsFileInfo;

	/**
	 * The file key of the requirements cached directory in distribute cache. It is specified by
	 * pyflink.table.TableEnvironment#set_python_requirements() or command line option "-pyreq".
	 * It is used to support installing python packages offline.
	 */
	@Nullable
	private final String pythonRequirementsCacheDirInfo;

	/**
	 * The python archives uploaded by pyflink.table.TableEnvironment#add_python_archive() or
	 * command line option "-pyarch". The key is the file key of the archives in distribute cache
	 * and the value is the name of the directory to extract to.
	 */
	private final Map<String, String> pythonArchivesInfo;

	/**
	 * The path of the python interpreter (e.g. /usr/local/bin/python) specified by
	 * pyflink.table.TableConfig#set_python_executable() or command line option "-pyexec".
	 */
	private final String pythonExec;

	/**
	 * Whether metric is enabled.
	 */
	private final boolean metricEnabled;

	/**
	 * Whether to use managed memory for the Python worker.
	 */
	private final boolean isUsingManagedMemory;

	/**
	 * The Configuration that contains execution configs and dependencies info.
	 * */
	private final Configuration mergedConfig;

	public PythonConfig(Configuration config) {
		mergedConfig = config;
		maxBundleSize = config.get(PythonOptions.MAX_BUNDLE_SIZE);
		maxBundleTimeMills = config.get(PythonOptions.MAX_BUNDLE_TIME_MILLS);
		maxArrowBatchSize = config.get(PythonOptions.MAX_ARROW_BATCH_SIZE);
		pythonFilesInfo = config.getOptional(PythonDependencyUtils.PYTHON_FILES).orElse(new HashMap<>());
		pythonRequirementsFileInfo = config.getOptional(PythonDependencyUtils.PYTHON_REQUIREMENTS_FILE)
			.orElse(new HashMap<>())
			.get(PythonDependencyUtils.FILE);
		pythonRequirementsCacheDirInfo = config.getOptional(PythonDependencyUtils.PYTHON_REQUIREMENTS_FILE)
			.orElse(new HashMap<>())
			.get(PythonDependencyUtils.CACHE);
		pythonArchivesInfo = config.getOptional(PythonDependencyUtils.PYTHON_ARCHIVES).orElse(new HashMap<>());
		pythonExec = config.get(PythonOptions.PYTHON_EXECUTABLE);
		metricEnabled = config.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
		isUsingManagedMemory = config.getBoolean(PythonOptions.USE_MANAGED_MEMORY);
	}

	public int getMaxBundleSize() {
		return maxBundleSize;
	}

	public long getMaxBundleTimeMills() {
		return maxBundleTimeMills;
	}

	public int getMaxArrowBatchSize() {
		return maxArrowBatchSize;
	}

	public Map<String, String> getPythonFilesInfo() {
		return pythonFilesInfo;
	}

	public Optional<String> getPythonRequirementsFileInfo() {
		return Optional.ofNullable(pythonRequirementsFileInfo);
	}

	public Optional<String> getPythonRequirementsCacheDirInfo() {
		return Optional.ofNullable(pythonRequirementsCacheDirInfo);
	}

	public Map<String, String> getPythonArchivesInfo() {
		return pythonArchivesInfo;
	}

	public String getPythonExec() {
		return pythonExec;
	}

	public boolean isMetricEnabled() {
		return metricEnabled;
	}

	public boolean isUsingManagedMemory() {
		return isUsingManagedMemory;
	}

	public Configuration getMergedConfig() {
		return mergedConfig;
	}
}
