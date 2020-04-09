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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * Configurations for the Python job which are used at run time.
 */
@Internal
public class PythonConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String PYTHON_FILES = "python.files";
	public static final String PYTHON_REQUIREMENTS_FILE = "python.requirements-file";
	public static final String PYTHON_REQUIREMENTS_CACHE = "python.requirements-cache";
	public static final String PYTHON_ARCHIVES = "python.archives";
	public static final String PYTHON_EXEC = "python.exec";

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
	 * The amount of memory to be allocated by the Python framework.
	 */
	private final String pythonFrameworkMemorySize;

	/**
	 * The amount of memory to be allocated by the input/output buffer of a Python worker.
	 */
	private final String pythonDataBufferMemorySize;

	/**
	 * The python files uploaded by pyflink.table.TableEnvironment#add_python_file() or command line
	 * option "-pyfs". It is a json string. The key is the file key in distribute cache and the
	 * value is the corresponding origin file name.
	 */
	@Nullable
	private final String pythonFilesInfo;

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
	 * command line option "-pyarch". It is a json string. The key is the file key of the archives
	 * in distribute cache and the value is the name of the directory to extract to.
	 */
	@Nullable
	private final String pythonArchivesInfo;

	/**
	 * The path of the python interpreter (e.g. /usr/local/bin/python) specified by
	 * pyflink.table.TableConfig#set_python_executable() or command line option "-pyexec".
	 */
	@Nullable
	private final String pythonExec;

	/**
	 * Whether metric is enabled.
	 */
	private final boolean metricEnabled;

	public PythonConfig(Configuration config) {
		maxBundleSize = config.get(PythonOptions.MAX_BUNDLE_SIZE);
		maxBundleTimeMills = config.get(PythonOptions.MAX_BUNDLE_TIME_MILLS);
		maxArrowBatchSize = config.get(PythonOptions.MAX_ARROW_BATCH_SIZE);
		pythonFrameworkMemorySize = config.get(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE);
		pythonDataBufferMemorySize = config.get(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE);
		pythonFilesInfo = config.getString(PYTHON_FILES, null);
		pythonRequirementsFileInfo = config.getString(PYTHON_REQUIREMENTS_FILE, null);
		pythonRequirementsCacheDirInfo = config.getString(PYTHON_REQUIREMENTS_CACHE, null);
		pythonArchivesInfo = config.getString(PYTHON_ARCHIVES, null);
		pythonExec = config.getString(PYTHON_EXEC, null);
		metricEnabled = config.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
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

	public String getPythonFrameworkMemorySize() {
		return pythonFrameworkMemorySize;
	}

	public String getPythonDataBufferMemorySize() {
		return pythonDataBufferMemorySize;
	}

	public Optional<String> getPythonFilesInfo() {
		return Optional.ofNullable(pythonFilesInfo);
	}

	public Optional<String> getPythonRequirementsFileInfo() {
		return Optional.ofNullable(pythonRequirementsFileInfo);
	}

	public Optional<String> getPythonRequirementsCacheDirInfo() {
		return Optional.ofNullable(pythonRequirementsCacheDirInfo);
	}

	public Optional<String> getPythonArchivesInfo() {
		return Optional.ofNullable(pythonArchivesInfo);
	}

	public Optional<String> getPythonExec() {
		return Optional.ofNullable(pythonExec);
	}

	public boolean isMetricEnabled() {
		return metricEnabled;
	}
}
