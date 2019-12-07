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

package org.apache.flink.python.env;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.python.PythonConfig;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * PythonDependencyInfo contains the information of third-party dependencies.
 */
@Internal
public final class PythonDependencyInfo {

	/**
	 * The python files uploaded by TableEnvironment#add_python_file() or command line option "-pyfs". The key is the
	 * path of the python file and the value is the corresponding origin file name.
	 */
	@Nonnull private final Map<String, String> pythonFiles;

	/**
	 * The path of the requirements file specified by TableEnvironment#set_python_requirements() or command line option
	 * "-pyreq".
	 */
	@Nullable private final String requirementsFilePath;

	/**
	 * The path of the requirements cached directory uploaded by TableEnvironment#set_python_requirements() or command
	 * line option "-pyreq". It is used to support installing python packages offline.
	 */
	@Nullable private final String requirementsCacheDir;

	/**
	 * The python archives uploaded by TableEnvironment#add_python_archive() or command line option "-pyarch". The key
	 * is the path of the archive file and the value is the name of the directory to extract to.
	 */
	@Nonnull private final Map<String, String> archives;

	/**
	 * The path of the python interpreter (e.g. /usr/local/bin/python) specified by
	 * pyflink.table.TableConfig#set_python_executable() or command line option "-pyexec".
	 */
	@Nullable private final String pythonExec;

	public PythonDependencyInfo(
		@Nonnull Map<String, String> pythonFiles,
		@Nullable String requirementsFilePath,
		@Nullable String requirementsCacheDir,
		@Nonnull Map<String, String> archives,
		@Nullable String pythonExec) {
		this.pythonFiles = Objects.requireNonNull(pythonFiles);
		this.requirementsFilePath = requirementsFilePath;
		this.requirementsCacheDir = requirementsCacheDir;
		this.pythonExec = pythonExec;
		this.archives = Objects.requireNonNull(archives);
	}

	public Map<String, String> getPythonFiles() {
		return pythonFiles;
	}

	public Optional<String> getRequirementsFilePath() {
		return Optional.ofNullable(requirementsFilePath);
	}

	public Optional<String> getRequirementsCacheDir() {
		return Optional.ofNullable(requirementsCacheDir);
	}

	public Optional<String> getPythonExec() {
		return Optional.ofNullable(pythonExec);
	}

	public Map<String, String> getArchives() {
		return archives;
	}

	/**
	 * Creates PythonDependencyInfo from GlobalJobParameters and DistributedCache.
	 *
	 * @param pythonConfig The python config.
	 * @param distributedCache The DistributedCache object of current task.
	 * @return The PythonDependencyInfo object that contains whole information of python dependency.
	 */
	public static PythonDependencyInfo create(PythonConfig pythonConfig, DistributedCache distributedCache)
		throws IOException {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, String> pythonFiles = new HashMap<>();
		if (pythonConfig.getPythonFilesInfo().isPresent()) {
			Map<String, String> filesIdToFilesName =
				mapper.readValue(pythonConfig.getPythonFilesInfo().get(), HashMap.class);
			for (Map.Entry<String, String> entry: filesIdToFilesName.entrySet()) {
				File pythonFile = distributedCache.getFile(entry.getKey());
				String filePath = pythonFile.getAbsolutePath();
				pythonFiles.put(filePath, entry.getValue());
			}
		}

		String requirementsFilePath = null;
		String requirementsCacheDir = null;
		if (pythonConfig.getPythonRequirementsFileInfo().isPresent()) {
			requirementsFilePath = distributedCache.getFile(
				pythonConfig.getPythonRequirementsFileInfo().get()).getAbsolutePath();
			if (pythonConfig.getPythonRequirementsCacheDirInfo().isPresent()) {
				requirementsCacheDir = distributedCache.getFile(
					pythonConfig.getPythonRequirementsCacheDirInfo().get()).getAbsolutePath();
			}
		}

		Map<String, String> archives = new HashMap<>();
		if (pythonConfig.getPythonArchivesInfo().isPresent()) {
			Map<String, String> archivesMap =
				mapper.readValue(pythonConfig.getPythonArchivesInfo().get(), HashMap.class);

			for (Map.Entry<String, String> entry: archivesMap.entrySet()) {
				String archiveFilePath = distributedCache.getFile(entry.getKey()).getAbsolutePath();
				String targetPath = entry.getValue();
				archives.put(archiveFilePath, targetPath);
			}
		}

		String pythonExec = pythonConfig.getPythonExec().orElse(null);

		return new PythonDependencyInfo(
			pythonFiles,
			requirementsFilePath,
			requirementsCacheDir,
			archives,
			pythonExec);
	}
}
