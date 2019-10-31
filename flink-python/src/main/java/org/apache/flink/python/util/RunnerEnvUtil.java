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

package org.apache.flink.python.util;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utils for building the most basic environment for running python udf workers.
 * The basic environment does not include python part of Apache Beam.
 * Users need to prepare it themselves.
 */
public class RunnerEnvUtil {

	public static final String[] PYTHON_BASIC_DEPENDENCIES = new String[] {
		"pyflink.zip",
		"py4j-0.10.8.1-src.zip",
		"cloudpickle-1.2.2-src.zip",
		"pyflink-udf-runner.sh"
	};

	public static List<File> extractBasicDependenciesFromResource(
		String tmpdir, ClassLoader classLoader, String prefix) throws IOException {
		List<File> extractedFiles = new ArrayList<>();
		for (String fileName : PYTHON_BASIC_DEPENDENCIES) {
			File file = new File(tmpdir, prefix + fileName);
			try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
				IOUtils.copyBytes(classLoader.getResourceAsStream(fileName), out);
			}
			if (file.getName().endsWith(".sh")) {
				file.setExecutable(true);
			}
			extractedFiles.add(file);
		}
		return extractedFiles;
	}

	public static Map<String, String> appendEnvironmentVariable(
		Map<String, String> systemEnv, List<String> pythonDependencies) {
		Map<String, String> result = new HashMap<>(systemEnv);

		String pythonPath = String.join(File.pathSeparator, pythonDependencies);
		if (systemEnv.get("PYTHONPATH") != null) {
			pythonPath = String.join(File.pathSeparator, pythonPath, systemEnv.get("PYTHONPATH"));
		}
		result.put("PYTHONPATH", pythonPath);

		if (systemEnv.get("FLINK_LOG_DIR") == null) {
			if (systemEnv.get("LOG_DIRS") != null) {
				// log directory of yarn mode
				result.put("FLINK_LOG_DIR", systemEnv.get("LOG_DIRS").split(File.pathSeparator)[0]);
			} else if (systemEnv.get(ConfigConstants.ENV_FLINK_LIB_DIR) != null) {
				// log directory of standalone mode
				File flinkHomeDir = new File(systemEnv.get(ConfigConstants.ENV_FLINK_LIB_DIR)).getParentFile();
				result.put("FLINK_LOG_DIR", new File(flinkHomeDir, "log").getAbsolutePath());
			} else if (systemEnv.get(ConfigConstants.ENV_FLINK_HOME_DIR) != null) {
				// log directory of pyflink shell mode or user specified flink
				result.put("FLINK_LOG_DIR",
					new File(systemEnv.get(ConfigConstants.ENV_FLINK_HOME_DIR), "log").getAbsolutePath());
			}
		}

		return result;
	}
}
