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

package org.apache.flink.streaming.python.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.python.PythonOptions;
import org.apache.flink.streaming.python.api.environment.PythonEnvironmentFactory;
import org.apache.flink.streaming.python.util.InterpreterUtils;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

/**
 * Allows the execution of Flink stream plan that is written in Python.
 */
public class PythonStreamBinder {
	private static final Logger LOG = LoggerFactory.getLogger(PythonStreamBinder.class);

	private final String localTmpPath;
	private Path tmpDistributedDir;

	PythonStreamBinder(Configuration globalConfig) {
		String configuredLocalTmpPath = globalConfig.getString(PythonOptions.PLAN_TMP_DIR);
		this.localTmpPath = configuredLocalTmpPath != null
			? configuredLocalTmpPath
			: System.getProperty("java.io.tmpdir") + File.separator + "flink_streaming_plan_" + UUID.randomUUID();

		this.tmpDistributedDir = new Path(globalConfig.getString(PythonOptions.DC_TMP_DIR));
	}

	/**
	 * Entry point for the execution of a python streaming task.
	 *
	 * @param args pathToScript [pathToPackage1 .. [pathToPackageX]] - [parameter1]..[parameterX]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration globalConfig = GlobalConfiguration.loadConfiguration();
		PythonStreamBinder binder = new PythonStreamBinder(globalConfig);
		try {
			binder.runPlan(args);
		} catch (Exception e) {
			System.out.println("Failed to run plan: " + e.getMessage());
			e.printStackTrace();
			LOG.error("Failed to run plan.", e);
		}
	}

	void runPlan(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: prog <pathToScript> [parameter1]..[parameterX] - [<pathToPackage1> .. [<pathToPackageX]]");
			return;
		}

		int split = 0;
		for (int x = 0; x < args.length; x++) {
			if (args[x].equals("-")) {
				split = x;
				break;
			}
		}

		try {
			String planFile = args[0];
			String[] filesToCopy = Arrays.copyOfRange(args, 1, split == 0 ? args.length : split);
			String[] planArgumentsArray = Arrays.copyOfRange(args, split == 0 ? args.length : split + 1, args.length);

			// verify existence of files
			Path planPath = new Path(planFile);
			if (!FileSystem.getUnguardedFileSystem(planPath.toUri()).exists(planPath)) {
				throw new FileNotFoundException("Plan file " + planFile + " does not exist.");
			}
			for (String file : filesToCopy) {
				Path filePath = new Path(file);
				if (!FileSystem.getUnguardedFileSystem(filePath.toUri()).exists(filePath)) {
					throw new FileNotFoundException("Additional file " + file + " does not exist.");
				}
			}

			// setup temporary local directory for flink python library and user files
			Path targetDir = new Path(localTmpPath);
			deleteIfExists(targetDir);
			targetDir.getFileSystem().mkdirs(targetDir);

			// copy user files to temporary location
			copyFile(planPath, targetDir, planPath.getName());
			for (String file : filesToCopy) {
				Path source = new Path(file);
				copyFile(source, targetDir, source.getName());
			}

			String planNameWithExtension = planPath.getName();
			String planName = planNameWithExtension.substring(0, planNameWithExtension.indexOf(".py"));

			InterpreterUtils.initAndExecPythonScript(new PythonEnvironmentFactory(localTmpPath, planName), Paths.get(localTmpPath), planName, planArgumentsArray);
		} finally {
			try {
				// clean up created files
				FileSystem local = FileSystem.getLocalFileSystem();
				local.delete(new Path(localTmpPath), true);
			} catch (IOException ioe) {
				LOG.error("PythonAPI file cleanup failed. {}", ioe.getMessage());
			}
		}
	}

	//=====File utils===================================================================================================

	private static void deleteIfExists(Path path) throws IOException {
		FileSystem fs = path.getFileSystem();
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	private static void copyFile(Path source, Path targetDirectory, String name) throws IOException {
		Path targetFilePath = new Path(targetDirectory, name);
		deleteIfExists(targetFilePath);
		FileUtils.copy(source, targetFilePath, true);
	}

}
