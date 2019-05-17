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

package org.apache.flink.client.python;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The util class help to prepare Python env and run the python process.
 */
public class PythonUtil {
	private static final Logger LOG = LoggerFactory.getLogger(PythonUtil.class);

	private static final String FLINK_OPT_DIR = System.getenv("FLINK_OPT_DIR");

	private static final String FLINK_OPT_DIR_PYTHON = FLINK_OPT_DIR + File.separator + "python";

	private static final String PYFLINK_LIB_ZIP_FILENAME = "pyflink.zip";

	private static final String PYFLINK_PY4J_FILENAME = "py4j-0.10.8.1-src.zip";

	/**
	 * Wrap Python exec environment.
	 */
	public static class PythonEnvironment {
		public String workingDirectory;

		public String pythonExec = "python";

		public String pythonPath;

		Map<String, String> systemEnv = new HashMap<>();
	}

	/**
	 * The hook thread that delete the tmp working dir of python process after the python process shutdown.
	 */
	private static class ShutDownPythonHook extends Thread {
		private Process p;
		private String pyFileDir;

		public ShutDownPythonHook(Process p, String pyFileDir) {
			this.p = p;
			this.pyFileDir = pyFileDir;
		}

		public void run() {

			p.destroyForcibly();

			if (pyFileDir != null) {
				File pyDir = new File(pyFileDir);
				FileUtils.deleteDirectoryQuietly(pyDir);
			}
		}
	}


	/**
	 * Prepare PythonEnvironment to start python process.
	 *
	 * @param filePathMap map file name to its file path.
	 * @return PythonEnvironment the Python environment which will be executed in Python process.
	 */
	public static PythonEnvironment preparePythonEnvironment(Map<String, Path> filePathMap) {
		PythonEnvironment env = new PythonEnvironment();

		// 1. setup temporary local directory for the user files
		String tmpDir = System.getProperty("java.io.tmpdir") +
			File.separator + "pyflink" + UUID.randomUUID();

		Path tmpDirPath = new Path(tmpDir);
		try {
			FileSystem fs = tmpDirPath.getFileSystem();
			if (fs.exists(tmpDirPath)) {
				fs.delete(tmpDirPath, true);
			}
			fs.mkdirs(tmpDirPath);
		} catch (IOException e) {
			LOG.error("Prepare tmp directory failed.", e);
		}

		env.workingDirectory = tmpDirPath.toString();

		StringBuilder pythonPathEnv = new StringBuilder();

		pythonPathEnv.append(env.workingDirectory);

		// 2. create symbolLink in the working directory for the pyflink dependency libs.
		final String[] libs = {PYFLINK_PY4J_FILENAME, PYFLINK_LIB_ZIP_FILENAME};
		for (String lib : libs) {
			String libFilePath = FLINK_OPT_DIR_PYTHON + File.separator + lib;
			String symbolicLinkFilePath = env.workingDirectory + File.separator + lib;
			createSymbolicLinkForPyflinkLib(libFilePath, symbolicLinkFilePath);
			pythonPathEnv.append(File.pathSeparator);
			pythonPathEnv.append(symbolicLinkFilePath);
		}

		// 3. copy relevant python files to tmp dir and set them in PYTHONPATH.
		filePathMap.forEach((sourceFileName, sourcePath) -> {
			Path targetPath = new Path(tmpDirPath, sourceFileName);
			try {
				FileUtils.copy(sourcePath, targetPath, true);
			} catch (IOException e) {
				LOG.error("Copy files to tmp dir failed", e);
			}
			String targetFileName = targetPath.toString();
			pythonPathEnv.append(File.pathSeparator);
			pythonPathEnv.append(targetFileName);

		});

		env.pythonPath = pythonPathEnv.toString();
		return env;
	}

	/**
	 * Creates symbolLink in working directory for pyflink lib.
	 *
	 * @param libFilePath          the pyflink lib file path.
	 * @param symbolicLinkFilePath the symbolic to pyflink lib.
	 */
	public static void createSymbolicLinkForPyflinkLib(String libFilePath, String symbolicLinkFilePath) {
		java.nio.file.Path libPath = FileSystems.getDefault().getPath(libFilePath);
		java.nio.file.Path symbolicLinkPath = FileSystems.getDefault().getPath(symbolicLinkFilePath);
		try {
			Files.createSymbolicLink(symbolicLinkPath, libPath);
		} catch (IOException e) {
			LOG.error("Create symbol link for pyflink lib failed.", e);
		}
	}

	/**
	 * Starts python process.
	 *
	 * @param pythonEnv the python Environment which will be in a process.
	 * @param commands  the commands that python process will execute.
	 * @return the process represent the python process.
	 * @throws IOException Thrown if an error occurred when python process start.
	 */
	public static Process startPythonProcess(PythonEnvironment pythonEnv, List<String> commands) throws IOException {
		ProcessBuilder pythonProcessBuilder = new ProcessBuilder();
		Map<String, String> env = pythonProcessBuilder.environment();
		env.put("PYTHONPATH", pythonEnv.pythonPath);
		pythonEnv.systemEnv.forEach(env::put);
		commands.add(0, pythonEnv.pythonExec);
		pythonProcessBuilder.command(commands);
		// set the working directory.
		pythonProcessBuilder.directory(new File(pythonEnv.workingDirectory));
		// redirect the stderr to stdout
		pythonProcessBuilder.redirectErrorStream(true);
		// set the child process the output same as the parent process.
		pythonProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		Process process = pythonProcessBuilder.start();
		if (!process.isAlive()) {
			throw new RuntimeException("Failed to start Python process. ");
		}

		// Make sure that the python sub process will be killed when JVM exit
		ShutDownPythonHook hook = new ShutDownPythonHook(process, pythonEnv.workingDirectory);
		Runtime.getRuntime().addShutdownHook(hook);

		return process;
	}
}
