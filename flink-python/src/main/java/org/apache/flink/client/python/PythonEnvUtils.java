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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The util class help to prepare Python env and run the python process.
 */
public final class PythonEnvUtils {
	private static final Logger LOG = LoggerFactory.getLogger(PythonEnvUtils.class);

	private static final String FLINK_OPT_DIR = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR);

	private static final String FLINK_OPT_DIR_PYTHON = FLINK_OPT_DIR + File.separator + "python";

	/**
	 * Wraps Python exec environment.
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
	 * Prepares PythonEnvironment to start python process.
	 *
	 * @param pythonLibFiles The dependent Python files.
	 * @return PythonEnvironment the Python environment which will be executed in Python process.
	 */
	public static PythonEnvironment preparePythonEnvironment(List<Path> pythonLibFiles) throws IOException {
		PythonEnvironment env = new PythonEnvironment();

		// 1. setup temporary local directory for the user files
		String tmpDir = System.getProperty("java.io.tmpdir") +
			File.separator + "pyflink" + File.separator + UUID.randomUUID();

		Path tmpDirPath = new Path(tmpDir);
		FileSystem fs = tmpDirPath.getFileSystem();
		if (fs.exists(tmpDirPath)) {
			fs.delete(tmpDirPath, true);
		}
		fs.mkdirs(tmpDirPath);

		env.workingDirectory = tmpDirPath.toString();

		StringBuilder pythonPathEnv = new StringBuilder();

		pythonPathEnv.append(env.workingDirectory);

		// 2. create symbolLink in the working directory for the pyflink dependency libs.
		List<java.nio.file.Path> pythonLibs = getLibFiles(FLINK_OPT_DIR_PYTHON);
		for (java.nio.file.Path libPath : pythonLibs) {
			java.nio.file.Path symbolicLinkFilePath = FileSystems.getDefault().getPath(env.workingDirectory,
				libPath.getFileName().toString());
			createSymbolicLinkForPyflinkLib(libPath, symbolicLinkFilePath);
			pythonPathEnv.append(File.pathSeparator);
			pythonPathEnv.append(symbolicLinkFilePath.toString());
		}

		// 3. copy relevant python files to tmp dir and set them in PYTHONPATH.
		for (Path pythonFile : pythonLibFiles) {
			String sourceFileName = pythonFile.getName();
			Path targetPath = new Path(tmpDirPath, sourceFileName);
			FileUtils.copy(pythonFile, targetPath, true);
			String targetFileNames = Files.walk(Paths.get(targetPath.toString()))
				.filter(Files::isRegularFile)
				.filter(f -> !f.toString().endsWith(".py"))
				.map(java.nio.file.Path::toString)
				.collect(Collectors.joining(File.pathSeparator));
			pythonPathEnv.append(File.pathSeparator);
			pythonPathEnv.append(targetFileNames);
		}

		// 4. add the parent directory to PYTHONPATH for files suffixed with .py
		String pyFileParents = Files.walk(Paths.get(tmpDirPath.toString()))
			.filter(file -> file.toString().endsWith(".py"))
			.map(java.nio.file.Path::getParent)
			.distinct()
			.map(java.nio.file.Path::toString)
			.collect(Collectors.joining(File.pathSeparator));
		if (!StringUtils.isNullOrWhitespaceOnly(pyFileParents)) {
			pythonPathEnv.append(File.pathSeparator);
			pythonPathEnv.append(pyFileParents);
		}

		env.pythonPath = pythonPathEnv.toString();
		return env;
	}

	/**
	 * Gets pyflink dependent libs in specified directory.
	 *
	 * @param libDir The lib directory
	 */
	public static List<java.nio.file.Path> getLibFiles(String libDir) {
		final List<java.nio.file.Path> libFiles = new ArrayList<>();
		SimpleFileVisitor<java.nio.file.Path> finder = new SimpleFileVisitor<java.nio.file.Path>() {
			@Override
			public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
				// exclude .txt file
				if (!file.toString().endsWith(".txt")) {
					libFiles.add(file);
				}
				return FileVisitResult.CONTINUE;
			}
		};
		try {
			Files.walkFileTree(FileSystems.getDefault().getPath(libDir), finder);
		} catch (IOException e) {
			LOG.error("Gets pyflink dependent libs failed.", e);
		}
		return libFiles;
	}

	/**
	 * Creates symbolLink in working directory for pyflink lib.
	 *
	 * @param libPath          the pyflink lib file path.
	 * @param symbolicLinkPath the symbolic link to pyflink lib.
	 */
	public static void createSymbolicLinkForPyflinkLib(java.nio.file.Path libPath, java.nio.file.Path symbolicLinkPath)
			throws IOException {
		try {
			Files.createSymbolicLink(symbolicLinkPath, libPath);
		} catch (IOException e) {
			LOG.error("Create symbol link for pyflink lib failed.", e);
			LOG.info("Try to copy pyflink lib to working directory");
			Files.copy(libPath, symbolicLinkPath);
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
