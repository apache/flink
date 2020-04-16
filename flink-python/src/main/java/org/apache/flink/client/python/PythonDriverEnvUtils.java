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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.PYTHON_CLIENT_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_FILES;
import static org.apache.flink.python.util.PythonDependencyUtils.FILE_DELIMITER;

/**
 * The util class help to prepare Python env and run the python process.
 */
final class PythonDriverEnvUtils {
	private static final Logger LOG = LoggerFactory.getLogger(PythonDriverEnvUtils.class);

	static final String PYFLINK_CLIENT_EXECUTABLE = "PYFLINK_CLIENT_EXECUTABLE";

	/**
	 * Wraps Python exec environment.
	 */
	static class PythonEnvironment {
		String tempDirectory;

		String pythonExec = "python";

		String pythonPath;

		Map<String, String> systemEnv = new HashMap<>();
	}

	/**
	 * The hook thread that delete the tmp working dir of python process after the python process shutdown.
	 */
	private static class ShutDownPythonHook extends Thread {
		private Process p;
		private String pyFileDir;

		ShutDownPythonHook(Process p, String pyFileDir) {
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
	 * @param config The Python configurations.
	 * @param entryPointScript The entry point script, optional.
	 * @param tmpDir The temporary directory which files will be copied to.
	 * @return PythonEnvironment the Python environment which will be executed in Python process.
	 */
	static PythonEnvironment preparePythonEnvironment(
		Configuration config,
		String entryPointScript,
		String tmpDir) throws IOException {
		PythonEnvironment env = new PythonEnvironment();

		// 1. set the path of python interpreter.
		String pythonExec = config.getOptional(PYTHON_CLIENT_EXECUTABLE)
			.orElse(System.getenv(PYFLINK_CLIENT_EXECUTABLE));
		if (pythonExec != null) {
			env.pythonExec = pythonExec;
		}

		// 2. setup temporary local directory for the user files
		tmpDir = new File(tmpDir).getAbsolutePath();
		Path tmpDirPath = new Path(tmpDir);
		tmpDirPath.getFileSystem().mkdirs(tmpDirPath);
		env.tempDirectory = tmpDir;

		// 3. append the internal lib files to PYTHONPATH.
		if (System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR) != null) {
			String pythonLibDir = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR) + File.separator + "python";
			env.pythonPath = getLibFiles(pythonLibDir).stream()
				.map(p -> p.toFile().getAbsolutePath())
				.collect(Collectors.joining(File.pathSeparator));
		}

		// 4. copy relevant python files to tmp dir and set them in PYTHONPATH.
		if (config.getOptional(PYTHON_FILES).isPresent()) {
			List<Path> pythonFiles = Arrays.stream(config.get(PYTHON_FILES).split(FILE_DELIMITER))
				.map(Path::new).collect(Collectors.toList());
			addToPythonPath(env, pythonFiles);
		}
		if (entryPointScript != null) {
			addToPythonPath(env, Collections.singletonList(new Path(entryPointScript)));
		}
		return env;
	}

	/**
	 * Creates symbolLink in working directory for pyflink lib.
	 *
	 * @param libPath          the pyflink lib file path.
	 * @param symbolicLinkPath the symbolic link to pyflink lib.
	 */
	private static void createSymbolicLink(java.nio.file.Path libPath, java.nio.file.Path symbolicLinkPath)
			throws IOException {
		try {
			Files.createSymbolicLink(symbolicLinkPath, libPath);
		} catch (IOException e) {
			LOG.warn("Create symbol link from {} to {} failed and copy instead.", symbolicLinkPath, libPath, e);
			Files.copy(libPath, symbolicLinkPath);
		}
	}

	/**
	 * Gets pyflink dependent libs in specified directory.
	 *
	 * @param libDir The lib directory
	 */
	private static List<java.nio.file.Path> getLibFiles(String libDir) {
		final List<java.nio.file.Path> libFiles = new ArrayList<>();
		SimpleFileVisitor<java.nio.file.Path> finder = new SimpleFileVisitor<java.nio.file.Path>() {
			@Override
			public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
				// only include zip file
				if (file.toString().endsWith(".zip")) {
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

	private static void addToPythonPath(PythonEnvironment env, List<Path> pythonFiles) throws IOException {
		List<String> pythonPathList = new ArrayList<>();
		Path tmpDirPath = new Path(env.tempDirectory);

		for (Path pythonFile : pythonFiles) {
			String sourceFileName = pythonFile.getName();
			// add random UUID parent directory to avoid name conflict.
			Path targetPath = new Path(
				tmpDirPath,
				String.join(File.separator, UUID.randomUUID().toString(), sourceFileName));
			if (!pythonFile.getFileSystem().isDistributedFS()) {
				// if the path is local file, try to create symbolic link.
				new File(targetPath.getParent().toString()).mkdir();
				createSymbolicLink(
					Paths.get(new File(pythonFile.getPath()).getAbsolutePath()),
					Paths.get(targetPath.toString()));
			} else {
				try {
					FileUtils.copy(pythonFile, targetPath, true);
				} catch (Exception e) {
					LOG.error("Error occurred when copying {} to {}, skipping...", pythonFile, targetPath, e);
					continue;
				}
			}
			if (Files.isRegularFile(Paths.get(targetPath.toString()).toRealPath()) && sourceFileName.endsWith(".py")) {
				// add the parent directory of .py file itself to PYTHONPATH
				pythonPathList.add(targetPath.getParent().toString());
			} else {
				pythonPathList.add(targetPath.toString());
			}
		}

		if (env.pythonPath != null && !env.pythonPath.isEmpty()) {
			pythonPathList.add(env.pythonPath);
		}
		env.pythonPath = String.join(File.pathSeparator, pythonPathList);
	}

	/**
	 * Starts python process.
	 *
	 * @param pythonEnv the python Environment which will be in a process.
	 * @param commands  the commands that python process will execute.
	 * @return the process represent the python process.
	 * @throws IOException Thrown if an error occurred when python process start.
	 */
	static Process startPythonProcess(PythonEnvironment pythonEnv, List<String> commands) throws IOException {
		ProcessBuilder pythonProcessBuilder = new ProcessBuilder();
		Map<String, String> env = pythonProcessBuilder.environment();
		env.put("PYTHONPATH", pythonEnv.pythonPath);
		pythonEnv.systemEnv.forEach(env::put);
		commands.add(0, pythonEnv.pythonExec);
		pythonProcessBuilder.command(commands);
		// redirect the stderr to stdout
		pythonProcessBuilder.redirectErrorStream(true);
		// set the child process the output same as the parent process.
		pythonProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		Process process = pythonProcessBuilder.start();
		if (!process.isAlive()) {
			throw new RuntimeException("Failed to start Python process. ");
		}

		// Make sure that the python sub process will be killed when JVM exit
		ShutDownPythonHook hook = new ShutDownPythonHook(process, pythonEnv.tempDirectory);
		Runtime.getRuntime().addShutdownHook(hook);

		return process;
	}
}
