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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FileSystem;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The util class help to prepare Python env and run the python process.
 */
public final class PythonDriverEnvUtils {
	private static final Logger LOG = LoggerFactory.getLogger(PythonDriverEnvUtils.class);

	private static final String FLINK_OPT_DIR = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR);

	private static final String FLINK_OPT_DIR_PYTHON = FLINK_OPT_DIR + File.separator + "python";

	@VisibleForTesting
	public static final String PYFLINK_PY_FILES = "PYFLINK_PY_FILES";

	@VisibleForTesting
	public static final String PYFLINK_PY_REQUIREMENTS = "PYFLINK_PY_REQUIREMENTS";

	@VisibleForTesting
	public static final String PYFLINK_PY_EXECUTABLE = "PYFLINK_PY_EXECUTABLE";

	@VisibleForTesting
	public static final String PYFLINK_PY_ARCHIVES = "PYFLINK_PY_ARCHIVES";

	/**
	 * Wraps Python exec environment.
	 */
	public static class PythonEnvironment {
		public String tempDirectory;

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
	 * @param pythonDriverOptions The Python driver options.
	 * @param tmpDir The temporary directory which files will be copied to.
	 * @return PythonEnvironment the Python environment which will be executed in Python process.
	 */
	public static PythonEnvironment preparePythonEnvironment(
			PythonDriverOptions pythonDriverOptions,
			String tmpDir) throws IOException, InterruptedException {
		PythonEnvironment env = new PythonEnvironment();

		tmpDir = new File(tmpDir).getAbsolutePath();

		// 1. setup temporary local directory for the user files
		Path tmpDirPath = new Path(tmpDir);
		FileSystem fs = tmpDirPath.getFileSystem();
		fs.mkdirs(tmpDirPath);

		env.tempDirectory = tmpDir;
		List<String> pythonPathList = new ArrayList<>();

		// 2. append the internal lib files to PYTHONPATH.
		List<java.nio.file.Path> pythonLibs = getLibFiles(FLINK_OPT_DIR_PYTHON);
		for (java.nio.file.Path lib: pythonLibs) {
			pythonPathList.add(lib.toFile().getAbsolutePath());
		}

		// 3. copy relevant python files to tmp dir and set them in PYTHONPATH.
		for (Path pythonFile : pythonDriverOptions.getPythonLibFiles()) {
			String sourceFileName = pythonFile.getName();
			// add random UUID parent directory to avoid name conflict.
			Path targetPath = new Path(
				tmpDirPath,
				String.join(File.separator, UUID.randomUUID().toString(), sourceFileName));
			if (!pythonFile.getFileSystem().isDistributedFS()) {
				// if the path is local file, try to create symbolic link.
				new File(targetPath.getParent().toString()).mkdir();
				createSymbolicLinkForPyflinkLib(
					Paths.get(new File(pythonFile.getPath()).getAbsolutePath()),
					Paths.get(targetPath.toString()));
			} else {
				FileUtils.copy(pythonFile, targetPath, true);
			}
			if (Files.isRegularFile(Paths.get(targetPath.toString()).toRealPath()) && sourceFileName.endsWith(".py")) {
				// add the parent directory of .py file itself to PYTHONPATH
				pythonPathList.add(targetPath.getParent().toString());
			} else {
				pythonPathList.add(targetPath.toString());
			}
		}

		env.pythonPath = String.join(File.pathSeparator, pythonPathList);

		if (!pythonDriverOptions.getPyFiles().isEmpty()) {
			env.systemEnv.put(PYFLINK_PY_FILES, String.join("\n", pythonDriverOptions.getPyFiles()));
		}
		if (!pythonDriverOptions.getPyArchives().isEmpty()) {
			env.systemEnv.put(
				PYFLINK_PY_ARCHIVES,
				joinTuples(pythonDriverOptions.getPyArchives()));
		}
		pythonDriverOptions.getPyRequirements().ifPresent(
			pyRequirements -> env.systemEnv.put(
				PYFLINK_PY_REQUIREMENTS,
				joinTuples(Collections.singleton(pyRequirements))));
		pythonDriverOptions.getPyExecutable().ifPresent(
			pyExecutable -> env.systemEnv.put(PYFLINK_PY_EXECUTABLE, pythonDriverOptions.getPyExecutable().get()));
		return env;
	}

	private static String joinTuples(Collection<Tuple2<String, String>> tuples) {
		List<String> joinedTuples = new ArrayList<>();
		for (Tuple2<String, String> tuple : tuples) {
			String f0 = tuple.f0 == null ? "" : tuple.f0;
			String f1 = tuple.f1 == null ? "" : tuple.f1;

			joinedTuples.add(String.join("\n", f0, f1));
		}
		return String.join("\n", joinedTuples);
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
	 * Gets pyflink dependent libs in specified directory.
	 *
	 * @param libDir The lib directory
	 */
	public static List<java.nio.file.Path> getLibFiles(String libDir) {
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
