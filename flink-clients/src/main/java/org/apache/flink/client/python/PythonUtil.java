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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The util class help to prepare python env and run the python process.
 */
public class PythonUtil {
	private static final Logger LOG = LoggerFactory.getLogger(PythonUtil.class);

	private static final String FLINK_OPT_DIR = System.getenv("FLINK_OPT_DIR");

	private static final String FLINK_OPT_DIR_PYTHON = FLINK_OPT_DIR + File.separator + "python";

	private static final String PYFLINK_LIB_ZIP_FILENAME = "pyflink.zip";

	private static final String PYFLINK_PY4J_FILENAME = "py4j-0.10.8.1-src.zip";

	/**
	 * Wrap python exec environment.
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
	 * @param filePathMap
	 * @return PythonEnvironment
	 * @throws IOException
	 */
	public static PythonEnvironment preparePythonEnvironment(Map<String, Path> filePathMap) throws IOException {
		PythonEnvironment env = new PythonEnvironment();

		// 1. setup temporary local directory for the user files
		String tmpDir = System.getProperty("java.io.tmpdir") +
			File.separator + "pyflink_tmp_" + UUID.randomUUID();

		Path tmpDirPath = new Path(tmpDir);
		FileSystem fs = tmpDirPath.getFileSystem();
		if (fs.exists(tmpDirPath)) {
			fs.delete(tmpDirPath, true);
		}
		fs.mkdirs(tmpDirPath);

		env.workingDirectory = tmpDirPath.toString();

		StringBuilder pythonPathEnv = new StringBuilder();

		pythonPathEnv.append(env.workingDirectory);

		// 2. copy flink python libraries to tmp dir and set them in PYTHONPATH.
		final String[] libs = {PYFLINK_PY4J_FILENAME, PYFLINK_LIB_ZIP_FILENAME};
		for (String lib : libs) {
			String sourceFilePath = FLINK_OPT_DIR_PYTHON + File.separator + lib;
			String targetFilePath = env.workingDirectory + File.separator + lib;
			copyPyflinkLibToTarget(sourceFilePath, targetFilePath);
			pythonPathEnv.append(File.pathSeparator);
			pythonPathEnv.append(targetFilePath);
		}

		// 3. copy relevant python files to tmp dir and set them in PYTHONPATH.
		filePathMap.forEach((sourceFileName, sourcePath) -> {
			Path targetPath = new Path(tmpDirPath, sourceFileName);
			try {
				PythonUtil.copy(sourcePath, targetPath);
			} catch (IOException e) {
				LOG.error("copy the file {} to tmp dir failed", sourceFileName);
			}

			String targetFileName = targetPath.toString();
			if (isNeedAddPath(targetFileName, libs)) {
				pythonPathEnv.append(File.pathSeparator);
				pythonPathEnv.append(targetFileName);
			}
		});

		env.pythonPath = pythonPathEnv.toString();
		return env;
	}

	/**
	 * Is need to add the path to PYTHONPATH.
	 *
	 * @param fileName
	 * @param libs
	 * @return
	 */
	private static boolean isNeedAddPath(String fileName, String[] libs) {
		if (fileName.endsWith(".zip")) {
			for (String lib : libs) {
				if (fileName.endsWith(lib)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Copy local pyflink lib to tmp file.
	 *
	 * @param sourceFilePath
	 * @param targetFilePath
	 * @throws IOException
	 */
	public static void copyPyflinkLibToTarget(String sourceFilePath, String targetFilePath) throws IOException {
		InputStream in = new FileInputStream(new File(sourceFilePath));
		if (in == null) {
			String err = "Can't extract python library files from the lib " + sourceFilePath;
			throw new IOException(err);
		}
		File targetFile = new File(targetFilePath);
		java.nio.file.Files.copy(
			in,
			targetFile.toPath(),
			StandardCopyOption.REPLACE_EXISTING);

		IOUtils.closeQuietly(in);

	}

	/**
	 * copy sourcePath to targetPath.
	 *
	 * @param sourcePath source Path
	 * @param targetPath target Path
	 * @throws IOException
	 */
	public static void copy(Path sourcePath, Path targetPath) throws IOException {
		// we unwrap the file system to get raw streams without safety net
		FileSystem sFs = FileSystem.getUnguardedFileSystem(sourcePath.toUri());
		FileSystem tFs = FileSystem.getUnguardedFileSystem(targetPath.toUri());
		if (!tFs.exists(targetPath)) {
			if (sFs.getFileStatus(sourcePath).isDir()) {
				tFs.mkdirs(targetPath);
				FileStatus[] contents = sFs.listStatus(sourcePath);
				for (FileStatus content : contents) {
					String distPath = content.getPath().toString();
					if (content.isDir() && distPath.endsWith("/")) {
						distPath = distPath.substring(0, distPath.length() - 1);
					}
					String targetSubPath = targetPath.toString() + distPath.substring(distPath.lastIndexOf("/"));
					//recursive copy sourcePath to targetPath
					copy(content.getPath(), new Path(targetSubPath));
				}
			} else {
				try (FSDataOutputStream lfsOutput = tFs.create(targetPath, FileSystem.WriteMode.NO_OVERWRITE); FSDataInputStream fsInput = sFs.open(sourcePath)) {
					IOUtils.copyBytes(fsInput, lfsOutput);
				} catch (IOException ioe) {
					LOG.error("could not copy file to target file cache.", ioe);
				}
			}
		}
	}

	/**
	 * start python process.
	 *
	 * @param pythonEnv
	 * @param commands
	 * @return
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
