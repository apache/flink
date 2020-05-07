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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.python.util.ResourceUtil;
import org.apache.flink.python.util.ZipUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.codehaus.commons.nullanalysis.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

/**
 * The ProcessPythonEnvironmentManager is used to prepare the working dir of python UDF worker and create
 * ProcessEnvironment object of Beam Fn API. It's used when the python function runner is configured to run python UDF
 * in process mode.
 */
@Internal
public final class ProcessPythonEnvironmentManager implements PythonEnvironmentManager {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessPythonEnvironmentManager.class);

	@VisibleForTesting
	static final String PYFLINK_GATEWAY_DISABLED = "PYFLINK_GATEWAY_DISABLED";
	@VisibleForTesting
	public static final String PYTHON_REQUIREMENTS_FILE = "_PYTHON_REQUIREMENTS_FILE";
	@VisibleForTesting
	public static final String PYTHON_REQUIREMENTS_CACHE = "_PYTHON_REQUIREMENTS_CACHE";
	@VisibleForTesting
	public static final String PYTHON_REQUIREMENTS_INSTALL_DIR = "_PYTHON_REQUIREMENTS_INSTALL_DIR";
	@VisibleForTesting
	public static final String PYTHON_WORKING_DIR = "_PYTHON_WORKING_DIR";

	@VisibleForTesting
	static final String PYTHON_REQUIREMENTS_DIR = "python-requirements";
	@VisibleForTesting
	static final String PYTHON_ARCHIVES_DIR = "python-archives";
	@VisibleForTesting
	static final String PYTHON_FILES_DIR = "python-files";

	private static final long CHECK_INTERVAL = 20;
	private static final long CHECK_TIMEOUT = 1000;

	private transient String baseDirectory;

	/**
	 * Directory for storing the installation result of the requirements file.
	 */
	private transient String requirementsDirectory;

	/**
	 * Directory for storing the extracted result of the archive files.
	 */
	private transient String archivesDirectory;

	/**
	 * Directory for storing the uploaded python files.
	 */
	private transient String filesDirectory;

	private transient Thread shutdownHook;

	@NotNull private final PythonDependencyInfo dependencyInfo;
	@NotNull private final Map<String, String> systemEnv;
	@NotNull private final String[] tmpDirectories;

	public ProcessPythonEnvironmentManager(
		@NotNull PythonDependencyInfo dependencyInfo,
		@NotNull String[] tmpDirectories,
		@NotNull Map<String, String> systemEnv) {
		this.dependencyInfo = Objects.requireNonNull(dependencyInfo);
		this.tmpDirectories = Objects.requireNonNull(tmpDirectories);
		this.systemEnv = Objects.requireNonNull(systemEnv);
	}

	@Override
	public void open() throws Exception {
		baseDirectory = createBaseDirectory(tmpDirectories);
		archivesDirectory = String.join(File.separator, baseDirectory, PYTHON_ARCHIVES_DIR);
		requirementsDirectory = String.join(File.separator, baseDirectory, PYTHON_REQUIREMENTS_DIR);
		filesDirectory = String.join(File.separator, baseDirectory, PYTHON_FILES_DIR);

		File baseDirectoryFile = new File(baseDirectory);
		if (!baseDirectoryFile.exists() && !baseDirectoryFile.mkdir()) {
			throw new IOException(
				"Could not create the base directory: " + baseDirectory);
		}
		shutdownHook = ShutdownHookUtil.addShutdownHook(
			this, ProcessPythonEnvironmentManager.class.getSimpleName(), LOG);
	}

	@Override
	public void close() throws Exception {
		try {
			int retries = 0;
			while (true) {
				try {
					FileUtils.deleteDirectory(new File(baseDirectory));
					break;
				} catch (Throwable t) {
					retries++;
					if (retries <= CHECK_TIMEOUT / CHECK_INTERVAL) {
						LOG.warn(
							String.format(
								"Failed to delete the working directory %s of the Python UDF worker. Retrying...",
								baseDirectory),
							t);
					} else {
						LOG.warn(
							String.format(
								"Failed to delete the working directory %s of the Python UDF worker.", baseDirectory),
							t);
						break;
					}
				}
			}
		} finally {
			if (shutdownHook != null) {
				ShutdownHookUtil.removeShutdownHook(
					shutdownHook, ProcessPythonEnvironmentManager.class.getSimpleName(), LOG);
				shutdownHook = null;
			}
		}
	}

	@Override
	public RunnerApi.Environment createEnvironment() throws IOException, InterruptedException {
		Map<String, String> env = constructEnvironmentVariables();
		File runnerScript = ResourceUtil.extractUdfRunner(baseDirectory);

		return Environments.createProcessEnvironment(
			"",
			"",
			runnerScript.getPath(),
			env);
	}

	/**
	 * Returns an empty RetrievalToken because no files will be transmit via ArtifactService in process mode.
	 *
	 * @return The path of empty RetrievalToken.
	 */
	@Override
	public String createRetrievalToken() throws IOException {
		File retrievalToken = new File(baseDirectory,
			"retrieval_token_" + UUID.randomUUID().toString() + ".json");
		if (retrievalToken.createNewFile()) {
			final DataOutputStream dos = new DataOutputStream(new FileOutputStream(retrievalToken));
			dos.writeBytes("{\"manifest\": {}}");
			dos.flush();
			dos.close();
			return retrievalToken.getAbsolutePath();
		} else {
			throw new IOException(
				"Could not create the RetrievalToken file: " + retrievalToken.getAbsolutePath());
		}
	}

	/**
	 * Constructs the environment variables which is used to launch the python UDF worker.
	 *
	 * <p>To avoid unnecessary IO, the artifacts will not be transmitted via the ArtifactService of Beam when running in
	 * process mode. Instead, the paths of the artifacts will be passed to the Python UDF worker directly.
	 *
	 * @return The environment variables which contain the paths of the python dependencies.
	 */
	@VisibleForTesting
	Map<String, String> constructEnvironmentVariables()
			throws IOException, IllegalArgumentException, InterruptedException {
		Map<String, String> env = new HashMap<>(this.systemEnv);

		constructFilesDirectory(env);

		constructArchivesDirectory(env);

		constructRequirementsDirectory(env);

		// set BOOT_LOG_DIR.
		env.put("BOOT_LOG_DIR", baseDirectory);

		// disable the launching of gateway server to prevent from this dead loop:
		// launch UDF worker -> import udf -> import job code
		//        ^                                    | (If the job code is not enclosed in a
		//        									   |  if name == 'main' statement)
		//        |                                    V
		// execute job in local mode <- launch gateway server and submit job to local executor
		env.put(PYFLINK_GATEWAY_DISABLED, "true");

		// set the path of python interpreter, it will be used to execute the udf worker.
		env.put("python", dependencyInfo.getPythonExec());
		LOG.info("Python interpreter path: {}", dependencyInfo.getPythonExec());
		return env;
	}

	private void constructFilesDirectory(Map<String, String> env) throws IOException {
		// link or copy python files to filesDirectory and add them to PYTHONPATH
		List<String> pythonFilePaths = new ArrayList<>();
		for (Map.Entry<String, String> entry : dependencyInfo.getPythonFiles().entrySet()) {
			// The origin file name will be wiped when downloaded from the distributed cache, restore the origin name to
			// make sure the python files could be imported.
			// The path of the restored python file will be as following:
			// ${baseDirectory}/${PYTHON_FILES_DIR}/${distributedCacheFileName}/${originFileName}
			String distributedCacheFileName = new File(entry.getKey()).getName();
			String originFileName = entry.getValue();

			Path target = FileSystems.getDefault().getPath(filesDirectory, distributedCacheFileName, originFileName);
			if (!target.getParent().toFile().mkdirs()) {
				throw new IOException(
					String.format("Could not create the directory: %s !", target.getParent().toString()));
			}
			Path src = FileSystems.getDefault().getPath(entry.getKey());
			try {
				Files.createSymbolicLink(target, src);
			} catch (IOException e) {
				LOG.warn(String.format(
					"Could not create the symbolic link of: %s, the link path is %s, fallback to copy.", src, target),
					e);
				FileUtils.copy(
					new org.apache.flink.core.fs.Path(src.toUri()),
					new org.apache.flink.core.fs.Path(target.toUri()), false);
			}

			File pythonFile = new File(entry.getKey());
			String pythonPath;
			if (pythonFile.isFile() && originFileName.endsWith(".py")) {
				// If the python file is file with suffix .py, add the parent directory to PYTHONPATH.
				pythonPath = String.join(File.separator, filesDirectory, distributedCacheFileName);
			} else {
				pythonPath = String.join(File.separator, filesDirectory, distributedCacheFileName, originFileName);
			}
			pythonFilePaths.add(pythonPath);
		}
		appendToPythonPath(env, pythonFilePaths);
		LOG.info("PYTHONPATH of python worker: {}", env.get("PYTHONPATH"));
	}

	private void constructArchivesDirectory(Map<String, String> env) throws IOException {
		if (!dependencyInfo.getArchives().isEmpty()) {
			// set the archives directory as the working directory, then user could access the content of the archives
			// via relative path
			env.put(PYTHON_WORKING_DIR, archivesDirectory);
			LOG.info("Python working dir of python worker: {}", archivesDirectory);

			// extract archives to archives directory
			for (Map.Entry<String, String> entry : dependencyInfo.getArchives().entrySet()) {
				ZipUtil.extractZipFileWithPermissions(
					entry.getKey(), String.join(File.separator, archivesDirectory, entry.getValue()));
			}
		}
	}

	private void constructRequirementsDirectory(Map<String, String> env) throws IOException {
		// set the requirements file and the dependencies specified by the requirements file will be installed in
		// boot.py during initialization
		if (dependencyInfo.getRequirementsFilePath().isPresent()) {
			File requirementsDirectoryFile = new File(requirementsDirectory);
			if (!requirementsDirectoryFile.mkdirs()) {
				throw new IOException(
					String.format("Creating the requirements target directory: %s failed!", requirementsDirectory));
			}

			env.put(PYTHON_REQUIREMENTS_FILE, dependencyInfo.getRequirementsFilePath().get());
			LOG.info("Requirements.txt of python worker: {}", dependencyInfo.getRequirementsFilePath().get());

			if (dependencyInfo.getRequirementsCacheDir().isPresent()) {
				env.put(PYTHON_REQUIREMENTS_CACHE, dependencyInfo.getRequirementsCacheDir().get());
				LOG.info("Requirements cache dir of python worker: {}", dependencyInfo.getRequirementsCacheDir().get());
			}

			// the dependencies specified by the requirements file will be installed into this directory, and will be
			// added to PYTHONPATH in boot.py
			env.put(PYTHON_REQUIREMENTS_INSTALL_DIR, requirementsDirectory);
			LOG.info("Requirements install directory of python worker: {}", requirementsDirectory);
		}
	}

	@VisibleForTesting
	String getBaseDirectory() {
		return baseDirectory;
	}

	@Override
	public String getBootLog() throws Exception {
		File bootLogFile = new File(baseDirectory + File.separator + "flink-python-udf-boot.log");
		String msg = "Failed to create stage bundle factory!";
		if (bootLogFile.exists()) {
			byte[] output = Files.readAllBytes(bootLogFile.toPath());
			msg += String.format(" %s", new String(output, Charset.defaultCharset()));
		}
		return msg;
	}

	private static void appendToPythonPath(Map<String, String> env, List<String> pythonDependencies) {
		if (pythonDependencies.isEmpty()) {
			return;
		}

		String pythonDependencyPath = String.join(File.pathSeparator, pythonDependencies);
		String pythonPath = env.get("PYTHONPATH");
		if (Strings.isNullOrEmpty(pythonPath)) {
			env.put("PYTHONPATH", pythonDependencyPath);
		} else {
			env.put("PYTHONPATH", String.join(File.pathSeparator, pythonDependencyPath, pythonPath));
		}
	}

	private static String createBaseDirectory(String[] tmpDirectories) throws IOException {
		Random rnd = new Random();
		// try to find a unique file name for the base directory
		int maxAttempts = 10;
		for (int attempt = 0; attempt < maxAttempts; attempt++) {
			String directory = tmpDirectories[rnd.nextInt(tmpDirectories.length)];
			File baseDirectory = new File(directory, "python-dist-" + UUID.randomUUID().toString());
			if (baseDirectory.mkdirs()) {
				return baseDirectory.getAbsolutePath();
			}
		}

		throw new IOException(
			"Could not find a unique directory name in '" + Arrays.toString(tmpDirectories) +
				"' for storing the generated files of python dependency.");
	}
}
