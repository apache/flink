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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for building the most basic environment for running python udf workers. The basic environment does not include
 * python part of Apache Beam. Users need to prepare it themselves.
 */
public class ResourceUtil {

	public static final String[] BUILT_IN_PYTHON_DEPENDENCIES = {
		"pyflink.zip",
		"py4j-0.10.8.1-src.zip",
		"cloudpickle-1.2.2-src.zip",
		"pyflink-udf-runner.sh"
	};

	public static List<File> extractBuiltInDependencies(
			String tmpdir,
			String prefix,
			boolean skipShellScript) throws IOException, InterruptedException {
		List<File> extractedFiles = new ArrayList<>();
		for (String fileName : BUILT_IN_PYTHON_DEPENDENCIES) {
			if (skipShellScript && fileName.endsWith(".sh")) {
				continue;
			}

			File file = new File(tmpdir, prefix + fileName);
			if (fileName.endsWith(".sh")) {
				// TODO: This is a hacky solution to prevent subprocesses to hold the file descriptor of shell scripts,
				// which will cause the execution of shell scripts failed with the exception "test file is busy"
				// randomly. It's a bug of JDK, see https://bugs.openjdk.java.net/browse/JDK-8068370. After moving flink
				// python jar to lib directory, we can solve this problem elegantly by extracting these files only once.
				String javaExecutable = String.join(File.separator, System.getProperty("java.home"), "bin", "java");
				String classPath = new File(
					ResourceUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getAbsolutePath();
				new ProcessBuilder(
					javaExecutable,
					"-cp",
					classPath,
					ResourceUtil.class.getName(),
					tmpdir,
					prefix,
					fileName).inheritIO().start().waitFor();
			} else {
				Files.copy(
					ResourceUtil.class.getClassLoader().getResourceAsStream(fileName),
					Paths.get(file.getAbsolutePath()));
			}
			extractedFiles.add(file);
		}
		return extractedFiles;
	}

	/**
	 * This main method is used to create the shell script in a subprocess, see the "TODO" hints in method
	 * {@link ResourceUtil#extractBuiltInDependencies}.
	 * @param args First argument is the directory where shell script will be created. Second argument is the prefix of
	 *             the shell script. Third argument is the fileName of the shell script.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String tmpdir = args[0];
		String prefix = args[1];
		String fileName = args[2];
		File file = new File(tmpdir, prefix + fileName);

		Files.copy(
			ResourceUtil.class.getClassLoader().getResourceAsStream(fileName), Paths.get(file.getAbsolutePath()));

		file.setExecutable(true);
	}
}
