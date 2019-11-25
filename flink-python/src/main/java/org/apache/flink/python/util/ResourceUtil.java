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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for building the most basic environment for running python udf workers.
 * The basic environment does not include python part of Apache Beam.
 * Users need to prepare it themselves.
 */
public class ResourceUtil {

	public static final String[] PYTHON_BASIC_DEPENDENCIES = {
		"pyflink.zip",
		"py4j-0.10.8.1-src.zip",
		"cloudpickle-1.2.2-src.zip",
		"pyflink-udf-runner.sh"
	};

	public static final int BUFF_SIZE = 4096;

	public static List<File> extractBasicDependenciesFromResource(
			String tmpdir,
			ClassLoader classLoader,
			String prefix,
			boolean skipShellScript) throws IOException, InterruptedException {
		List<File> extractedFiles = new ArrayList<>();
		for (String fileName : PYTHON_BASIC_DEPENDENCIES) {
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
				copyBytes(
					classLoader.getResourceAsStream(fileName),
					new BufferedOutputStream(new FileOutputStream(file)));
			}
			extractedFiles.add(file);
		}
		return extractedFiles;
	}

	/**
	 * This main method is used to create the shell script in a subprocess, see the "TODO" hints in method
	 * { @link ResourceUtil#extractBasicDependenciesFromResource }.
	 * @param args First argument is the directory where shell script will be created. Second argument is the prefix of
	 *             the shell script. Third argument is the fileName of the shell script.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String tmpdir = args[0];
		String prefix = args[1];
		String fileName = args[2];
		File file = new File(tmpdir, prefix + fileName);

		copyBytes(
			ResourceUtil.class.getClassLoader().getResourceAsStream(fileName),
			new BufferedOutputStream(new FileOutputStream(file)));

		file.setExecutable(true);
	}

	/**
	 * This util class will be executed in a separated java process. So implementing this method here instead of reusing
	 * flink-core utils to minimize its dependencies, which will make the code much simple.
	 * simple.
	 * @param input The input stream.
	 * @param output The output stream.
	 * @throws IOException
	 */
	public static void copyBytes(InputStream input, OutputStream output) throws IOException {
		try (InputStream in = input; OutputStream out = output) {
			final byte[] buf = new byte[BUFF_SIZE];
			int bytesRead = in.read(buf);
			while (bytesRead >= 0) {
				out.write(buf, 0, bytesRead);
				bytesRead = in.read(buf);
			}
		}
	}
}
