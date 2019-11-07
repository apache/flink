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
			String prefix) throws IOException {
		List<File> extractedFiles = new ArrayList<>();
		for (String fileName : PYTHON_BASIC_DEPENDENCIES) {
			File file = new File(tmpdir, prefix + fileName);

			try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
				InputStream in = classLoader.getResourceAsStream(fileName)) {
				// This util will use in the extract program before launching pyflink shell,
				// so it must do this itself to minimize the dependencies.
				final byte[] buf = new byte[BUFF_SIZE];
				int bytesRead = in.read(buf);
				while (bytesRead >= 0) {
					out.write(buf, 0, bytesRead);
					bytesRead = in.read(buf);
				}
			}

			if (file.getName().endsWith(".sh")) {
				file.setExecutable(true);
			}
			extractedFiles.add(file);
		}
		return extractedFiles;
	}
}
