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

package org.apache.flink.tests.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Wrap the <flink-dist-dir>/bin/sql-client.sh as the FlinkSQLClient so that we can submit the SQL job in the end-to-end
 * test.
 */
public class FlinkSQLClient {

	private final List<String> jars = new ArrayList<>();
	private final Path bin;
	private boolean embedded = true;
	private String defaultEnv = null;
	private String sessionEnv = null;

	public FlinkSQLClient(Path bin) {
		this.bin = bin;
	}

	public FlinkSQLClient embedded(boolean embedded) {
		this.embedded = embedded;
		return this;
	}

	public FlinkSQLClient addJAR(String jar) {
		jars.add(jar);
		return this;
	}

	public FlinkSQLClient addJAR(Path jar) {
		jars.add(jar.toAbsolutePath().toString());
		return this;
	}

	public AutoClosableProcess.AutoClosableProcessBuilder createProcess(String sql) throws IOException {
		List<String> commands = new ArrayList<>();
		commands.add(bin.resolve("sql-client.sh").toAbsolutePath().toString());
		if (embedded) {
			commands.add("embedded");
		}
		if (defaultEnv != null) {
			commands.add("--defaults");
			commands.add(defaultEnv);
		}
		if (sessionEnv != null) {
			commands.add("--environment");
			commands.add(sessionEnv);
		}
		for (String jar : jars) {
			commands.add("--jar");
			commands.add(jar);
		}
		commands.add("--update");
		commands.add("\"" + sql + "\"");
		return AutoClosableProcess.create(commands.toArray(new String[0]));
	}

	public FlinkSQLClient defaultEnvironmentFile(String propertiesFile) {
		this.defaultEnv = propertiesFile;
		return this;
	}

	public FlinkSQLClient sessionEnvironmentFile(String propertiesFile) {
		this.sessionEnv = propertiesFile;
		return this;
	}

	/**
	 * Search the jar files under the given end-to-end directory based on the regular expression, if the expression
	 * match the file name of the jar, then it will be included in the return list.
	 *
	 * @param e2eDir            directory to search
	 * @param regularExpression the regular expression.
	 * @return the matching jar paths.
	 * @throws IOException if any IO error happen.
	 */
	public static List<Path> findSQLJarPaths(String e2eDir, String regularExpression) throws IOException {
		Pattern p = Pattern.compile(regularExpression);
		return Files.list(Paths.get(e2eDir))
			.filter(path -> p.matcher(path.getFileName().toString()).find())
			.collect(Collectors.toList());
	}

	/**
	 * Search the unique jar file under the given end-to-end directory.
	 *
	 * @param e2eDir  directory to search.
	 * @param pattern the regular expression.
	 * @return the unique matching jar path.
	 * @throws IOException if no jar found or multiple jar files found.
	 */
	public static Path findSQLJarPath(String e2eDir, String pattern) throws IOException {
		List<Path> paths = findSQLJarPaths(e2eDir, pattern);
		if (paths.size() == 0) {
			throw new FileNotFoundException("No path with pattern \"" + pattern + "\" under " + e2eDir);
		} else if (paths.size() > 1) {
			throw new RuntimeException("Multiple paths with pattern \"" + pattern + "\" under " + e2eDir);
		}
		return paths.get(0);
	}
}
