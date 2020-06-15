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

package org.apache.flink.tests.util.flink;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Programmatic definition of a SQL job-submission.
 */
public class SQLJobSubmission {

	private final List<String> sqlLines;
	private final List<String> jars;
	private final String defaultEnvFile;
	private final String sessionEnvFile;

	private SQLJobSubmission(
			List<String> sqlLines,
			List<String> jars,
			String defaultEnvFile,
			String sessionEnvFile) {
		this.sqlLines = checkNotNull(sqlLines);
		this.jars = checkNotNull(jars);
		this.defaultEnvFile = defaultEnvFile;
		this.sessionEnvFile = sessionEnvFile;
	}

	public Optional<String> getDefaultEnvFile() {
		return Optional.ofNullable(defaultEnvFile);
	}

	public Optional<String> getSessionEnvFile() {
		return Optional.ofNullable(sessionEnvFile);
	}

	public List<String> getJars() {
		return this.jars;
	}

	public List<String> getSqlLines(){
		return this.sqlLines;
	}

	/**
	 * Builder for the {@link SQLJobSubmission}.
	 */
	public static class SQLJobSubmissionBuilder {
		private final List<String> sqlLines;
		private final List<String> jars = new ArrayList<>();
		private String defaultEnvFile = null;
		private String sessionEnvFile = null;

		public SQLJobSubmissionBuilder(List<String> sqlLines) {
			this.sqlLines = sqlLines;
		}

		public SQLJobSubmissionBuilder setDefaultEnvFile(String defaultEnvFile) {
			this.defaultEnvFile = defaultEnvFile;
			return this;
		}

		public SQLJobSubmissionBuilder setSessionEnvFile(String sessionEnvFile) {
			this.sessionEnvFile = sessionEnvFile;
			return this;
		}

		public SQLJobSubmissionBuilder addJar(Path jarFile) {
			this.jars.add(jarFile.toAbsolutePath().toString());
			return this;
		}

		public SQLJobSubmissionBuilder addJars(List<Path> jarFiles) {
			jarFiles.forEach(this::addJar);
			return this;
		}

		public SQLJobSubmission build() {
			return new SQLJobSubmission(sqlLines, jars, defaultEnvFile, sessionEnvFile);
		}
	}
}
