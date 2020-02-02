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

import org.apache.flink.util.Preconditions;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Programmatic definition of a job-submission.
 */
public class JobSubmission {

	private final Path jar;
	private final List<String> options;
	private final List<String> arguments;

	JobSubmission(
			final Path jar,
			final List<String> options,
			final List<String> arguments) {
		this.jar = jar;
		this.options = options;
		this.arguments = Collections.unmodifiableList(arguments);
	}

	public List<String> getArguments() {
		return arguments;
	}

	public List<String> getOptions() {
		return options;
	}

	public Path getJar() {
		return jar;
	}

	/**
	 * Builder for the {@link JobSubmission}.
	 */
	public static class JobSubmissionBuilder {
		private final Path jar;
		private final List<String> arguments = new ArrayList<>(2);
		private final List<String> options = new ArrayList<>(2);

		public JobSubmissionBuilder(final Path jar) {
			Preconditions.checkNotNull(jar);
			Preconditions.checkArgument(jar.isAbsolute(), "Jar path must be absolute.");
			this.jar = jar;
		}

		/**
		 * Sets the parallelism for the job.
		 *
		 * @param parallelism parallelism for the job
		 * @return the modified builder
		 */
		public JobSubmissionBuilder setParallelism(final int parallelism) {
			addOption("-p", String.valueOf(parallelism));
			return this;
		}

		/**
		 * Sets whether the job should be submitted in a detached manner.
		 *
		 * @param detached whether to submit the job in a detached manner
		 * @return the modified builder
		 */
		public JobSubmissionBuilder setDetached(final boolean detached) {
			addOption("-d");
			return this;
		}

		/**
		 * Adds a program argument.
		 *
		 * @param argument argument argument
		 * @return the modified builder
		 */
		public JobSubmissionBuilder addArgument(final String argument) {
			Preconditions.checkNotNull(argument);
			arguments.add(argument);
			return this;
		}

		/**
		 * Convenience method for providing key-value program arguments. Invoking this method is equivalent to invoking
		 * {@link #addArgument(String)} twice.
		 *
		 * @param key argument key
		 * @param value argument value
		 * @return the modified builder
		 */
		public JobSubmissionBuilder addArgument(final String key, final String value) {
			addArgument(key);
			addArgument(value);
			return this;
		}

		/**
		 * Adds a program option.
		 *
		 * @param option option option
		 * @return the modified builder
		 */
		public JobSubmissionBuilder addOption(final String option) {
			Preconditions.checkNotNull(option);
			options.add(option);
			return this;
		}

		/**
		 * Convenience method for providing key-value program options. Invoking this method is equivalent to invoking
		 * {@link #addOption(String)} twice.
		 *
		 * @param key option key
		 * @param value option value
		 * @return the modified builder
		 */
		public JobSubmissionBuilder addOption(final String key, final String value) {
			addOption(key);
			addOption(value);
			return this;
		}

		/**
		 * Convenience method for setting Flink's main entry point.
		 *
		 * @param entryPoint fully qualified class name of the entry point
		 * @return the modified builder
		 */
		public JobSubmissionBuilder setEntryPoint(final String entryPoint) {
			addOption("-c", entryPoint);
			return this;
		}

		public JobSubmission build() {
			return new JobSubmission(jar, options, arguments);
		}
	}
}
