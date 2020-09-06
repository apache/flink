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
	private final int parallelism;
	private final boolean detached;
	private final List<String> arguments;

	JobSubmission(final Path jar, final int parallelism, final boolean detached, final List<String> arguments) {
		this.jar = jar;
		this.parallelism = parallelism;
		this.detached = detached;
		this.arguments = Collections.unmodifiableList(arguments);
	}

	public List<String> getArguments() {
		return arguments;
	}

	public boolean isDetached() {
		return detached;
	}

	public int getParallelism() {
		return parallelism;
	}

	public Path getJar() {
		return jar;
	}

	/**
	 * Builder for the {@link JobSubmission}.
	 */
	public static class JobSubmissionBuilder {
		private final Path jar;
		private int parallelism = 0;
		private final List<String> arguments = new ArrayList<>(2);
		private boolean detached = false;

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
			this.parallelism = parallelism;
			return this;
		}

		/**
		 * Sets whether the job should be submitted in a detached manner.
		 *
		 * @param detached whether to submit the job in a detached manner
		 * @return the modified builder
		 */
		public JobSubmissionBuilder setDetached(final boolean detached) {
			this.detached = detached;
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
			this.arguments.add(argument);
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

		public JobSubmission build() {
			return new JobSubmission(jar, parallelism, detached, arguments);
		}
	}
}
