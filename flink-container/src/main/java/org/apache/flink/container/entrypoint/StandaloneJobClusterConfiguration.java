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

package org.apache.flink.container.entrypoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the {@link StandaloneJobClusterEntryPoint}.
 */
final class StandaloneJobClusterConfiguration extends EntrypointClusterConfiguration {

	@Nonnull
	private final String jobClassName;

	@Nonnull
	private final SavepointRestoreSettings savepointRestoreSettings;

	@Nonnull
	private final JobID jobId;

	StandaloneJobClusterConfiguration(
			@Nonnull String configDir,
			@Nonnull Properties dynamicProperties,
			@Nonnull String[] args,
			@Nullable String hostname,
			int restPort,
			@Nonnull String jobClassName,
			@Nonnull SavepointRestoreSettings savepointRestoreSettings,
			@Nonnull JobID jobId) {
		super(configDir, dynamicProperties, args, hostname, restPort);
		this.jobClassName = requireNonNull(jobClassName, "jobClassName");
		this.savepointRestoreSettings = requireNonNull(savepointRestoreSettings, "savepointRestoreSettings");
		this.jobId = requireNonNull(jobId, "jobId");
	}

	@Nonnull
	String getJobClassName() {
		return jobClassName;
	}

	@Nonnull
	SavepointRestoreSettings getSavepointRestoreSettings() {
		return savepointRestoreSettings;
	}

	@Nonnull
	JobID getJobId() {
		return jobId;
	}
}
