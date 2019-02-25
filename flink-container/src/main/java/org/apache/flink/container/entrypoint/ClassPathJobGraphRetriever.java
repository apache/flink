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
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * {@link JobGraphRetriever} which creates the {@link JobGraph} from a class
 * on the class path.
 */
class ClassPathJobGraphRetriever implements JobGraphRetriever {

	private static final Logger LOG = LoggerFactory.getLogger(ClassPathJobGraphRetriever.class);
	private static final String JAVA_CLASS_PATH = "java.class.path";
	private static final String PATH_SEPARATOR = "path.separator";
	private static final String DEFAULT_PATH_SEPARATOR = ":";

	@Nonnull
	private final JobID jobId;

	@Nonnull
	private final SavepointRestoreSettings savepointRestoreSettings;

	@Nonnull
	private final String[] programArguments;

	@Nullable
	private final String jobClassName;

	ClassPathJobGraphRetriever(
			@Nonnull JobID jobId,
			@Nonnull SavepointRestoreSettings savepointRestoreSettings,
			@Nonnull String[] programArguments,
			@Nullable String jobClassName) {
		this.jobId = requireNonNull(jobId, "jobId");
		this.savepointRestoreSettings = requireNonNull(savepointRestoreSettings, "savepointRestoreSettings");
		this.programArguments = requireNonNull(programArguments, "programArguments");
		this.jobClassName = jobClassName;
	}

	@Override
	public JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
		final PackagedProgram packagedProgram = createPackagedProgram();
		final int defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
		try {
			final JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
				packagedProgram,
				configuration,
				defaultParallelism,
				jobId);
			jobGraph.setAllowQueuedScheduling(true);
			jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);

			return jobGraph;
		} catch (Exception e) {
			throw new FlinkException("Could not create the JobGraph from the provided user code jar.", e);
		}
	}

	private PackagedProgram createPackagedProgram() throws FlinkException {
		try {
			final Class<?> mainClass = getClass().getClassLoader().loadClass(jobClassName);
			return new PackagedProgram(mainClass, programArguments);
		} catch (ClassNotFoundException | ProgramInvocationException e) {
			throw new FlinkException("Could not load the provided entrypoint class.", e);
		}
	}

}
