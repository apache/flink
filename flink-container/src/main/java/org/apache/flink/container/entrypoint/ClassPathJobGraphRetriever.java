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

import javax.annotation.Nonnull;

/**
 * {@link JobGraphRetriever} which creates the {@link JobGraph} from a class
 * on the class path.
 */
public class ClassPathJobGraphRetriever implements JobGraphRetriever {

	@Nonnull
	private final String jobClassName;

	@Nonnull
	private final SavepointRestoreSettings savepointRestoreSettings;

	@Nonnull
	private final String[] programArguments;

	public static final JobID FIXED_JOB_ID = new JobID(0, 0);

	public ClassPathJobGraphRetriever(
			@Nonnull String jobClassName,
			@Nonnull SavepointRestoreSettings savepointRestoreSettings,
			@Nonnull String[] programArguments) {
		this.jobClassName = jobClassName;
		this.savepointRestoreSettings = savepointRestoreSettings;
		this.programArguments = programArguments;
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
				FIXED_JOB_ID);
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
