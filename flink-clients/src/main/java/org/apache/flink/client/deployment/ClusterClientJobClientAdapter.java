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

package org.apache.flink.client.deployment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link JobClient} interface that uses a {@link ClusterClient} underneath..
 */
public class ClusterClientJobClientAdapter<ClusterID> implements JobClient {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterClientJobClientAdapter.class);

	private final ClusterClient<ClusterID> clusterClient;

	private final JobID jobID;

	private final Thread shutdownHook;

	public ClusterClientJobClientAdapter(final ClusterClient<ClusterID> clusterClient, final JobID jobID, final boolean withShutdownHook) {
		this.jobID = checkNotNull(jobID);
		this.clusterClient = checkNotNull(clusterClient);

		if (withShutdownHook) {
			shutdownHook = ShutdownHookUtil.addShutdownHook(
					clusterClient::shutDownCluster, clusterClient.getClass().getSimpleName(), LOG);
		} else {
			shutdownHook = null;
		}
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(final ClassLoader userClassloader) {
		checkNotNull(userClassloader);

		final CompletableFuture<JobResult> jobResultFuture = clusterClient.requestJobResult(jobID);
		return jobResultFuture.handle((jobResult, throwable) -> {
			if (throwable != null) {
				ExceptionUtils.checkInterrupted(throwable);
				throw new CompletionException(new ProgramInvocationException("Could not run job", jobID, throwable));
			} else {
				try {
					return jobResult.toJobExecutionResult(userClassloader);
				} catch (JobExecutionException | IOException | ClassNotFoundException e) {
					throw new CompletionException(new ProgramInvocationException("Job failed", jobID, e));
				}
			}
		});
	}

	@Override
	public void close() throws Exception {
		if (shutdownHook != null) {
			ShutdownHookUtil.removeShutdownHook(shutdownHook, clusterClient.getClass().getSimpleName(), LOG);
		}
		this.clusterClient.close();
	}
}
