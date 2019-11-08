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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Encapsulates the functionality necessary to submit a program to a remote cluster.
 *
 * @param <T> type of the cluster id
 */
public abstract class ClusterClient<T> implements AutoCloseable {

	/**
	 * User overridable hook to close the client, possibly closes internal services.
	 * @deprecated use the {@link #close()} instead. This method stays for backwards compatibility.
	 */
	public void shutdown() throws Exception {
		close();
	}

	@Override
	public void close() throws Exception {

	}

	/**
	 * Requests the {@link JobStatus} of the job with the given {@link JobID}.
	 */
	public abstract CompletableFuture<JobStatus> getJobStatus(JobID jobId);

	/**
	 * Cancels a job identified by the job id.
	 * @param jobId the job id
	 * @throws Exception In case an error occurred.
	 */
	public abstract void cancel(JobID jobId) throws Exception;

	/**
	 * Cancels a job identified by the job id and triggers a savepoint.
	 * @param jobId the job id
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return path where the savepoint is located
	 * @throws Exception In case an error occurred.
	 */
	public abstract String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) throws Exception;

	/**
	 * Stops a program on Flink cluster whose job-manager is configured in this client's configuration.
	 * Stopping works only for streaming programs. Be aware, that the program might continue to run for
	 * a while after sending the stop command, because after sources stopped to emit data all operators
	 * need to finish processing.
	 *
	 * @param jobId the job ID of the streaming program to stop
	 * @param advanceToEndOfEventTime flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return a {@link CompletableFuture} containing the path where the savepoint is located
	 * @throws Exception
	 *             If the job ID is invalid (ie, is unknown or refers to a batch job) or if sending the stop signal
	 *             failed. That might be due to an I/O problem, ie, the job-manager is unreachable.
	 */
	public abstract String stopWithSavepoint(final JobID jobId, final boolean advanceToEndOfEventTime, @Nullable final String savepointDirectory) throws Exception;

	/**
	 * Triggers a savepoint for the job identified by the job id. The savepoint will be written to the given savepoint
	 * directory, or {@link org.apache.flink.configuration.CheckpointingOptions#SAVEPOINT_DIRECTORY} if it is null.
	 *
	 * @param jobId job id
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return path future where the savepoint is located
	 * @throws FlinkException if no connection to the cluster could be established
	 */
	public abstract CompletableFuture<String> triggerSavepoint(JobID jobId, @Nullable String savepointDirectory) throws FlinkException;

	public abstract CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) throws FlinkException;

	/**
	 * Lists the currently running and finished jobs on the cluster.
	 *
	 * @return future collection of running and finished jobs
	 * @throws Exception if no connection to the cluster could be established
	 */
	public abstract CompletableFuture<Collection<JobStatusMessage>> listJobs() throws Exception;

	/**
	 * Requests and returns the accumulators for the given job identifier. Accumulators can be
	 * requested while a is running or after it has finished. The default class loader is used
	 * to deserialize the incoming accumulator results.
	 * @param jobID The job identifier of a job.
	 * @return A Map containing the accumulator's name and its value.
	 */
	public Map<String, OptionalFailure<Object>> getAccumulators(JobID jobID) throws Exception {
		return getAccumulators(jobID, ClassLoader.getSystemClassLoader());
	}

	/**
	 * Requests and returns the accumulators for the given job identifier. Accumulators can be
	 * requested while a is running or after it has finished.
	 * @param jobID The job identifier of a job.
	 * @param loader The class loader for deserializing the accumulator results.
	 * @return A Map containing the accumulator's name and its value.
	 */
	public abstract Map<String, OptionalFailure<Object>> getAccumulators(JobID jobID, ClassLoader loader) throws Exception;

	// ------------------------------------------------------------------------
	//  Abstract methods to be implemented by the cluster specific Client
	// ------------------------------------------------------------------------

	/**
	 * Returns an URL (as a string) to the JobManager web interface.
	 */
	public abstract String getWebInterfaceURL();

	/**
	 * Returns the cluster id identifying the cluster to which the client is connected.
	 *
	 * @return cluster id of the connected cluster
	 */
	public abstract T getClusterId();

	/**
	 * Return the Flink configuration object.
	 * @return The Flink configuration object
	 */
	public abstract Configuration getFlinkConfiguration();

	/**
	 * Submit the given {@link JobGraph} to the cluster.
	 *
	 * @param jobGraph to submit
	 * @return Future which is completed with the {@link JobSubmissionResult}
	 */
	public abstract CompletableFuture<JobSubmissionResult> submitJob(@Nonnull JobGraph jobGraph);

	/**
	 * Request the {@link JobResult} for the given {@link JobID}.
	 *
	 * @param jobId for which to request the {@link JobResult}
	 * @return Future which is completed with the {@link JobResult}
	 */
	public abstract CompletableFuture<JobResult> requestJobResult(@Nonnull JobID jobId);

	public void shutDownCluster() {
		throw new UnsupportedOperationException("The " + getClass().getSimpleName() + " does not support shutDownCluster.");
	}
}
