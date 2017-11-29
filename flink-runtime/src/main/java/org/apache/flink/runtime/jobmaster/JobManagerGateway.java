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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Public JobManager gateway.
 *
 * <p>This interface constitutes the operations an external component can
 * trigger on the JobManager.
 */
public interface JobManagerGateway extends RestfulGateway {

	/**
	 * Requests the BlobServer port.
	 *
	 * @param timeout for this operation
	 * @return Future containing the BlobServer port
	 */
	CompletableFuture<Integer> requestBlobServerPort(Time timeout);

	/**
	 * Submits a job to the JobManager.
	 *
	 * @param jobGraph to submit
	 * @param listeningBehaviour of the client
	 * @param timeout for this operation
	 * @return Future containing an Acknowledge message if the submission succeeded
	 */
	CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, ListeningBehaviour listeningBehaviour, Time timeout);

	/**
	 * Cancels the given job after taking a savepoint and returning its path.
	 *
	 * If the savepointPath is null, then the JobManager will use the default savepoint directory
	 * to store the savepoint in. After the savepoint has been taken and the job has been canceled
	 * successfully, the path of the savepoint is returned.
	 *
	 * @param jobId identifying the job to cancel
	 * @param savepointPath Optional path for the savepoint to be stored under; if null, then the default path is
	 *                      taken
	 * @param timeout for the asynchronous operation
	 * @return Future containing the savepoint path of the taken savepoint or an Exception if the operation failed
	 */
	CompletableFuture<String> cancelJobWithSavepoint(JobID jobId, @Nullable String savepointPath, Time timeout);

	/**
	 * Cancels the given job.
	 *
	 * @param jobId identifying the job to cancel
	 * @param timeout for the asynchronous operation
	 * @return Future containing Acknowledge or an Exception if the operation failed
	 */
	CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout);

	/**
	 * Stops the given job.
	 *
	 * @param jobId identifying the job to cancel
	 * @param timeout for the asynchronous operation
	 * @return Future containing Acknowledge or an Exception if the operation failed
	 */
	CompletableFuture<Acknowledge> stopJob(JobID jobId, Time timeout);

	/**
	 * Requests the class loading properties for the given JobID.
	 *
	 * @param jobId for which the class loading properties are requested
	 * @param timeout for this operation
	 * @return Future containing the optional class loading properties if they could be retrieved from the JobManager.
	 */
	CompletableFuture<Optional<JobManagerMessages.ClassloadingProps>> requestClassloadingProps(JobID jobId, Time timeout);

	/**
	 * Requests the TaskManager instance registered under the given instanceId from the JobManager.
	 * If there is no Instance registered, then {@link Optional#empty()} is returned.
	 *
	 * @param resourceId identifying the TaskManager which shall be retrieved
	 * @param timeout for the asynchronous operation
	 * @return Future containing the TaskManager instance registered under instanceId, otherwise {@link Optional#empty()}
	 */
	CompletableFuture<Optional<Instance>> requestTaskManagerInstance(ResourceID resourceId, Time timeout);

	/**
	 * Requests all currently registered TaskManager instances from the JobManager.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the collection of all currently registered TaskManager instances
	 */
	CompletableFuture<Collection<Instance>> requestTaskManagerInstances(Time timeout);

	/**
	 * Requests the job overview from the JobManager.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the job overview
	 */
	CompletableFuture<JobIdsWithStatusOverview> requestJobsOverview(Time timeout);
}
