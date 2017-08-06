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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.rpc.RpcGateway;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Public JobManager gateway.
 *
 * <p>This interface constitutes the operations an external component can
 * trigger on the JobManager.
 */
public interface JobManagerGateway extends RpcGateway {

	/**
	 * Requests the class loading properties for the given JobID.
	 *
	 * @param jobId for which the class loading properties are requested
	 * @param timeout for this operation
	 * @return Future containing the optional class loading properties if they could be retrieved from the JobManager.
	 */
	CompletableFuture<Optional<JobManagerMessages.ClassloadingProps>> requestClassloadingProps(JobID jobId, Time timeout);

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
}
