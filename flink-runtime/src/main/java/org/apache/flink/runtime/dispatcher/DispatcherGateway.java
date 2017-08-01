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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway for the Dispatcher component.
 */
public interface DispatcherGateway extends RpcGateway {

	/**
	 * Submit a job to the dispatcher.
	 *
	 * @param jobGraph JobGraph to submit
	 * @param timeout RPC timeout
	 * @return A future acknowledge if the submission succeeded
	 */
	CompletableFuture<Acknowledge> submitJob(
		JobGraph jobGraph,
		@RpcTimeout Time timeout);

	/**
	 * Lists the current set of submitted jobs.
	 *
	 * @param timeout RPC timeout
	 * @return A future collection of currently submitted jobs
	 */
	CompletableFuture<Collection<JobID>> listJobs(
		@RpcTimeout Time timeout);
}
