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
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway to report key-value state registration and deregistrations.
 */
public interface KvStateRegistryGateway {

	/**
	 * Notifies that queryable state has been registered.
	 *
	 * @param jobId	identifying the job for which to register a key value state
	 * @param jobVertexId JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange Key group range the KvState instance belongs to.
	 * @param registrationName Name under which the KvState has been registered.
	 * @param kvStateId ID of the registered KvState instance.
	 * @param kvStateServerAddress Server address where to find the KvState instance.
	 * @return Future acknowledge if the key-value state has been registered
	 */
	CompletableFuture<Acknowledge> notifyKvStateRegistered(
		final JobID jobId,
		final JobVertexID jobVertexId,
		final KeyGroupRange keyGroupRange,
		final String registrationName,
		final KvStateID kvStateId,
		final InetSocketAddress kvStateServerAddress);

	/**
	 * Notifies that queryable state has been unregistered.
	 *
	 * @param jobId	identifying the job for which to unregister a key value state
	 * @param jobVertexId JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange Key group index the KvState instance belongs to.
	 * @param registrationName Name under which the KvState has been registered.
	 * @return Future acknowledge if the key-value state has been unregistered
	 */
	CompletableFuture<Acknowledge> notifyKvStateUnregistered(
		final JobID jobId,
		final JobVertexID jobVertexId,
		final KeyGroupRange keyGroupRange,
		final String registrationName);
}
