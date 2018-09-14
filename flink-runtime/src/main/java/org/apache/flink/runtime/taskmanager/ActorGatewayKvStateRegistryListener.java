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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateMessage;
import org.apache.flink.runtime.query.KvStateRegistryListener;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;

import java.net.InetSocketAddress;

/**
 * This implementation uses {@link ActorGateway} to forward key-value state notifications to the job
 * manager. The notifications are wrapped in an actor message and send to the given actor gateway.
 */
public class ActorGatewayKvStateRegistryListener implements KvStateRegistryListener {

	private ActorGateway jobManager;

	private InetSocketAddress kvStateServerAddress;

	public ActorGatewayKvStateRegistryListener(
		ActorGateway jobManager,
		InetSocketAddress kvStateServerAddress) {

		this.jobManager = Preconditions.checkNotNull(jobManager, "JobManager");
		this.kvStateServerAddress = Preconditions.checkNotNull(kvStateServerAddress, "ServerAddress");
	}

	@Override
	public void notifyKvStateRegistered(
		JobID jobId,
		JobVertexID jobVertexId,
		KeyGroupRange keyGroupRange,
		String registrationName,
		KvStateID kvStateId) {

		Object msg = new KvStateMessage.NotifyKvStateRegistered(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName,
			kvStateId,
			kvStateServerAddress);

		jobManager.tell(msg);
	}

	@Override
	public void notifyKvStateUnregistered(
		JobID jobId,
		JobVertexID jobVertexId,
		KeyGroupRange keyGroupRange,
		String registrationName) {

		Object msg = new KvStateMessage.NotifyKvStateUnregistered(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName);

		jobManager.tell(msg);
	}
}
