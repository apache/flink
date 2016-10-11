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

package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateRegistryGateway;
import org.apache.flink.runtime.query.KvStateRegistryListener;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;

public class RpcKvStateRegistryListener implements KvStateRegistryListener {

	private final KvStateRegistryGateway kvStateRegistryGateway;
	private final KvStateServerAddress kvStateServerAddress;

	public RpcKvStateRegistryListener(
			KvStateRegistryGateway kvStateRegistryGateway,
			KvStateServerAddress kvStateServerAddress) {
		this.kvStateRegistryGateway = Preconditions.checkNotNull(kvStateRegistryGateway);
		this.kvStateServerAddress = Preconditions.checkNotNull(kvStateServerAddress);
	}

	@Override
	public void notifyKvStateRegistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName,
			KvStateID kvStateId) {
		kvStateRegistryGateway.notifyKvStateRegistered(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName,
			kvStateId,
			kvStateServerAddress);

	}

	@Override
	public void notifyKvStateUnregistered(
		JobID jobId,
		JobVertexID jobVertexId,
		KeyGroupRange keyGroupRange,
		String registrationName) {

		kvStateRegistryGateway.notifyKvStateUnregistered(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName);

	}
}
