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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;

/**
 * A listener for a {@link KvStateRegistry}.
 *
 * <p>The registry calls these methods when KvState instances are registered
 * and unregistered.
 */
public interface KvStateRegistryListener {

	/**
	 * Notifies the listener about a registered KvState instance.
	 *
	 * @param jobId            Job ID the KvState instance belongs to
	 * @param jobVertexId      JobVertexID the KvState instance belongs to
	 * @param keyGroupRange    Key group range the KvState instance belongs to
	 * @param registrationName Name under which the KvState is registered
	 * @param kvStateId        ID of the KvState instance
	 */
	void notifyKvStateRegistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName,
			KvStateID kvStateId);

	/**
	 * Notifies the listener about an unregistered KvState instance.
	 *
	 * @param jobId            Job ID the KvState instance belongs to
	 * @param jobVertexId      JobVertexID the KvState instance belongs to
	 * @param keyGroupRange    Key group range the KvState instance belongs to
	 * @param registrationName Name under which the KvState is registered
	 */
	void notifyKvStateUnregistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName);

}
