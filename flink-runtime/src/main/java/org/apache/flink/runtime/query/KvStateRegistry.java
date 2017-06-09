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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.netty.KvStateServer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.taskmanager.Task;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A registry for {@link InternalKvState} instances per task manager.
 *
 * <p>This is currently only used for KvState queries: KvState instances, which
 * are marked as queryable in their state descriptor are registered here and
 * can be queried by the {@link KvStateServer}.
 *
 * <p>KvState is registered when it is created/restored and unregistered when
 * the owning operator stops running.
 */
public class KvStateRegistry {

	/** All registered KvState instances. */
	private final ConcurrentHashMap<KvStateID, InternalKvState<?>> registeredKvStates =
			new ConcurrentHashMap<>();

	/** Registry listener to be notified on registration/unregistration. */
	private final AtomicReference<KvStateRegistryListener> listener = new AtomicReference<>();

	/**
	 * Registers a listener with the registry.
	 *
	 * @param listener The registry listener.
	 * @throws IllegalStateException If there is a registered listener
	 */
	public void registerListener(KvStateRegistryListener listener) {
		if (!this.listener.compareAndSet(null, listener)) {
			throw new IllegalStateException("Listener already registered.");
		}
	}

	/**
	 * Unregisters the listener with the registry.
	 */
	public void unregisterListener() {
		listener.set(null);
	}

	/**
	 * Registers the KvState instance identified by the given 4-tuple of JobID,
	 * JobVertexID, key group index, and registration name.
	 *
	 * @param kvStateId KvStateID to identify the KvState instance
	 * @param kvState   KvState instance to register
	 * @throws IllegalStateException If there is a KvState instance registered
	 *                               with the same ID.
	 */

	/**
	 * Registers the KvState instance and returns the assigned ID.
	 *
	 * @param jobId            JobId the KvState instance belongs to
	 * @param jobVertexId      JobVertexID the KvState instance belongs to
	 * @param keyGroupRange    Key group range the KvState instance belongs to
	 * @param registrationName Name under which the KvState is registered
	 * @param kvState          KvState instance to be registered
	 * @return Assigned KvStateID
	 */
	public KvStateID registerKvState(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName,
			InternalKvState<?> kvState) {

		KvStateID kvStateId = new KvStateID();

		if (registeredKvStates.putIfAbsent(kvStateId, kvState) == null) {
			KvStateRegistryListener listener = this.listener.get();
			if (listener != null) {
				listener.notifyKvStateRegistered(
						jobId,
						jobVertexId,
						keyGroupRange,
						registrationName,
						kvStateId);
			}

			return kvStateId;
		} else {
			throw new IllegalStateException(kvStateId + " is already registered.");
		}
	}

	/**
	 * Unregisters the KvState instance identified by the given KvStateID.
	 *
	 * @param jobId     JobId the KvState instance belongs to
	 * @param kvStateId KvStateID to identify the KvState instance
	 * @param keyGroupRange    Key group range the KvState instance belongs to
	 */
	public void unregisterKvState(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName,
			KvStateID kvStateId) {

		if (registeredKvStates.remove(kvStateId) != null) {
			KvStateRegistryListener listener = this.listener.get();
			if (listener != null) {
				listener.notifyKvStateUnregistered(
						jobId,
						jobVertexId,
						keyGroupRange,
						registrationName);
			}
		}
	}

	/**
	 * Returns the KvState instance identified by the given KvStateID or
	 * <code>null</code> if none is registered.
	 *
	 * @param kvStateId KvStateID to identify the KvState instance
	 * @return KvState instance identified by the KvStateID or <code>null</code>
	 */
	public InternalKvState<?> getKvState(KvStateID kvStateId) {
		return registeredKvStates.get(kvStateId);
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates a {@link TaskKvStateRegistry} facade for the {@link Task}
	 * identified by the given JobID and JobVertexID instance.
	 *
	 * @param jobId JobID of the task
	 * @param jobVertexId JobVertexID of the task
	 * @return A {@link TaskKvStateRegistry} facade for the task
	 */
	public TaskKvStateRegistry createTaskRegistry(JobID jobId, JobVertexID jobVertexId) {
		return new TaskKvStateRegistry(this, jobId, jobVertexId);
	}

}
