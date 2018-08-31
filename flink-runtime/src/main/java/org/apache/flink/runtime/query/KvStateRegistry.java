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
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.taskmanager.Task;

import java.util.concurrent.ConcurrentHashMap;

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
	private final ConcurrentHashMap<KvStateID, KvStateEntry<?, ?, ?>> registeredKvStates = new ConcurrentHashMap<>(4);

	/** Registry listeners to be notified on registration/unregistration. */
	private final ConcurrentHashMap<JobID, KvStateRegistryListener> listeners = new ConcurrentHashMap<>(4);

	/**
	 * Registers a listener with the registry.
	 *
	 * @param jobId identifying the job for which to register a {@link KvStateRegistryListener}
	 * @param listener The registry listener.
	 * @throws IllegalStateException If there is a registered listener
	 */
	public void registerListener(JobID jobId, KvStateRegistryListener listener) {
		final KvStateRegistryListener previousValue = listeners.putIfAbsent(jobId, listener);

		if (previousValue != null) {
			throw new IllegalStateException("Listener already registered under " + jobId + '.');
		}
	}

	/**
	 * Unregisters the listener with the registry.
	 *
	 * @param jobId for which to unregister the {@link KvStateRegistryListener}
	 */
	public void unregisterListener(JobID jobId) {
		listeners.remove(jobId);
	}

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
			InternalKvState<?, ?, ?> kvState) {

		KvStateID kvStateId = new KvStateID();

		if (registeredKvStates.putIfAbsent(kvStateId, new KvStateEntry<>(kvState)) == null) {
			final KvStateRegistryListener listener = getKvStateRegistryListener(jobId);

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
			throw new IllegalStateException(
					"State \"" + registrationName + " \"(id=" + kvStateId + ") appears registered although it should not.");
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

		KvStateEntry<?, ?, ?> entry = registeredKvStates.remove(kvStateId);
		if (entry != null) {
			entry.clear();

			final KvStateRegistryListener listener = getKvStateRegistryListener(jobId);
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
	 * Returns the {@link KvStateEntry} containing the requested instance as identified by the
	 * given KvStateID, along with its {@link KvStateInfo} or <code>null</code> if none is registered.
	 *
	 * @param kvStateId KvStateID to identify the KvState instance
	 * @return The {@link KvStateEntry} instance identified by the KvStateID or <code>null</code> if there is none
	 */
	public KvStateEntry<?, ?, ?> getKvState(KvStateID kvStateId) {
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

	// ------------------------------------------------------------------------
	// Internal methods
	// ------------------------------------------------------------------------

	private KvStateRegistryListener getKvStateRegistryListener(JobID jobId) {
		// first check whether we are running the legacy code which registers
		// a single listener under HighAvailabilityServices.DEFAULT_JOB_ID
		KvStateRegistryListener listener = listeners.get(HighAvailabilityServices.DEFAULT_JOB_ID);

		if (listener == null) {
			listener = listeners.get(jobId);
		}
		return listener;
	}
}
