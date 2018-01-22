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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link KvStateRegistry}.
 */
public class KvStateRegistryTest extends TestLogger {

	/**
	 * Tests that {@link KvStateRegistryListener} only receive the notifications which
	 * are destined for them.
	 */
	@Test
	public void testKvStateRegistryListenerNotification() {
		final JobID jobId1 = new JobID();
		final JobID jobId2 = new JobID();

		final KvStateRegistry kvStateRegistry = new KvStateRegistry();

		final ArrayDeque<JobID> registeredNotifications1 = new ArrayDeque<>(2);
		final ArrayDeque<JobID> deregisteredNotifications1 = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener listener1 = new TestingKvStateRegistryListener(
			registeredNotifications1,
			deregisteredNotifications1);

		final ArrayDeque<JobID> registeredNotifications2 = new ArrayDeque<>(2);
		final ArrayDeque<JobID> deregisteredNotifications2 = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener listener2 = new TestingKvStateRegistryListener(
			registeredNotifications2,
			deregisteredNotifications2);

		kvStateRegistry.registerListener(jobId1, listener1);
		kvStateRegistry.registerListener(jobId2, listener2);

		final JobVertexID jobVertexId = new JobVertexID();
		final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
		final String registrationName = "foobar";
		final KvStateID kvStateID = kvStateRegistry.registerKvState(
			jobId1,
			jobVertexId,
			keyGroupRange,
			registrationName,
			new DummyKvState<>());

		assertThat(registeredNotifications1.poll(), equalTo(jobId1));
		assertThat(registeredNotifications2.isEmpty(), is(true));

		final JobVertexID jobVertexId2 = new JobVertexID();
		final KeyGroupRange keyGroupRange2 = new KeyGroupRange(0, 1);
		final String registrationName2 = "barfoo";
		final KvStateID kvStateID2 = kvStateRegistry.registerKvState(
			jobId2,
			jobVertexId2,
			keyGroupRange2,
			registrationName2,
			new DummyKvState<>());

		assertThat(registeredNotifications2.poll(), equalTo(jobId2));
		assertThat(registeredNotifications1.isEmpty(), is(true));

		kvStateRegistry.unregisterKvState(
			jobId1,
			jobVertexId,
			keyGroupRange,
			registrationName,
			kvStateID);

		assertThat(deregisteredNotifications1.poll(), equalTo(jobId1));
		assertThat(deregisteredNotifications2.isEmpty(), is(true));

		kvStateRegistry.unregisterKvState(
			jobId2,
			jobVertexId2,
			keyGroupRange2,
			registrationName2,
			kvStateID2);

		assertThat(deregisteredNotifications2.poll(), equalTo(jobId2));
		assertThat(deregisteredNotifications1.isEmpty(), is(true));
	}

	/**
	 * Tests that {@link KvStateRegistryListener} registered under {@link HighAvailabilityServices#DEFAULT_JOB_ID}
	 * will be used for all notifications.
	 */
	@Test
	public void testPreFlip6CodePathPreference() {
		final KvStateRegistry kvStateRegistry = new KvStateRegistry();
		final ArrayDeque<JobID> stateRegistrationNotifications = new ArrayDeque<>(2);
		final ArrayDeque<JobID> stateDeregistrationNotifications = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener testingListener = new TestingKvStateRegistryListener(
			stateRegistrationNotifications,
			stateDeregistrationNotifications);

		final ArrayDeque<JobID> anotherQueue = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener anotherListener = new TestingKvStateRegistryListener(
			anotherQueue,
			anotherQueue);

		final JobID jobId = new JobID();

		kvStateRegistry.registerListener(HighAvailabilityServices.DEFAULT_JOB_ID, testingListener);
		kvStateRegistry.registerListener(jobId, anotherListener);

		final JobVertexID jobVertexId = new JobVertexID();

		final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
		final String registrationName = "registrationName";
		final KvStateID kvStateID = kvStateRegistry.registerKvState(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName,
			new DummyKvState());

		assertThat(stateRegistrationNotifications.poll(), equalTo(jobId));
		// another listener should not have received any notifications
		assertThat(anotherQueue.isEmpty(), is(true));

		kvStateRegistry.unregisterKvState(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName,
			kvStateID);

		assertThat(stateDeregistrationNotifications.poll(), equalTo(jobId));
		// another listener should not have received any notifications
		assertThat(anotherQueue.isEmpty(), is(true));
	}

	/**
	 * Testing implementation of {@link KvStateRegistryListener}.
	 */
	private static final class TestingKvStateRegistryListener implements KvStateRegistryListener {

		private final Queue<JobID> stateRegisteredNotifications;
		private final Queue<JobID> stateDeregisteredNotifications;

		private TestingKvStateRegistryListener(
				Queue<JobID> stateRegisteredNotifications,
				Queue<JobID> stateDeregisteredNotifications) {
			this.stateRegisteredNotifications = stateRegisteredNotifications;
			this.stateDeregisteredNotifications = stateDeregisteredNotifications;
		}

		@Override
		public void notifyKvStateRegistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName, KvStateID kvStateId) {
			stateRegisteredNotifications.offer(jobId);
		}

		@Override
		public void notifyKvStateUnregistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName) {
			stateDeregisteredNotifications.offer(jobId);
		}
	}

	/**
	 * Testing implementation of {@link InternalKvState}.
	 *
	 * @param <T> type of the state
	 */
	private static final class DummyKvState<T> implements InternalKvState<T> {

		@Override
		public void setCurrentNamespace(Object namespace) {
			// noop
		}

		@Override
		public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
			return serializedKeyAndNamespace;
		}

		@Override
		public void clear() {
			// noop
		}
	}

}
