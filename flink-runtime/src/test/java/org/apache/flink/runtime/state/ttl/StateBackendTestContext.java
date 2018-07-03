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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

/** Base class for state backend test context. */
public abstract class StateBackendTestContext {
	private final StateBackend stateBackend;
	private final CheckpointStorageLocation checkpointStorageLocation;
	private final TtlTimeProvider timeProvider;

	private AbstractKeyedStateBackend<String> keyedStateBackend;

	protected StateBackendTestContext(TtlTimeProvider timeProvider) {
		this.timeProvider = Preconditions.checkNotNull(timeProvider);
		this.stateBackend = Preconditions.checkNotNull(createStateBackend());
		this.checkpointStorageLocation = createCheckpointStorageLocation();
	}

	protected abstract StateBackend createStateBackend();

	private CheckpointStorageLocation createCheckpointStorageLocation() {
		try {
			return stateBackend
				.createCheckpointStorage(new JobID())
				.initializeLocationForCheckpoint(2L);
		} catch (IOException e) {
			throw new RuntimeException("unexpected");
		}
	}

	void createAndRestoreKeyedStateBackend() {
		Environment env = new DummyEnvironment();
		try {
			disposeKeyedStateBackend();
			keyedStateBackend = stateBackend.createKeyedStateBackend(
				env, new JobID(), "test", StringSerializer.INSTANCE, 10,
				new KeyGroupRange(0, 9), env.getTaskKvStateRegistry(), timeProvider);
			keyedStateBackend.setCurrentKey("defaultKey");
		} catch (Exception e) {
			throw new RuntimeException("unexpected");
		}
	}

	void disposeKeyedStateBackend() {
		if (keyedStateBackend != null) {
			keyedStateBackend.dispose();
			keyedStateBackend = null;
		}
	}

	@Nonnull
	KeyedStateHandle takeSnapshot() throws Exception {
		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture =
			keyedStateBackend.snapshot(682375462392L, 10L,
				checkpointStorageLocation, CheckpointOptions.forCheckpointWithDefaultLocation());
		if (!snapshotRunnableFuture.isDone()) {
			snapshotRunnableFuture.run();
		}
		return snapshotRunnableFuture.get().getJobManagerOwnedSnapshot();
	}

	void restoreSnapshot(@Nullable KeyedStateHandle snapshot) throws Exception {
		Collection<KeyedStateHandle> restoreState =
			snapshot == null ? null : new StateObjectCollection<>(Collections.singleton(snapshot));
		keyedStateBackend.restore(restoreState);
		if (snapshot != null) {
			snapshot.discardState();
		}
	}

	void setCurrentKey(String key) {
		Preconditions.checkNotNull(keyedStateBackend, "keyed backend is not initialised");
		keyedStateBackend.setCurrentKey(key);
	}

	@SuppressWarnings("unchecked")
	<N, S extends State, V> S createState(
		StateDescriptor<S, V> stateDescriptor,
		@SuppressWarnings("SameParameterValue") N defaultNamespace) throws Exception {
		S state = keyedStateBackend.getOrCreateKeyedState(StringSerializer.INSTANCE, stateDescriptor);
		((InternalKvState<?, N, ?>) state).setCurrentNamespace(defaultNamespace);
		return state;
	}
}
