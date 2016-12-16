/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.util.Migration;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;

/**
 * Extension of {@link OneInputStreamOperatorTestHarness} that allows the operator to get
 * a {@link KeyedStateBackend}.
 */
public class KeyedOneInputStreamOperatorTestHarness<K, IN, OUT>
		extends OneInputStreamOperatorTestHarness<IN, OUT> {

	// in case the operator creates one we store it here so that we
	// can snapshot its state
	private AbstractKeyedStateBackend<?> keyedStateBackend = null;

	// when we restore we keep the state here so that we can call restore
	// when the operator requests the keyed state backend
	private List<KeyGroupsStateHandle> restoredKeyedState = null;

	public KeyedOneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			final KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType,
			int maxParallelism,
			int numSubtasks,
			int subtaskIndex) throws Exception {
		super(operator, maxParallelism, numSubtasks, subtaskIndex);

		ClosureCleaner.clean(keySelector, false);
		config.setStatePartitioner(0, keySelector);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));

		setupMockTaskCreateKeyedBackend();
	}


	public KeyedOneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			final KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType) throws Exception {
		this(operator, keySelector, keyType, 1, 1, 0);
	}

	private void setupMockTaskCreateKeyedBackend() {

		try {
			doAnswer(new Answer<KeyedStateBackend>() {
				@Override
				public KeyedStateBackend answer(InvocationOnMock invocationOnMock) throws Throwable {

					final TypeSerializer keySerializer = (TypeSerializer) invocationOnMock.getArguments()[0];
					final int numberOfKeyGroups = (Integer) invocationOnMock.getArguments()[1];
					final KeyGroupRange keyGroupRange = (KeyGroupRange) invocationOnMock.getArguments()[2];

					if(keyedStateBackend != null) {
						keyedStateBackend.dispose();
					}

					if (restoredKeyedState == null) {
						keyedStateBackend = stateBackend.createKeyedStateBackend(
								mockTask.getEnvironment(),
								new JobID(),
								"test_op",
								keySerializer,
								numberOfKeyGroups,
								keyGroupRange,
								mockTask.getEnvironment().getTaskKvStateRegistry());
						return keyedStateBackend;
					} else {
						keyedStateBackend = stateBackend.restoreKeyedStateBackend(
								mockTask.getEnvironment(),
								new JobID(),
								"test_op",
								keySerializer,
								numberOfKeyGroups,
								keyGroupRange,
								restoredKeyedState,
								mockTask.getEnvironment().getTaskKvStateRegistry());
						restoredKeyedState = null;
						return keyedStateBackend;
					}
				}
			}).when(mockTask).createKeyedStateBackend(any(TypeSerializer.class), anyInt(), any(KeyGroupRange.class));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/**
	 *
	 */
	@Override
	public StreamStateHandle snapshotLegacy(long checkpointId, long timestamp) throws Exception {
		// simply use an in-memory handle
		MemoryStateBackend backend = new MemoryStateBackend();

		CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");
		CheckpointStreamFactory.CheckpointStateOutputStream outStream =
				streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);

		if (operator instanceof StreamCheckpointedOperator) {
			((StreamCheckpointedOperator) operator).snapshotState(outStream, checkpointId, timestamp);
		}

		if (keyedStateBackend != null) {
			RunnableFuture<KeyGroupsStateHandle> keyedSnapshotRunnable = keyedStateBackend.snapshot(checkpointId,
					timestamp,
					streamFactory);
			if(!keyedSnapshotRunnable.isDone()) {
				Thread runner = new Thread(keyedSnapshotRunnable);
				runner.start();
			}
			outStream.write(1);
			ObjectOutputStream oos = new ObjectOutputStream(outStream);
			oos.writeObject(keyedSnapshotRunnable.get());
			oos.flush();
		} else {
			outStream.write(0);
		}
		return outStream.closeAndGetHandle();
	}

	/**
	 *
	 */
	@Override
	public void restore(StreamStateHandle snapshot) throws Exception {
		try (FSDataInputStream inStream = snapshot.openInputStream()) {

			if (operator instanceof StreamCheckpointedOperator) {
				((StreamCheckpointedOperator) operator).restoreState(inStream);
			}

			byte keyedStatePresent = (byte) inStream.read();
			if (keyedStatePresent == 1) {
				ObjectInputStream ois = new ObjectInputStream(inStream);
				this.restoredKeyedState = Collections.singletonList((KeyGroupsStateHandle) ois.readObject());
			}
		}
	}


	private static boolean hasMigrationHandles(Collection<KeyGroupsStateHandle> allKeyGroupsHandles) {
		for (KeyGroupsStateHandle handle : allKeyGroupsHandles) {
			if (handle instanceof Migration) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void initializeState(OperatorStateHandles operatorStateHandles) throws Exception {
		if (operatorStateHandles != null) {
			int numKeyGroups = getEnvironment().getTaskInfo().getNumberOfKeyGroups();
			int numSubtasks = getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
			int subtaskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();

			// create a new OperatorStateHandles that only contains the state for our key-groups

			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
					numKeyGroups,
					numSubtasks);

			KeyGroupRange localKeyGroupRange =
					keyGroupPartitions.get(subtaskIndex);

			restoredKeyedState = null;
			Collection<KeyGroupsStateHandle> managedKeyedState = operatorStateHandles.getManagedKeyedState();
			if (managedKeyedState != null) {

				// if we have migration handles, don't reshuffle state and preserve
				// the migration tag
				if (hasMigrationHandles(managedKeyedState)) {
					List<KeyGroupsStateHandle> result = new ArrayList<>(managedKeyedState.size());
					result.addAll(managedKeyedState);
					restoredKeyedState = result;
				} else {
					restoredKeyedState = StateAssignmentOperation.getKeyGroupsStateHandles(
							managedKeyedState,
							localKeyGroupRange);
				}
			}
		}

		super.initializeState(operatorStateHandles);
	}
}
