/**
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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.tasks.TimeServiceProvider;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Extension of {@link OneInputStreamOperatorTestHarness} that allows the operator to get
 * a {@link KeyedStateBackend}.
 *
 */
public class KeyedOneInputStreamOperatorTestHarness<K, IN, OUT>
		extends OneInputStreamOperatorTestHarness<IN, OUT> {

	// in case the operator creates one we store it here so that we
	// can snapshot its state
	private KeyedStateBackend<?> keyedStateBackend = null;

	// when we restore we keep the state here so that we can call restore
	// when the operator requests the keyed state backend
	private KeyGroupsStateHandle restoredKeyedState = null;

	public KeyedOneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			final KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType) {
		super(operator);

		ClosureCleaner.clean(keySelector, false);
		config.setStatePartitioner(0, keySelector);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));
		config.setNumberOfKeyGroups(MAX_PARALLELISM);

		setupMockTaskCreateKeyedBackend();
	}

	public KeyedOneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator,
			ExecutionConfig executionConfig,
			KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType) {
		super(operator, executionConfig);

		ClosureCleaner.clean(keySelector, false);
		config.setStatePartitioner(0, keySelector);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));
		config.setNumberOfKeyGroups(MAX_PARALLELISM);

		setupMockTaskCreateKeyedBackend();
	}

	public KeyedOneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator,
			ExecutionConfig executionConfig,
			TimeServiceProvider testTimeProvider,
			KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType) {
		super(operator, executionConfig, testTimeProvider);

		ClosureCleaner.clean(keySelector, false);
		config.setStatePartitioner(0, keySelector);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));
		config.setNumberOfKeyGroups(MAX_PARALLELISM);

		setupMockTaskCreateKeyedBackend();
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
						keyedStateBackend.close();
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
								Collections.singletonList(restoredKeyedState),
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
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#snapshotState(org.apache.flink.core.fs.FSDataOutputStream, long, long)} ()}
	 */
	@Override
	public StreamStateHandle snapshot(long checkpointId, long timestamp) throws Exception {
		// simply use an in-memory handle
		MemoryStateBackend backend = new MemoryStateBackend();

		CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");
		CheckpointStreamFactory.CheckpointStateOutputStream outStream =
				streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);

		operator.snapshotState(outStream, checkpointId, timestamp);

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
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#restoreState(org.apache.flink.core.fs.FSDataInputStream)} ()}
	 */
	@Override
	public void restore(StreamStateHandle snapshot) throws Exception {
		FSDataInputStream inStream = snapshot.openInputStream();
		operator.restoreState(inStream);

		byte keyedStatePresent = (byte) inStream.read();
		if (keyedStatePresent == 1) {
			ObjectInputStream ois = new ObjectInputStream(inStream);
			this.restoredKeyedState = (KeyGroupsStateHandle) ois.readObject();
		}
	}

	/**
	 * Calls close and dispose on the operator.
	 */
	public void close() throws Exception {
		super.close();
		if(keyedStateBackend != null) {
			keyedStateBackend.close();
		}
	}
}
