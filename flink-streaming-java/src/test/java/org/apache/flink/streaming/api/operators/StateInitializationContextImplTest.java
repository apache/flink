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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.TestTaskLocalStateStore;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.util.LongArrayList;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StateInitializationContextImpl}.
 */
public class StateInitializationContextImplTest {

	static final int NUM_HANDLES = 10;

	private StateInitializationContextImpl initializationContext;
	private CloseableRegistry closableRegistry;

	private int writtenKeyGroups;
	private Set<Integer> writtenOperatorStates;

	@Before
	public void setUp() throws Exception {

		this.writtenKeyGroups = 0;
		this.writtenOperatorStates = new HashSet<>();

		this.closableRegistry = new CloseableRegistry();

		ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos(64);

		List<KeyedStateHandle> keyedStateHandles = new ArrayList<>(NUM_HANDLES);
		int prev = 0;
		for (int i = 0; i < NUM_HANDLES; ++i) {
			out.reset();
			int size = i % 4;
			int end = prev + size;
			DataOutputView dov = new DataOutputViewStreamWrapper(out);
			KeyGroupRangeOffsets offsets =
					new KeyGroupRangeOffsets(i == 9 ? KeyGroupRange.EMPTY_KEY_GROUP_RANGE : new KeyGroupRange(prev, end));
			prev = end + 1;
			for (int kg : offsets.getKeyGroupRange()) {
				offsets.setKeyGroupOffset(kg, out.getPosition());
				dov.writeInt(kg);
				++writtenKeyGroups;
			}

			KeyedStateHandle handle =
					new KeyGroupsStateHandle(offsets, new ByteStateHandleCloseChecking("kg-" + i, out.toByteArray()));

			keyedStateHandles.add(handle);
		}

		List<OperatorStateHandle> operatorStateHandles = new ArrayList<>(NUM_HANDLES);

		for (int i = 0; i < NUM_HANDLES; ++i) {
			int size = i % 4;
			out.reset();
			DataOutputView dov = new DataOutputViewStreamWrapper(out);
			LongArrayList offsets = new LongArrayList(size);
			for (int s = 0; s < size; ++s) {
				offsets.add(out.getPosition());
				int val = i * NUM_HANDLES + s;
				dov.writeInt(val);
				writtenOperatorStates.add(val);
			}

			Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>();
			offsetsMap.put(
					DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
					new OperatorStateHandle.StateMetaInfo(offsets.toArray(), OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
			OperatorStateHandle operatorStateHandle =
					new OperatorStreamStateHandle(offsetsMap, new ByteStateHandleCloseChecking("os-" + i, out.toByteArray()));
			operatorStateHandles.add(operatorStateHandle);
		}

		OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(
			StateObjectCollection.empty(),
			new StateObjectCollection<>(operatorStateHandles),
			StateObjectCollection.empty(),
			new StateObjectCollection<>(keyedStateHandles));

		OperatorID operatorID = new OperatorID();
		TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();
		taskStateSnapshot.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);

		JobManagerTaskRestore jobManagerTaskRestore = new JobManagerTaskRestore(0L, taskStateSnapshot);

		TaskStateManager manager = new TaskStateManagerImpl(
			new JobID(),
			new ExecutionAttemptID(),
			new TestTaskLocalStateStore(),
			jobManagerTaskRestore,
			mock(CheckpointResponder.class));

		DummyEnvironment environment = new DummyEnvironment(
			"test",
			1,
			0,
			prev);

		environment.setTaskStateManager(manager);

		StateBackend stateBackend = new MemoryStateBackend(1024);
		StreamTaskStateInitializer streamTaskStateManager = new StreamTaskStateInitializerImpl(
			environment,
			stateBackend,
			mock(ProcessingTimeService.class)) {

			@Override
			protected <K> InternalTimeServiceManager<K> internalTimeServiceManager(
				AbstractKeyedStateBackend<K> keyedStatedBackend,
				KeyContext keyContext,
				Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates) throws Exception {

				// We do not initialize a timer service manager here, because it would already consume the raw keyed
				// state as part of initialization. For the purpose of this test, we want an unconsumed raw keyed
				// stream.
				return null;
			}
		};

		AbstractStreamOperator<?> mockOperator = mock(AbstractStreamOperator.class);
		when(mockOperator.getOperatorID()).thenReturn(operatorID);

		StreamOperatorStateContext stateContext = streamTaskStateManager.streamOperatorStateContext(
			operatorID,
			"TestOperatorClass",
			mockOperator,
			// notice that this essentially disables the previous test of the keyed stream because it was and is always
			// consumed by the timer service.
			IntSerializer.INSTANCE,
			closableRegistry);

		this.initializationContext =
				new StateInitializationContextImpl(
						stateContext.isRestored(),
						stateContext.operatorStateBackend(),
						mock(KeyedStateStore.class),
						stateContext.rawKeyedStateInputs(),
						stateContext.rawOperatorStateInputs());
	}

	@Test
	public void getOperatorStateStreams() throws Exception {

		int i = 0;
		int s = 0;
		for (StatePartitionStreamProvider streamProvider : initializationContext.getRawOperatorStateInputs()) {
			if (0 == i % 4) {
				++i;
			}
			Assert.assertNotNull(streamProvider);
			try (InputStream is = streamProvider.getStream()) {
				DataInputView div = new DataInputViewStreamWrapper(is);

				int val = div.readInt();
				Assert.assertEquals(i * NUM_HANDLES + s, val);
			}

			++s;
			if (s == i % 4) {
				s = 0;
				++i;
			}
		}

	}

	@Test
	public void getKeyedStateStreams() throws Exception {

		int readKeyGroupCount = 0;

		for (KeyGroupStatePartitionStreamProvider stateStreamProvider
				: initializationContext.getRawKeyedStateInputs()) {

			Assert.assertNotNull(stateStreamProvider);

			try (InputStream is = stateStreamProvider.getStream()) {
				DataInputView div = new DataInputViewStreamWrapper(is);
				int val = div.readInt();
				++readKeyGroupCount;
				Assert.assertEquals(stateStreamProvider.getKeyGroupId(), val);
			}
		}

		Assert.assertEquals(writtenKeyGroups, readKeyGroupCount);
	}

	@Test
	public void getOperatorStateStore() throws Exception {

		Set<Integer> readStatesCount = new HashSet<>();

		for (StatePartitionStreamProvider statePartitionStreamProvider
				: initializationContext.getRawOperatorStateInputs()) {

			Assert.assertNotNull(statePartitionStreamProvider);

			try (InputStream is = statePartitionStreamProvider.getStream()) {
				DataInputView div = new DataInputViewStreamWrapper(is);
				Assert.assertTrue(readStatesCount.add(div.readInt()));
			}
		}

		Assert.assertEquals(writtenOperatorStates, readStatesCount);
	}

	@Test
	public void close() throws Exception {

		int count = 0;
		int stopCount = NUM_HANDLES / 2;
		boolean isClosed = false;

		try {
			for (KeyGroupStatePartitionStreamProvider stateStreamProvider
					: initializationContext.getRawKeyedStateInputs()) {
				Assert.assertNotNull(stateStreamProvider);

				if (count == stopCount) {
					closableRegistry.close();
					isClosed = true;
				}

				try (InputStream is = stateStreamProvider.getStream()) {
					DataInputView div = new DataInputViewStreamWrapper(is);
					try {
						int val = div.readInt();
						Assert.assertEquals(stateStreamProvider.getKeyGroupId(), val);
						if (isClosed) {
							Assert.fail("Close was ignored: stream");
						}
						++count;
					} catch (IOException ioex) {
						if (!isClosed) {
							throw ioex;
						}
					}
				}
			}
			Assert.fail("Close was ignored: registry");
		} catch (IOException iex) {
			Assert.assertTrue(isClosed);
			Assert.assertEquals(stopCount, count);
		}

	}

	static final class ByteStateHandleCloseChecking extends ByteStreamStateHandle {

		private static final long serialVersionUID = -6201941296931334140L;

		public ByteStateHandleCloseChecking(String handleName, byte[] data) {
			super(handleName, data);
		}

		@Override
		public FSDataInputStream openInputStream() throws IOException {
			final FSDataInputStream original = super.openInputStream();

			return new FSDataInputStream() {

				private boolean closed = false;

				@Override
				public void seek(long desired) throws IOException {
					original.seek(desired);
				}

				@Override
				public long getPos() throws IOException {
					return original.getPos();
				}

				@Override
				public int read() throws IOException {
					if (closed) {
						throw new IOException("Stream closed");
					}
					return original.read();
				}

				@Override
				public void close() throws IOException {
					original.close();
					this.closed = true;
				}
			};
		}
	}

}
