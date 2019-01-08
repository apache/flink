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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.testutils.ArtificialCNFExceptionThrowingClassLoader;
import org.apache.flink.util.FutureUtil;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend}.
 */
public class MemoryStateBackendTest extends StateBackendTestBase<MemoryStateBackend> {

	@Override
	protected MemoryStateBackend getStateBackend() throws Exception {
		return new MemoryStateBackend(useAsyncMode());
	}

	protected boolean useAsyncMode() {
		return false;
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return true;
	}

	// disable these because the verification does not work for this state backend
	@Override
	@Test
	public void testValueStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testListStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testReducingStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testMapStateRestoreWithWrongSerializers() {}



	/**
	 * Verifies that the operator state backend fails with appropriate error and message if
	 * previous serializer can not be restored.
	 */
	@Test
	public void testOperatorStateRestoreFailsIfSerializerDeserializationFails() throws Exception {
		DummyEnvironment env = new DummyEnvironment();
		AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		OperatorStateBackend operatorStateBackend =
			abstractStateBackend.createOperatorStateBackend(env, "test-op-name");

		// write some state
		ListStateDescriptor<Serializable> stateDescriptor1 = new ListStateDescriptor<>("test1", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor2 = new ListStateDescriptor<>("test2", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor3 = new ListStateDescriptor<>("test3", new JavaSerializer<>());
		ListState<Serializable> listState1 = operatorStateBackend.getListState(stateDescriptor1);
		ListState<Serializable> listState2 = operatorStateBackend.getListState(stateDescriptor2);
		ListState<Serializable> listState3 = operatorStateBackend.getUnionListState(stateDescriptor3);

		listState1.add(42);
		listState1.add(4711);

		listState2.add(7);
		listState2.add(13);
		listState2.add(23);

		listState3.add(17);
		listState3.add(18);
		listState3.add(19);
		listState3.add(20);

		CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(MemoryStateBackend.DEFAULT_MAX_STATE_SIZE);

		RunnableFuture<SnapshotResult<OperatorStateHandle>> runnableFuture =
			operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

		SnapshotResult<OperatorStateHandle> snapshotResult = FutureUtil.runIfNotDoneAndGet(runnableFuture);
		OperatorStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();

		try {

			operatorStateBackend.close();
			operatorStateBackend.dispose();

			env = new DummyEnvironment(
				new ArtificialCNFExceptionThrowingClassLoader(
					getClass().getClassLoader(),
					Collections.singleton(JavaSerializer.class.getName())));

			operatorStateBackend = abstractStateBackend.createOperatorStateBackend(
				env,
				"testOperator");

			operatorStateBackend.restore(StateObjectCollection.singleton(stateHandle));

			fail("The operator state restore should have failed if the previous state serializer could not be loaded.");
		} catch (IOException expected) {
			Assert.assertTrue(expected.getMessage().contains("Unable to restore operator state"));
		} finally {
			stateHandle.discardState();
		}
	}

	/**
	 * Verifies that memory-backed keyed state backend fails with appropriate error and message if
	 * previous serializer can not be restored.
	 */
	@Test
	public void testKeyedStateRestoreFailsIfSerializerDeserializationFails() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		KeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
		kvId.initializeSerializerUnlessSet(new ExecutionConfig());

		HeapKeyedStateBackend<Integer> heapBackend = (HeapKeyedStateBackend<Integer>) backend;

		assertEquals(0, heapBackend.numKeyValueStateEntries());

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// write some state
		backend.setCurrentKey(0);
		state.update("hello");
		state.update("ciao");

		KeyedStateHandle snapshot = runSnapshot(
			((HeapKeyedStateBackend<Integer>) backend).snapshot(
				682375462378L,
				2,
				streamFactory,
				CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);

		backend.dispose();

		// ========== restore snapshot ==========

		try {
			restoreKeyedBackend(
				IntSerializer.INSTANCE,
				snapshot,
				new DummyEnvironment(
					new ArtificialCNFExceptionThrowingClassLoader(
						getClass().getClassLoader(),
						Collections.singleton(StringSerializer.class.getName()))));

			fail("The keyed state restore should have failed if the previous state serializer could not be loaded.");
		} catch (IOException expected) {
			Assert.assertTrue(expected.getMessage().contains("Unable to restore keyed state"));
		}
	}

	@Ignore
	@Test
	public void testConcurrentMapIfQueryable() throws Exception {
		super.testConcurrentMapIfQueryable();
	}
}
