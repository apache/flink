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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.FutureUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TypeSerializerSerializationUtil.class)
public class MemoryStateBackendTest extends StateBackendTestBase<MemoryStateBackend> {

	@Override
	protected MemoryStateBackend getStateBackend() throws Exception {
		return new MemoryStateBackend(useAsyncMode());
	}

	protected boolean useAsyncMode() {
		return false;
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

	@Test
	@SuppressWarnings("unchecked")
	public void testNumStateEntries() throws Exception {
		KeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
		kvId.initializeSerializerUnlessSet(new ExecutionConfig());

		HeapKeyedStateBackend<Integer> heapBackend = (HeapKeyedStateBackend<Integer>) backend;

		assertEquals(0, heapBackend.numStateEntries());

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(0);
		state.update("hello");
		state.update("ciao");

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.update("foo");

		assertEquals(2, heapBackend.numStateEntries());

		backend.setCurrentKey(0);
		state.clear();

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.clear();

		assertEquals(0, heapBackend.numStateEntries());

		backend.dispose();
	}

	@Test
	public void testOversizedState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			try {
				CheckpointStreamFactory.CheckpointStateOutputStream outStream =
						streamFactory.createCheckpointStateOutputStream(12, 459);

				ObjectOutputStream oos = new ObjectOutputStream(outStream);
				oos.writeObject(state);

				oos.flush();

				outStream.closeAndGetHandle();

				fail("this should cause an exception");
			}
			catch (IOException e) {
				// now darling, isn't that exactly what we wanted?
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(state);
			oos.flush();
			StreamStateHandle handle = os.closeAndGetHandle();

			assertNotNull(handle);

			try (ObjectInputStream ois = new ObjectInputStream(handle.openInputStream())) {
				assertEquals(state, ois.readObject());
				assertTrue(ois.available() <= 0);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOversizedStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);

			try {
				oos.writeObject(state);
				oos.flush();
				os.closeAndGetHandle();
				fail("this should cause an exception");
			}
			catch (IOException e) {
				// oh boy! what an exception!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Verifies that the operator state backend fails with appropriate error and message if
	 * previous serializer can not be restored.
	 */
	@Test
	public void testOperatorStateRestoreFailsIfSerializerDeserializationFails() throws Exception {
		AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		Environment env = mock(Environment.class);
		when(env.getExecutionConfig()).thenReturn(new ExecutionConfig());
		when(env.getUserClassLoader()).thenReturn(OperatorStateBackendTest.class.getClassLoader());

		OperatorStateBackend operatorStateBackend = abstractStateBackend.createOperatorStateBackend(env, "test-op-name");

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

		CheckpointStreamFactory streamFactory = abstractStateBackend.createStreamFactory(new JobID(), "testOperator");
		RunnableFuture<OperatorStateHandle> runnableFuture =
			operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forFullCheckpoint());
		OperatorStateHandle stateHandle = FutureUtil.runIfNotDoneAndGet(runnableFuture);

		try {

			operatorStateBackend.close();
			operatorStateBackend.dispose();

			operatorStateBackend = abstractStateBackend.createOperatorStateBackend(
				env,
				"testOperator");

			// mock failure when deserializing serializer
			TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<?> mockProxy =
					mock(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class);
			doThrow(new IOException()).when(mockProxy).read(any(DataInputViewStreamWrapper.class));
			PowerMockito.whenNew(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class).withAnyArguments().thenReturn(mockProxy);

			operatorStateBackend.restore(Collections.singletonList(stateHandle));

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
		KeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
		kvId.initializeSerializerUnlessSet(new ExecutionConfig());

		HeapKeyedStateBackend<Integer> heapBackend = (HeapKeyedStateBackend<Integer>) backend;

		assertEquals(0, heapBackend.numStateEntries());

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// write some state
		backend.setCurrentKey(0);
		state.update("hello");
		state.update("ciao");

		KeyedStateHandle snapshot = runSnapshot(((HeapKeyedStateBackend<Integer>) backend).snapshot(
			682375462378L,
			2,
			streamFactory,
			CheckpointOptions.forFullCheckpoint()));

		backend.dispose();

		// ========== restore snapshot ==========

		Environment env = mock(Environment.class);
		when(env.getExecutionConfig()).thenReturn(new ExecutionConfig());
		when(env.getUserClassLoader()).thenReturn(OperatorStateBackendTest.class.getClassLoader());

		// mock failure when deserializing serializer
		TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<?> mockProxy =
				mock(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class);
		doThrow(new IOException()).when(mockProxy).read(any(DataInputViewStreamWrapper.class));
		PowerMockito.whenNew(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class).withAnyArguments().thenReturn(mockProxy);

		try {
			restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

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
