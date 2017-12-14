/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend.PartitionableListState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.Preconditions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TypeSerializerSerializationUtil.class, IntSerializer.class})
public class OperatorStateBackendTest {

	private final ClassLoader classLoader = getClass().getClassLoader();

	@Test
	public void testCreateOnAbstractStateBackend() throws Exception {
		// we use the memory state backend as a subclass of the AbstractStateBackend
		final AbstractStateBackend abstractStateBackend = new MemoryStateBackend();
		final OperatorStateBackend operatorStateBackend = abstractStateBackend.createOperatorStateBackend(
				createMockEnvironment(), "test-operator");

		assertNotNull(operatorStateBackend);
		assertTrue(operatorStateBackend.getRegisteredStateNames().isEmpty());
	}

	@Test
	public void testRegisterStatesWithoutTypeSerializer() throws Exception {
		// prepare an execution config with a non standard type registered
		final Class<?> registeredType = FutureTask.class;

		// validate the precondition of this test - if this condition fails, we need to pick a different
		// example serializer
		assertFalse(new KryoSerializer<>(File.class, new ExecutionConfig()).getKryo().getDefaultSerializer(registeredType)
				instanceof com.esotericsoftware.kryo.serializers.JavaSerializer);

		final ExecutionConfig cfg = new ExecutionConfig();
		cfg.registerTypeWithKryoSerializer(registeredType, com.esotericsoftware.kryo.serializers.JavaSerializer.class);

		final OperatorStateBackend operatorStateBackend = new DefaultOperatorStateBackend(classLoader, cfg, false);

		ListStateDescriptor<File> stateDescriptor = new ListStateDescriptor<>("test", File.class);
		ListStateDescriptor<String> stateDescriptor2 = new ListStateDescriptor<>("test2", String.class);

		ListState<File> listState = operatorStateBackend.getListState(stateDescriptor);
		assertNotNull(listState);

		ListState<String> listState2 = operatorStateBackend.getListState(stateDescriptor2);
		assertNotNull(listState2);

		assertEquals(2, operatorStateBackend.getRegisteredStateNames().size());

		// make sure that type registrations are forwarded
		TypeSerializer<?> serializer = ((PartitionableListState<?>) listState).getStateMetaInfo().getPartitionStateSerializer();
		assertTrue(serializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) serializer).getKryo().getSerializer(registeredType)
				instanceof com.esotericsoftware.kryo.serializers.JavaSerializer);

		Iterator<String> it = listState2.get().iterator();
		assertFalse(it.hasNext());
		listState2.add("kevin");
		listState2.add("sunny");

		it = listState2.get().iterator();
		assertEquals("kevin", it.next());
		assertEquals("sunny", it.next());
		assertFalse(it.hasNext());
	}

	@Test
	public void testRegisterStates() throws Exception {
		final OperatorStateBackend operatorStateBackend =
				new DefaultOperatorStateBackend(classLoader, new ExecutionConfig(), false);

		ListStateDescriptor<Serializable> stateDescriptor1 = new ListStateDescriptor<>("test1", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor2 = new ListStateDescriptor<>("test2", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor3 = new ListStateDescriptor<>("test3", new JavaSerializer<>());
		ListState<Serializable> listState1 = operatorStateBackend.getListState(stateDescriptor1);
		assertNotNull(listState1);
		assertEquals(1, operatorStateBackend.getRegisteredStateNames().size());
		Iterator<Serializable> it = listState1.get().iterator();
		assertFalse(it.hasNext());
		listState1.add(42);
		listState1.add(4711);

		it = listState1.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertFalse(it.hasNext());

		ListState<Serializable> listState2 = operatorStateBackend.getListState(stateDescriptor2);
		assertNotNull(listState2);
		assertEquals(2, operatorStateBackend.getRegisteredStateNames().size());
		assertFalse(it.hasNext());
		listState2.add(7);
		listState2.add(13);
		listState2.add(23);

		it = listState2.get().iterator();
		assertEquals(7, it.next());
		assertEquals(13, it.next());
		assertEquals(23, it.next());
		assertFalse(it.hasNext());

		ListState<Serializable> listState3 = operatorStateBackend.getUnionListState(stateDescriptor3);
		assertNotNull(listState3);
		assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());
		assertFalse(it.hasNext());
		listState3.add(17);
		listState3.add(3);
		listState3.add(123);

		it = listState3.get().iterator();
		assertEquals(17, it.next());
		assertEquals(3, it.next());
		assertEquals(123, it.next());
		assertFalse(it.hasNext());

		ListState<Serializable> listState1b = operatorStateBackend.getListState(stateDescriptor1);
		assertNotNull(listState1b);
		listState1b.add(123);
		it = listState1b.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertEquals(123, it.next());
		assertFalse(it.hasNext());

		it = listState1.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertEquals(123, it.next());
		assertFalse(it.hasNext());

		it = listState1b.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertEquals(123, it.next());
		assertFalse(it.hasNext());

		try {
			operatorStateBackend.getUnionListState(stateDescriptor2);
			fail("Did not detect changed mode");
		} catch (IllegalStateException ignored) {

		}

		try {
			operatorStateBackend.getListState(stateDescriptor3);
			fail("Did not detect changed mode");
		} catch (IllegalStateException ignored) {

		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCorrectClassLoaderUsedOnSnapshot() throws Exception {

		AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		final Environment env = createMockEnvironment();
		OperatorStateBackend operatorStateBackend = abstractStateBackend.createOperatorStateBackend(env, "test-op-name");

		AtomicInteger copyCounter = new AtomicInteger(0);
		TypeSerializer<Integer> serializer = new VerifyingIntSerializer(env.getUserClassLoader(), copyCounter);

		// write some state
		ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("test", serializer);
		ListState<Integer> listState = operatorStateBackend.getListState(stateDescriptor);

		listState.add(42);

		CheckpointStreamFactory streamFactory = abstractStateBackend.createStreamFactory(new JobID(), "testOperator");
		RunnableFuture<OperatorStateHandle> runnableFuture =
			operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forCheckpoint());
		FutureUtil.runIfNotDoneAndGet(runnableFuture);

		// make sure that the copy method has been called
		assertTrue(copyCounter.get() > 0);
	}

	/**
	 * Int serializer which verifies that the given classloader is set for the copy operation
	 */
	private static final class VerifyingIntSerializer extends TypeSerializer<Integer> {

		private static final long serialVersionUID = -5344563614550163898L;

		private transient ClassLoader classLoader;
		private transient AtomicInteger atomicInteger;

		private VerifyingIntSerializer(ClassLoader classLoader, AtomicInteger atomicInteger) {
			this.classLoader = Preconditions.checkNotNull(classLoader);
			this.atomicInteger = Preconditions.checkNotNull(atomicInteger);
		}

		@Override
		public boolean isImmutableType() {
			// otherwise the copy method won't be called for the deepCopy operation
			return false;
		}

		@Override
		public TypeSerializer<Integer> duplicate() {
			return this;
		}

		@Override
		public Integer createInstance() {
			return 0;
		}

		@Override
		public Integer copy(Integer from) {
			assertEquals(classLoader, Thread.currentThread().getContextClassLoader());
			atomicInteger.incrementAndGet();
			return IntSerializer.INSTANCE.copy(from);
		}

		@Override
		public Integer copy(Integer from, Integer reuse) {
			assertEquals(classLoader, Thread.currentThread().getContextClassLoader());
			atomicInteger.incrementAndGet();
			return IntSerializer.INSTANCE.copy(from, reuse);
		}

		@Override
		public int getLength() {
			return IntSerializer.INSTANCE.getLength();
		}

		@Override
		public void serialize(Integer record, DataOutputView target) throws IOException {
			IntSerializer.INSTANCE.serialize(record, target);
		}

		@Override
		public Integer deserialize(DataInputView source) throws IOException {
			return IntSerializer.INSTANCE.deserialize(source);
		}

		@Override
		public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
			return IntSerializer.INSTANCE.deserialize(reuse, source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			assertEquals(classLoader, Thread.currentThread().getContextClassLoader());
			atomicInteger.incrementAndGet();
			IntSerializer.INSTANCE.copy(source, target);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof VerifyingIntSerializer) {
				return ((VerifyingIntSerializer)obj).canEqual(this);
			} else {
				return false;
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof VerifyingIntSerializer;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return IntSerializer.INSTANCE.snapshotConfiguration();
		}

		@Override
		public CompatibilityResult<Integer> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			return IntSerializer.INSTANCE.ensureCompatibility(configSnapshot);
		}
	}

	@Test
	public void testSnapshotEmpty() throws Exception {
		final AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		final OperatorStateBackend operatorStateBackend =
				abstractStateBackend.createOperatorStateBackend(createMockEnvironment(), "testOperator");

		CheckpointStreamFactory streamFactory =
				abstractStateBackend.createStreamFactory(new JobID(), "testOperator");

		RunnableFuture<OperatorStateHandle> snapshot =
				operatorStateBackend.snapshot(0L, 0L, streamFactory, CheckpointOptions.forCheckpoint());

		OperatorStateHandle stateHandle = FutureUtil.runIfNotDoneAndGet(snapshot);
		assertNull(stateHandle);
	}

	@Test
	public void testSnapshotRestoreSync() throws Exception {
		AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		OperatorStateBackend operatorStateBackend = abstractStateBackend.createOperatorStateBackend(createMockEnvironment(), "test-op-name");
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
				operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forCheckpoint());
		OperatorStateHandle stateHandle = FutureUtil.runIfNotDoneAndGet(runnableFuture);

		try {

			operatorStateBackend.close();
			operatorStateBackend.dispose();

			operatorStateBackend = abstractStateBackend.createOperatorStateBackend(
					createMockEnvironment(),
					"testOperator");

			operatorStateBackend.restore(Collections.singletonList(stateHandle));

			assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());

			listState1 = operatorStateBackend.getListState(stateDescriptor1);
			listState2 = operatorStateBackend.getListState(stateDescriptor2);
			listState3 = operatorStateBackend.getUnionListState(stateDescriptor3);

			assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());

			Iterator<Serializable> it = listState1.get().iterator();
			assertEquals(42, it.next());
			assertEquals(4711, it.next());
			assertFalse(it.hasNext());

			it = listState2.get().iterator();
			assertEquals(7, it.next());
			assertEquals(13, it.next());
			assertEquals(23, it.next());
			assertFalse(it.hasNext());

			it = listState3.get().iterator();
			assertEquals(17, it.next());
			assertEquals(18, it.next());
			assertEquals(19, it.next());
			assertEquals(20, it.next());
			assertFalse(it.hasNext());

			operatorStateBackend.close();
			operatorStateBackend.dispose();
		} finally {
			stateHandle.discardState();
		}
	}

	@Test
	public void testSnapshotRestoreAsync() throws Exception {
		OperatorStateBackend operatorStateBackend =
				new DefaultOperatorStateBackend(OperatorStateBackendTest.class.getClassLoader(), new ExecutionConfig(), true);

		ListStateDescriptor<MutableType> stateDescriptor1 =
				new ListStateDescriptor<>("test1", new JavaSerializer<MutableType>());
		ListStateDescriptor<MutableType> stateDescriptor2 =
				new ListStateDescriptor<>("test2", new JavaSerializer<MutableType>());
		ListStateDescriptor<MutableType> stateDescriptor3 =
				new ListStateDescriptor<>("test3", new JavaSerializer<MutableType>());
		ListState<MutableType> listState1 = operatorStateBackend.getListState(stateDescriptor1);
		ListState<MutableType> listState2 = operatorStateBackend.getListState(stateDescriptor2);
		ListState<MutableType> listState3 = operatorStateBackend.getUnionListState(stateDescriptor3);

		listState1.add(MutableType.of(42));
		listState1.add(MutableType.of(4711));

		listState2.add(MutableType.of(7));
		listState2.add(MutableType.of(13));
		listState2.add(MutableType.of(23));

		listState3.add(MutableType.of(17));
		listState3.add(MutableType.of(18));
		listState3.add(MutableType.of(19));
		listState3.add(MutableType.of(20));

		BlockerCheckpointStreamFactory streamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);

		OneShotLatch waiterLatch = new OneShotLatch();
		OneShotLatch blockerLatch = new OneShotLatch();

		streamFactory.setWaiterLatch(waiterLatch);
		streamFactory.setBlockerLatch(blockerLatch);

		RunnableFuture<OperatorStateHandle> runnableFuture =
				operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forCheckpoint());

		ExecutorService executorService = Executors.newFixedThreadPool(1);

		executorService.submit(runnableFuture);

		// wait until the async checkpoint is in the write code, then continue
		waiterLatch.await();

		// do some mutations to the state, to test if our snapshot will NOT reflect them

		listState1.add(MutableType.of(77));

		int n = 0;

		for (MutableType mutableType : listState2.get()) {
			if (++n == 2) {
				// allow the write code to continue, so that we could do changes while state is written in parallel.
				blockerLatch.trigger();
			}
			mutableType.setValue(mutableType.getValue() + 10);
		}

		listState3.clear();

		operatorStateBackend.getListState(
				new ListStateDescriptor<>("test4", new JavaSerializer<MutableType>()));

		// run the snapshot
		OperatorStateHandle stateHandle = runnableFuture.get();

		try {

			operatorStateBackend.close();
			operatorStateBackend.dispose();

			AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

			operatorStateBackend = abstractStateBackend.createOperatorStateBackend(
					createMockEnvironment(),
					"testOperator");

			operatorStateBackend.restore(Collections.singletonList(stateHandle));

			assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());

			listState1 = operatorStateBackend.getListState(stateDescriptor1);
			listState2 = operatorStateBackend.getListState(stateDescriptor2);
			listState3 = operatorStateBackend.getUnionListState(stateDescriptor3);

			assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());

			Iterator<MutableType> it = listState1.get().iterator();
			assertEquals(42, it.next().value);
			assertEquals(4711, it.next().value);
			assertFalse(it.hasNext());

			it = listState2.get().iterator();
			assertEquals(7, it.next().value);
			assertEquals(13, it.next().value);
			assertEquals(23, it.next().value);
			assertFalse(it.hasNext());

			it = listState3.get().iterator();
			assertEquals(17, it.next().value);
			assertEquals(18, it.next().value);
			assertEquals(19, it.next().value);
			assertEquals(20, it.next().value);
			assertFalse(it.hasNext());

			operatorStateBackend.close();
			operatorStateBackend.dispose();
		} finally {
			stateHandle.discardState();
		}

		executorService.shutdown();
	}

	@Test
	public void testSnapshotAsyncClose() throws Exception {
		DefaultOperatorStateBackend operatorStateBackend =
				new DefaultOperatorStateBackend(OperatorStateBackendTest.class.getClassLoader(), new ExecutionConfig(), true);

		ListStateDescriptor<MutableType> stateDescriptor1 =
				new ListStateDescriptor<>("test1", new JavaSerializer<MutableType>());

		ListState<MutableType> listState1 = operatorStateBackend.getOperatorState(stateDescriptor1);


		listState1.add(MutableType.of(42));
		listState1.add(MutableType.of(4711));

		BlockerCheckpointStreamFactory streamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);

		OneShotLatch waiterLatch = new OneShotLatch();
		OneShotLatch blockerLatch = new OneShotLatch();

		streamFactory.setWaiterLatch(waiterLatch);
		streamFactory.setBlockerLatch(blockerLatch);

		RunnableFuture<OperatorStateHandle> runnableFuture =
				operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forCheckpoint());

		ExecutorService executorService = Executors.newFixedThreadPool(1);

		executorService.submit(runnableFuture);

		// wait until the async checkpoint is in the write code, then continue
		waiterLatch.await();

		operatorStateBackend.close();

		blockerLatch.trigger();

		try {
			runnableFuture.get(60, TimeUnit.SECONDS);
			Assert.fail();
		} catch (ExecutionException eex) {
			Assert.assertTrue(eex.getCause() instanceof IOException);
		}
	}

	@Test
	public void testSnapshotAsyncCancel() throws Exception {
		DefaultOperatorStateBackend operatorStateBackend =
				new DefaultOperatorStateBackend(OperatorStateBackendTest.class.getClassLoader(), new ExecutionConfig(), true);

		ListStateDescriptor<MutableType> stateDescriptor1 =
				new ListStateDescriptor<>("test1", new JavaSerializer<MutableType>());

		ListState<MutableType> listState1 = operatorStateBackend.getOperatorState(stateDescriptor1);


		listState1.add(MutableType.of(42));
		listState1.add(MutableType.of(4711));

		BlockerCheckpointStreamFactory streamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);

		OneShotLatch waiterLatch = new OneShotLatch();
		OneShotLatch blockerLatch = new OneShotLatch();

		streamFactory.setWaiterLatch(waiterLatch);
		streamFactory.setBlockerLatch(blockerLatch);

		RunnableFuture<OperatorStateHandle> runnableFuture =
				operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forCheckpoint());

		ExecutorService executorService = Executors.newFixedThreadPool(1);

		executorService.submit(runnableFuture);

		// wait until the async checkpoint is in the stream's write code, then continue
		waiterLatch.await();

		// cancel the future, which should close the underlying stream
		runnableFuture.cancel(true);
		Assert.assertTrue(streamFactory.getLastCreatedStream().isClosed());

		// we allow the stream under test to proceed
		blockerLatch.trigger();

		try {
			runnableFuture.get(60, TimeUnit.SECONDS);
			Assert.fail();
		} catch (CancellationException ignore) {
		}
	}

	@Test
	public void testRestoreFailsIfSerializerDeserializationFails() throws Exception {
		AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		OperatorStateBackend operatorStateBackend = abstractStateBackend.createOperatorStateBackend(createMockEnvironment(), "test-op-name");

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
			operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forCheckpoint());
		OperatorStateHandle stateHandle = FutureUtil.runIfNotDoneAndGet(runnableFuture);

		try {

			operatorStateBackend.close();
			operatorStateBackend.dispose();

			operatorStateBackend = abstractStateBackend.createOperatorStateBackend(
				createMockEnvironment(),
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

	static final class MutableType implements Serializable {

		private static final long serialVersionUID = 1L;

		private int value;

		public MutableType() {
			this(0);
		}

		public MutableType(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			MutableType that = (MutableType) o;

			return value == that.value;
		}

		@Override
		public int hashCode() {
			return value;
		}

		static MutableType of(int value) {
			return new MutableType(value);
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static Environment createMockEnvironment() {
		Environment env = mock(Environment.class);
		when(env.getExecutionConfig()).thenReturn(new ExecutionConfig());
		when(env.getUserClassLoader()).thenReturn(OperatorStateBackendTest.class.getClassLoader());
		return env;
	}
}
