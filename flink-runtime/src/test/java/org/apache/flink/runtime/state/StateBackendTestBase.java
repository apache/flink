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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Joiner;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateRegistryListener;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.heap.AbstractHeapState;
import org.apache.flink.runtime.state.heap.NestedMapsStateTable;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Generic tests for the partitioned state part of {@link AbstractStateBackend}.
 */
@SuppressWarnings("serial")
public abstract class StateBackendTestBase<B extends AbstractStateBackend> extends TestLogger {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	protected abstract B getStateBackend() throws Exception;

	protected CheckpointStreamFactory createStreamFactory() throws Exception {
		return getStateBackend().createStreamFactory(new JobID(), "test_op");
	}

	protected <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer) throws Exception {
		return createKeyedBackend(keySerializer, new DummyEnvironment("test", 1, 0));
	}

	protected <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer, Environment env) throws Exception {
		return createKeyedBackend(
				keySerializer,
				10,
				new KeyGroupRange(0, 9),
				env);
	}

	protected <K> AbstractKeyedStateBackend<K> createKeyedBackend(
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			Environment env) throws Exception {

		AbstractKeyedStateBackend<K> backend = getStateBackend().createKeyedStateBackend(
				env,
				new JobID(),
				"test_op",
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				env.getTaskKvStateRegistry());

		backend.restore(null);

		return backend;
	}

	protected <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(TypeSerializer<K> keySerializer, KeyedStateHandle state) throws Exception {
		return restoreKeyedBackend(keySerializer, state, new DummyEnvironment("test", 1, 0));
	}

	protected <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(
			TypeSerializer<K> keySerializer,
			KeyedStateHandle state,
			Environment env) throws Exception {
		return restoreKeyedBackend(
				keySerializer,
				10,
				new KeyGroupRange(0, 9),
				Collections.singletonList(state),
				env);
	}

	protected <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			List<KeyedStateHandle> state,
			Environment env) throws Exception {

		AbstractKeyedStateBackend<K> backend = getStateBackend().createKeyedStateBackend(
				env,
				new JobID(),
				"test_op",
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				env.getTaskKvStateRegistry());

		if (null != state) {
			backend.restore(state);
		}

		return backend;
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testBackendUsesRegisteredKryoDefaultSerializer() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		Environment env = new DummyEnvironment("test", 1, 0);
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);

		// cast because our test serializer is not typed to TestPojo
		env.getExecutionConfig().addDefaultKryoSerializer(TestPojo.class, (Class) ExceptionThrowingTestSerializer.class);

		TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

		// make sure that we are in fact using the KryoSerializer
		assertTrue(pojoType.createSerializer(env.getExecutionConfig()) instanceof KryoSerializer);

		ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

		ValueState<TestPojo> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// we will be expecting ExpectedKryoTestException to be thrown,
		// because the ExceptionThrowingTestSerializer should be used
		int numExceptions = 0;

		backend.setCurrentKey(1);

		try {
			// backends that eagerly serializes (such as RocksDB) will fail here
			state.update(new TestPojo("u1", 1));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		try {
			// backends that lazily serializes (such as memory state backend) will fail here
			runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		assertEquals("Didn't see the expected Kryo exception.", 1, numExceptions);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testBackendUsesRegisteredKryoDefaultSerializerUsingGetOrCreate() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		Environment env = new DummyEnvironment("test", 1, 0);
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);

		// cast because our test serializer is not typed to TestPojo
		env.getExecutionConfig()
				.addDefaultKryoSerializer(TestPojo.class, (Class) ExceptionThrowingTestSerializer.class);

		TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

		// make sure that we are in fact using the KryoSerializer
		assertTrue(pojoType.createSerializer(env.getExecutionConfig()) instanceof KryoSerializer);

		pojoType.createSerializer(env.getExecutionConfig());

		ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

		ValueState<TestPojo> state = backend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, kvId);
		assertTrue(state instanceof InternalValueState);
		((InternalValueState) state).setCurrentNamespace(VoidNamespace.INSTANCE);

		// we will be expecting ExpectedKryoTestException to be thrown,
		// because the ExceptionThrowingTestSerializer should be used
		int numExceptions = 0;

		backend.setCurrentKey(1);

		try {
			// backends that eagerly serializes (such as RocksDB) will fail here
			state.update(new TestPojo("u1", 1));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		try {
			// backends that lazily serializes (such as memory state backend) will fail here
			runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		assertEquals("Didn't see the expected Kryo exception.", 1, numExceptions);
	}

	@Test
	public void testBackendUsesRegisteredKryoSerializer() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		Environment env = new DummyEnvironment("test", 1, 0);
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);

		env.getExecutionConfig()
				.registerTypeWithKryoSerializer(TestPojo.class, ExceptionThrowingTestSerializer.class);

		TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

		// make sure that we are in fact using the KryoSerializer
		assertTrue(pojoType.createSerializer(env.getExecutionConfig()) instanceof KryoSerializer);

		ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

		ValueState<TestPojo> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// we will be expecting ExpectedKryoTestException to be thrown,
		// because the ExceptionThrowingTestSerializer should be used
		int numExceptions = 0;

		backend.setCurrentKey(1);

		try {
			// backends that eagerly serializes (such as RocksDB) will fail here
			state.update(new TestPojo("u1", 1));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		try {
			// backends that lazily serializes (such as memory state backend) will fail here
			runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		assertEquals("Didn't see the expected Kryo exception.", 1, numExceptions);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testBackendUsesRegisteredKryoSerializerUsingGetOrCreate() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		Environment env = new DummyEnvironment("test", 1, 0);
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);

		env.getExecutionConfig().registerTypeWithKryoSerializer(TestPojo.class, ExceptionThrowingTestSerializer.class);

		TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

		// make sure that we are in fact using the KryoSerializer
		assertTrue(pojoType.createSerializer(env.getExecutionConfig()) instanceof KryoSerializer);

		ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

		ValueState<TestPojo> state = backend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, kvId);
		assertTrue(state instanceof InternalValueState);
		((InternalValueState) state).setCurrentNamespace(VoidNamespace.INSTANCE);

		// we will be expecting ExpectedKryoTestException to be thrown,
		// because the ExceptionThrowingTestSerializer should be used
		int numExceptions = 0;

		backend.setCurrentKey(1);

		try {
			// backends that eagerly serializes (such as RocksDB) will fail here
			state.update(new TestPojo("u1", 1));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		try {
			// backends that lazily serializes (such as memory state backend) will fail here
			runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));
		} catch (ExpectedKryoTestException e) {
			numExceptions++;
		} catch (Exception e) {
			if (e.getCause() instanceof ExpectedKryoTestException) {
				numExceptions++;
			} else {
				throw e;
			}
		}

		assertEquals("Didn't see the expected Kryo exception.", 1, numExceptions);
	}


	/**
	 * Verify state restore resilience when:
	 *  - snapshot was taken without any Kryo registrations, specific serializers or default serializers for the state type
	 *  - restored with the state type registered (no specific serializer)
	 *
	 * This test should not fail, because de- / serialization of the state should noth be performed with Kryo's default
	 * {@link com.esotericsoftware.kryo.serializers.FieldSerializer}.
	 */
	@Test
	public void testKryoRegisteringRestoreResilienceWithRegisteredType() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		Environment env = new DummyEnvironment("test", 1, 0);
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);

		TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

		// make sure that we are in fact using the KryoSerializer
		assertTrue(pojoType.createSerializer(env.getExecutionConfig()) instanceof KryoSerializer);

		ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

		ValueState<TestPojo> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// ============== create snapshot - no Kryo registration or specific / default serializers ==============

		// make some more modifications
		backend.setCurrentKey(1);
		state.update(new TestPojo("u1", 1));

		backend.setCurrentKey(2);
		state.update(new TestPojo("u2", 2));

		KeyedStateHandle snapshot = runSnapshot(backend.snapshot(
				682375462378L,
				2,
				streamFactory,
				CheckpointOptions.forFullCheckpoint()));

		backend.dispose();

		// ====================================== restore snapshot  ======================================

		env.getExecutionConfig().registerKryoType(TestPojo.class);
		
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

		snapshot.discardState();

		state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		backend.setCurrentKey(1);
		assertEquals(state.value(), new TestPojo("u1", 1));

		backend.setCurrentKey(2);
		assertEquals(state.value(), new TestPojo("u2", 2));

		backend.dispose();
	}

	/**
	 * Verify state restore resilience when:
	 *  - snapshot was taken without any Kryo registrations, specific serializers or default serializers for the state type
	 *  - restored with a default serializer for the state type
	 *
	 * <p> The default serializer used on restore is {@link CustomKryoTestSerializer}, which deliberately
	 * fails only on deserialization. We use the deliberate deserialization failure to acknowledge test success.
	 *
	 * @throws Exception expects {@link ExpectedKryoTestException} to be thrown.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testKryoRegisteringRestoreResilienceWithDefaultSerializer() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		Environment env = new DummyEnvironment("test", 1, 0);
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);

		TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

		// make sure that we are in fact using the KryoSerializer
		assertTrue(pojoType.createSerializer(env.getExecutionConfig()) instanceof KryoSerializer);

		ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

		ValueState<TestPojo> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// ============== create snapshot - no Kryo registration or specific / default serializers ==============

		// make some more modifications
		backend.setCurrentKey(1);
		state.update(new TestPojo("u1", 1));

		backend.setCurrentKey(2);
		state.update(new TestPojo("u2", 2));

		KeyedStateHandle snapshot = runSnapshot(backend.snapshot(
				682375462378L,
				2,
				streamFactory,
				CheckpointOptions.forFullCheckpoint()));

		backend.dispose();

		// ========== restore snapshot - should use default serializer (ONLY SERIALIZATION) ==========

		// cast because our test serializer is not typed to TestPojo
		env.getExecutionConfig().addDefaultKryoSerializer(TestPojo.class, (Class) CustomKryoTestSerializer.class);

		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

		snapshot.discardState();

		// re-initialize to ensure that we create the KryoSerializer from scratch, otherwise
		// initializeSerializerUnlessSet would not pick up our new config
		kvId = new ValueStateDescriptor<>("id", pojoType);
		state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);

		// update to test state backends that eagerly serialize, such as RocksDB
		state.update(new TestPojo("u1", 11));

		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(
				682375462378L,
				2,
				streamFactory,
				CheckpointOptions.forFullCheckpoint()));

		backend.dispose();

		// ========= restore snapshot - should use default serializer (FAIL ON DESERIALIZATION) =========

		// cast because our test serializer is not typed to TestPojo
		env.getExecutionConfig().addDefaultKryoSerializer(TestPojo.class, (Class) CustomKryoTestSerializer.class);

		// on the second restore, since the custom serializer will be used for
		// deserialization, we expect the deliberate failure to be thrown
		expectedException.expect(ExpectedKryoTestException.class);

		// state backends that eagerly deserializes (such as the memory state backend) will fail here
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2, env);

		state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		// state backends that lazily deserializes (such as RocksDB) will fail here
		state.value();
	}

	/**
	 * Verify state restore resilience when:
	 *  - snapshot was taken without any Kryo registrations, specific serializers or default serializers for the state type
	 *  - restored with a specific serializer for the state type
	 *
	 * <p> The specific serializer used on restore is {@link CustomKryoTestSerializer}, which deliberately
	 * fails only on deserialization. We use the deliberate deserialization failure to acknowledge test success.
	 *
	 * @throws Exception expects {@link ExpectedKryoTestException} to be thrown.
	 */
	@Test
	public void testKryoRegisteringRestoreResilienceWithRegisteredSerializer() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		Environment env = new DummyEnvironment("test", 1, 0);

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);

		TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);

		// make sure that we are in fact using the KryoSerializer
		assertTrue(pojoType.createSerializer(env.getExecutionConfig()) instanceof KryoSerializer);

		ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);
		ValueState<TestPojo> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// ============== create snapshot - no Kryo registration or specific / default serializers ==============

		// make some more modifications
		backend.setCurrentKey(1);
		state.update(new TestPojo("u1", 1));

		backend.setCurrentKey(2);
		state.update(new TestPojo("u2", 2));

		KeyedStateHandle snapshot = runSnapshot(backend.snapshot(
				682375462378L,
				2,
				streamFactory,
				CheckpointOptions.forFullCheckpoint()));

		backend.dispose();

		// ========== restore snapshot - should use specific serializer (ONLY SERIALIZATION) ==========

		env.getExecutionConfig().registerTypeWithKryoSerializer(TestPojo.class, CustomKryoTestSerializer.class);

		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);

		snapshot.discardState();

		// re-initialize to ensure that we create the KryoSerializer from scratch, otherwise
		// initializeSerializerUnlessSet would not pick up our new config
		kvId = new ValueStateDescriptor<>("id", pojoType);
		state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);

		// update to test state backends that eagerly serialize, such as RocksDB
		state.update(new TestPojo("u1", 11));

		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(
				682375462378L,
				2,
				streamFactory,
				CheckpointOptions.forFullCheckpoint()));

		backend.dispose();

		// ========= restore snapshot - should use specific serializer (FAIL ON DESERIALIZATION) =========

		env.getExecutionConfig().registerTypeWithKryoSerializer(TestPojo.class, CustomKryoTestSerializer.class);

		// on the second restore, since the custom serializer will be used for
		// deserialization, we expect the deliberate failure to be thrown
		expectedException.expect(ExpectedKryoTestException.class);

		// state backends that eagerly deserializes (such as the memory state backend) will fail here
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2, env);

		state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		// state backends that lazily deserializes (such as RocksDB) will fail here
		state.value();
	}


	@Test
	@SuppressWarnings("unchecked")
	public void testValueState() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;

		// this is only available after the backend initialized the serializer
		TypeSerializer<String> valueSerializer = kvId.getSerializer();
		
		// some modifications to the state
		backend.setCurrentKey(1);
		assertNull(state.value());
		assertNull(getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.update("1");
		backend.setCurrentKey(2);
		assertNull(state.value());
		assertNull(getSerializedValue(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.update("2");
		backend.setCurrentKey(1);
		assertEquals("1", state.value());
		assertEquals("1", getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		// draw a snapshot
		KeyedStateHandle snapshot1 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// make some more modifications
		backend.setCurrentKey(1);
		state.update("u1");
		backend.setCurrentKey(2);
		state.update("u2");
		backend.setCurrentKey(3);
		state.update("u3");

		// draw another snapshot
		KeyedStateHandle snapshot2 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// validate the original state
		backend.setCurrentKey(1);
		assertEquals("u1", state.value());
		assertEquals("u1", getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("u2", state.value());
		assertEquals("u2", getSerializedValue(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(3);
		assertEquals("u3", state.value());
		assertEquals("u3", getSerializedValue(kvState, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

		snapshot1.discardState();

		ValueState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState1 = (InternalKvState<VoidNamespace>) restored1;

		backend.setCurrentKey(1);
		assertEquals("1", restored1.value());
		assertEquals("1", getSerializedValue(restoredKvState1, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("2", restored1.value());
		assertEquals("2", getSerializedValue(restoredKvState1, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);

		snapshot2.discardState();

		ValueState<String> restored2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState2 = (InternalKvState<VoidNamespace>) restored2;

		backend.setCurrentKey(1);
		assertEquals("u1", restored2.value());
		assertEquals("u1", getSerializedValue(restoredKvState2, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("u2", restored2.value());
		assertEquals("u2", getSerializedValue(restoredKvState2, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(3);
		assertEquals("u3", restored2.value());
		assertEquals("u3", getSerializedValue(restoredKvState2, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
	}

	/**
	 * Tests {@link ValueState#value()} and {@link InternalKvState#getSerializedValue(byte[])}
	 * accessing the state concurrently. They should not get in the way of each
	 * other.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testValueStateRace() throws Exception {
		final AbstractKeyedStateBackend<Integer> backend =
			createKeyedBackend(IntSerializer.INSTANCE);
		final Integer namespace = 1;

		final ValueStateDescriptor<String> kvId =
			new ValueStateDescriptor<>("id", String.class);

		final TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		final TypeSerializer<Integer> namespaceSerializer =
			IntSerializer.INSTANCE;

		final ValueState<String> state = backend
			.getPartitionedState(namespace, IntSerializer.INSTANCE, kvId);

		// this is only available after the backend initialized the serializer
		final TypeSerializer<String> valueSerializer = kvId.getSerializer();

		@SuppressWarnings("unchecked")
		final InternalKvState<Integer> kvState = (InternalKvState<Integer>) state;

		/**
		 * 1) Test that ValueState#value() before and after
		 * KvState#getSerializedValue(byte[]) return the same value.
		 */

		// set some key and namespace
		final int key1 = 1;
		backend.setCurrentKey(key1);
		kvState.setCurrentNamespace(2);
		state.update("2");
		assertEquals("2", state.value());

		// query another key and namespace
		assertNull(getSerializedValue(kvState, 3, keySerializer,
			namespace, IntSerializer.INSTANCE,
			valueSerializer));

		// the state should not have changed!
		assertEquals("2", state.value());

		// re-set values
		kvState.setCurrentNamespace(namespace);

		/**
		 * 2) Test two threads concurrently using ValueState#value() and
		 * KvState#getSerializedValue(byte[]).
		 */

		// some modifications to the state
		final int key2 = 10;
		backend.setCurrentKey(key2);
		assertNull(state.value());
		assertNull(getSerializedValue(kvState, key2, keySerializer,
			namespace, namespaceSerializer, valueSerializer));
		state.update("1");

		final CheckedThread getter = new CheckedThread("State getter") {
			@Override
			public void go() throws Exception {
				while (!isInterrupted()) {
					assertEquals("1", state.value());
				}
			}
		};

		final CheckedThread serializedGetter = new CheckedThread("Serialized state getter") {
			@Override
			public void go() throws Exception {
				while(!isInterrupted() && getter.isAlive()) {
					final String serializedValue =
						getSerializedValue(kvState, key2, keySerializer,
							namespace, namespaceSerializer,
							valueSerializer);
					assertEquals("1", serializedValue);
				}
			}
		};

		getter.start();
		serializedGetter.start();

		// run both threads for max 100ms
		Timer t = new Timer("stopper");
		t.schedule(new TimerTask() {
			@Override
			public void run() {
				getter.interrupt();
				serializedGetter.interrupt();
				this.cancel();
			}
		}, 100);

		// wait for both threads to finish
		try {
			// serializedGetter will finish if its assertion fails or if
			// getter is not alive any more
			serializedGetter.sync();
			// if serializedGetter crashed, getter will not know -> interrupt just in case
			getter.interrupt();
			getter.sync();
			t.cancel(); // if not executed yet
		} finally {
			// clean up
			backend.dispose();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleValueStates() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(
				IntSerializer.INSTANCE,
				1,
				new KeyGroupRange(0, 0),
				new DummyEnvironment("test_op", 1, 0));

		ValueStateDescriptor<String> desc1 = new ValueStateDescriptor<>("a-string", StringSerializer.INSTANCE);
		ValueStateDescriptor<Integer> desc2 = new ValueStateDescriptor<>("an-integer", IntSerializer.INSTANCE);

		ValueState<String> state1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc1);
		ValueState<Integer> state2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc2);

		// some modifications to the state
		backend.setCurrentKey(1);
		assertNull(state1.value());
		assertNull(state2.value());
		state1.update("1");

		// state2 should still have nothing
		assertEquals("1", state1.value());
		assertNull(state2.value());
		state2.update(13);

		// both have some state now
		assertEquals("1", state1.value());
		assertEquals(13, (int) state2.value());

		// draw a snapshot
		KeyedStateHandle snapshot1 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

		backend.dispose();
		backend = restoreKeyedBackend(
				IntSerializer.INSTANCE,
				1,
				new KeyGroupRange(0, 0),
				Collections.singletonList(snapshot1),
				new DummyEnvironment("test_op", 1, 0));

		snapshot1.discardState();

		backend.setCurrentKey(1);

		state1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc1);
		state2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc2);

		// verify that they are still the same
		assertEquals("1", state1.value());
		assertEquals(13, (int) state2.value());

		backend.dispose();
	}

	/**
	 * This test verifies that passing {@code null} to {@link ValueState#update(Object)} acts
	 * the same as {@link ValueState#clear()}.
	 *
	 * @throws Exception
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testValueStateNullUpdate() throws Exception {
		// precondition: LongSerializer must fail on null value. this way the test would fail
		// later if null values where actually stored in the state instead of acting as clear()
		try {
			LongSerializer.INSTANCE.serialize(null,
				new DataOutputViewStreamWrapper(new ByteArrayOutputStream()));
			fail("Should fail with NullPointerException");
		} catch (NullPointerException e) {
			// alrighty
		}

		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<Long> kvId = new ValueStateDescriptor<>("id", LongSerializer.INSTANCE, 42L);

		ValueState<Long> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// some modifications to the state
		backend.setCurrentKey(1);

		// verify default value
		assertEquals(42L, (long) state.value());
		state.update(1L);
		assertEquals(1L, (long) state.value());

		backend.setCurrentKey(2);
		assertEquals(42L, (long) state.value());

		backend.setCurrentKey(1);
		state.clear();
		assertEquals(42L, (long) state.value());

		state.update(17L);
		assertEquals(17L, (long) state.value());

		state.update(null);
		assertEquals(42L, (long) state.value());

		// draw a snapshot
		KeyedStateHandle snapshot1 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

		backend.dispose();
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

		snapshot1.discardState();

		backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.dispose();
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	public void testListState() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

		ListState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;

		// this is only available after the backend initialized the serializer
		TypeSerializer<String> valueSerializer = kvId.getElementSerializer();

		Joiner joiner = Joiner.on(",");
		// some modifications to the state
		backend.setCurrentKey(1);
		assertEquals(null, state.get());
		assertEquals(null, getSerializedList(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.add("1");
		backend.setCurrentKey(2);
		assertEquals(null, state.get());
		assertEquals(null, getSerializedList(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.add("2");
		backend.setCurrentKey(1);
		assertEquals("1", joiner.join(state.get()));
		assertEquals("1", joiner.join(getSerializedList(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// make some more modifications
		backend.setCurrentKey(1);
		state.add("u1");
		backend.setCurrentKey(2);
		state.add("u2");
		backend.setCurrentKey(3);
		state.add("u3");

		// draw another snapshot
		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// validate the original state
		backend.setCurrentKey(1);
		assertEquals("1,u1", joiner.join(state.get()));
		assertEquals("1,u1", joiner.join(getSerializedList(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));
		backend.setCurrentKey(2);
		assertEquals("2,u2", joiner.join(state.get()));
		assertEquals("2,u2", joiner.join(getSerializedList(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));
		backend.setCurrentKey(3);
		assertEquals("u3", joiner.join(state.get()));
		assertEquals("u3", joiner.join(getSerializedList(kvState, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));

		backend.dispose();
		// restore the first snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
		snapshot1.discardState();

		ListState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState1 = (InternalKvState<VoidNamespace>) restored1;

		backend.setCurrentKey(1);
		assertEquals("1", joiner.join(restored1.get()));
		assertEquals("1", joiner.join(getSerializedList(restoredKvState1, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));
		backend.setCurrentKey(2);
		assertEquals("2", joiner.join(restored1.get()));
		assertEquals("2", joiner.join(getSerializedList(restoredKvState1, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));

		backend.dispose();
		// restore the second snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
		snapshot2.discardState();

		ListState<String> restored2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState2 = (InternalKvState<VoidNamespace>) restored2;

		backend.setCurrentKey(1);
		assertEquals("1,u1", joiner.join(restored2.get()));
		assertEquals("1,u1", joiner.join(getSerializedList(restoredKvState2, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));
		backend.setCurrentKey(2);
		assertEquals("2,u2", joiner.join(restored2.get()));
		assertEquals("2,u2", joiner.join(getSerializedList(restoredKvState2, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));
		backend.setCurrentKey(3);
		assertEquals("u3", joiner.join(restored2.get()));
		assertEquals("u3", joiner.join(getSerializedList(restoredKvState2, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer)));

		backend.dispose();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReducingState() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ReducingStateDescriptor<String> kvId = new ReducingStateDescriptor<>("id", new AppendingReduce(), String.class);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

		ReducingState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;

		// this is only available after the backend initialized the serializer
		TypeSerializer<String> valueSerializer = kvId.getSerializer();

		// some modifications to the state
		backend.setCurrentKey(1);
		assertEquals(null, state.get());
		assertNull(getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.add("1");
		backend.setCurrentKey(2);
		assertEquals(null, state.get());
		assertNull(getSerializedValue(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.add("2");
		backend.setCurrentKey(1);
		assertEquals("1", state.get());
		assertEquals("1", getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// make some more modifications
		backend.setCurrentKey(1);
		state.add("u1");
		backend.setCurrentKey(2);
		state.add("u2");
		backend.setCurrentKey(3);
		state.add("u3");

		// draw another snapshot
		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// validate the original state
		backend.setCurrentKey(1);
		assertEquals("1,u1", state.get());
		assertEquals("1,u1", getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("2,u2", state.get());
		assertEquals("2,u2", getSerializedValue(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(3);
		assertEquals("u3", state.get());
		assertEquals("u3", getSerializedValue(kvState, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
		// restore the first snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
		snapshot1.discardState();

		ReducingState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState1 = (InternalKvState<VoidNamespace>) restored1;

		backend.setCurrentKey(1);
		assertEquals("1", restored1.get());
		assertEquals("1", getSerializedValue(restoredKvState1, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("2", restored1.get());
		assertEquals("2", getSerializedValue(restoredKvState1, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
		// restore the second snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
		snapshot2.discardState();

		ReducingState<String> restored2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState2 = (InternalKvState<VoidNamespace>) restored2;

		backend.setCurrentKey(1);
		assertEquals("1,u1", restored2.get());
		assertEquals("1,u1", getSerializedValue(restoredKvState2, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("2,u2", restored2.get());
		assertEquals("2,u2", getSerializedValue(restoredKvState2, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(3);
		assertEquals("u3", restored2.get());
		assertEquals("u3", getSerializedValue(restoredKvState2, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	public void testFoldingState() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		FoldingStateDescriptor<Integer, String> kvId = new FoldingStateDescriptor<>("id",
				"Fold-Initial:",
				new AppendingFold(),
				String.class);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

		FoldingState<Integer, String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;

		// this is only available after the backend initialized the serializer
		TypeSerializer<String> valueSerializer = kvId.getSerializer();

		// some modifications to the state
		backend.setCurrentKey(1);
		assertEquals(null, state.get());
		assertEquals(null, getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.add(1);
		backend.setCurrentKey(2);
		assertEquals(null, state.get());
		assertEquals(null, getSerializedValue(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		state.add(2);
		backend.setCurrentKey(1);
		assertEquals("Fold-Initial:,1", state.get());
		assertEquals("Fold-Initial:,1", getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// make some more modifications
		backend.setCurrentKey(1);
		state.clear();
		state.add(101);
		backend.setCurrentKey(2);
		state.add(102);
		backend.setCurrentKey(3);
		state.add(103);

		// draw another snapshot
		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// validate the original state
		backend.setCurrentKey(1);
		assertEquals("Fold-Initial:,101", state.get());
		assertEquals("Fold-Initial:,101", getSerializedValue(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("Fold-Initial:,2,102", state.get());
		assertEquals("Fold-Initial:,2,102", getSerializedValue(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(3);
		assertEquals("Fold-Initial:,103", state.get());
		assertEquals("Fold-Initial:,103", getSerializedValue(kvState, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
		// restore the first snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
		snapshot1.discardState();

		FoldingState<Integer, String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState1 = (InternalKvState<VoidNamespace>) restored1;

		backend.setCurrentKey(1);
		assertEquals("Fold-Initial:,1", restored1.get());
		assertEquals("Fold-Initial:,1", getSerializedValue(restoredKvState1, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("Fold-Initial:,2", restored1.get());
		assertEquals("Fold-Initial:,2", getSerializedValue(restoredKvState1, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
		// restore the second snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
		snapshot1.discardState();

		@SuppressWarnings("unchecked")
		FoldingState<Integer, String> restored2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState2 = (InternalKvState<VoidNamespace>) restored2;

		backend.setCurrentKey(1);
		assertEquals("Fold-Initial:,101", restored2.get());
		assertEquals("Fold-Initial:,101", getSerializedValue(restoredKvState2, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(2);
		assertEquals("Fold-Initial:,2,102", restored2.get());
		assertEquals("Fold-Initial:,2,102", getSerializedValue(restoredKvState2, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));
		backend.setCurrentKey(3);
		assertEquals("Fold-Initial:,103", restored2.get());
		assertEquals("Fold-Initial:,103", getSerializedValue(restoredKvState2, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, valueSerializer));

		backend.dispose();
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	public void testMapState() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		MapStateDescriptor<Integer, String> kvId = new MapStateDescriptor<>("id", Integer.class, String.class);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

		MapState<Integer, String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;

		// these are only available after the backend initialized the serializer
		TypeSerializer<Integer> userKeySerializer = kvId.getKeySerializer();
		TypeSerializer<String> userValueSerializer = kvId.getValueSerializer();

		// some modifications to the state
		backend.setCurrentKey(1);
		assertEquals(null, state.get(1));
		assertEquals(null, getSerializedMap(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));
		state.put(1, "1");
		backend.setCurrentKey(2);
		assertEquals(null, state.get(2));
		assertEquals(null, getSerializedMap(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));
		state.put(2, "2");
		backend.setCurrentKey(1);
		assertTrue(state.contains(1));
		assertEquals("1", state.get(1));
		assertEquals(new HashMap<Integer, String>() {{ put (1, "1"); }},
				getSerializedMap(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// make some more modifications
		backend.setCurrentKey(1);
		state.put(1, "101");
		backend.setCurrentKey(2);
		state.put(102, "102");
		backend.setCurrentKey(3);
		state.put(103, "103");
		state.putAll(new HashMap<Integer, String>() {{ put(1031, "1031"); put(1032, "1032"); }});

		// draw another snapshot
		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forFullCheckpoint()));

		// validate the original state
		backend.setCurrentKey(1);
		assertEquals("101", state.get(1));
		assertEquals(new HashMap<Integer, String>() {{ put(1, "101"); }},
				getSerializedMap(kvState, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));
		backend.setCurrentKey(2);
		assertEquals("102", state.get(102));
		assertEquals(new HashMap<Integer, String>() {{ put(2, "2"); put(102, "102"); }},
				getSerializedMap(kvState, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));
		backend.setCurrentKey(3);
		assertTrue(state.contains(103));
		assertEquals("103", state.get(103));
		assertEquals(new HashMap<Integer, String>() {{ put(103, "103"); put(1031, "1031"); put(1032, "1032"); }},
				getSerializedMap(kvState, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));

		List<Integer> keys = new ArrayList<>();
		for (Integer key : state.keys()) {
			keys.add(key);
		}
		List<Integer> expectedKeys = new ArrayList<Integer>() {{ add(103); add(1031); add(1032); }};
		assertEquals(keys.size(), expectedKeys.size());
		keys.removeAll(expectedKeys);
		assertTrue(keys.isEmpty());

		List<String> values = new ArrayList<>();
		for (String value : state.values()) {
			values.add(value);
		}
		List<String> expectedValues = new ArrayList<String>() {{ add("103"); add("1031"); add("1032"); }};
		assertEquals(values.size(), expectedValues.size());
		values.removeAll(expectedValues);
		assertTrue(values.isEmpty());

		// make some more modifications
		backend.setCurrentKey(1);
		state.clear();
		backend.setCurrentKey(2);
		state.remove(102);
		backend.setCurrentKey(3);
		final String updateSuffix = "_updated";
		Iterator<Map.Entry<Integer, String>> iterator = state.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, String> entry = iterator.next();
			if (entry.getValue().length() != 4) {
				iterator.remove();
			} else {
				entry.setValue(entry.getValue() + updateSuffix);
			}
		}

		// validate the state
		backend.setCurrentKey(1);
		backend.setCurrentKey(2);
		assertFalse(state.contains(102));
		backend.setCurrentKey(3);
		for (Map.Entry<Integer, String> entry : state.entries()) {
			assertEquals(4 + updateSuffix.length(), entry.getValue().length());
			assertTrue(entry.getValue().endsWith(updateSuffix));
		}

		backend.dispose();
		// restore the first snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
		snapshot1.discardState();

		MapState<Integer, String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState1 = (InternalKvState<VoidNamespace>) restored1;

		backend.setCurrentKey(1);
		assertEquals("1", restored1.get(1));
		assertEquals(new HashMap<Integer, String>() {{ put (1, "1"); }},
				getSerializedMap(restoredKvState1, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));
		backend.setCurrentKey(2);
		assertEquals("2", restored1.get(2));
		assertEquals(new HashMap<Integer, String>() {{ put (2, "2"); }},
				getSerializedMap(restoredKvState1, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));

		backend.dispose();
		// restore the second snapshot and validate it
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
		snapshot2.discardState();

		@SuppressWarnings("unchecked")
		MapState<Integer, String> restored2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<VoidNamespace> restoredKvState2 = (InternalKvState<VoidNamespace>) restored2;

		backend.setCurrentKey(1);
		assertEquals("101", restored2.get(1));
		assertEquals(new HashMap<Integer, String>() {{ put (1, "101"); }},
				getSerializedMap(restoredKvState2, 1, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));
		backend.setCurrentKey(2);
		assertEquals("102", restored2.get(102));
		assertEquals(new HashMap<Integer, String>() {{ put(2, "2"); put (102, "102"); }},
				getSerializedMap(restoredKvState2, 2, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));
		backend.setCurrentKey(3);
		assertEquals("103", restored2.get(103));
		assertEquals(new HashMap<Integer, String>() {{ put(103, "103"); put(1031, "1031"); put(1032, "1032"); }},
				getSerializedMap(restoredKvState2, 3, keySerializer, VoidNamespace.INSTANCE, namespaceSerializer, userKeySerializer, userValueSerializer));

		backend.dispose();
	}

	/**
	 * Verify that {@link ValueStateDescriptor} allows {@code null} as default.
	 */
	@Test
	public void testValueStateNullAsDefaultValue() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		assertEquals(null, state.value());

		state.update("Ciao");
		assertEquals("Ciao", state.value());

		state.clear();
		assertEquals(null, state.value());

		backend.dispose();
	}


	/**
	 * Verify that an empty {@code ValueState} will yield the default value.
	 */
	@Test
	public void testValueStateDefaultValue() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, "Hello");

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		assertEquals("Hello", state.value());

		state.update("Ciao");
		assertEquals("Ciao", state.value());

		state.clear();
		assertEquals("Hello", state.value());

		backend.dispose();
	}

	/**
	 * Verify that an empty {@code ReduceState} yields {@code null}.
	 */
	@Test
	public void testReducingStateDefaultValue() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ReducingStateDescriptor<String> kvId = new ReducingStateDescriptor<>("id", new AppendingReduce(), String.class);

		ReducingState<String> state = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		assertNull(state.get());

		state.add("Ciao");
		assertEquals("Ciao", state.get());

		state.clear();
		assertNull(state.get());

		backend.dispose();
	}

	/**
	 * Verify that an empty {@code FoldingState} yields {@code null}.
	 */
	@Test
	public void testFoldingStateDefaultValue() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		FoldingStateDescriptor<Integer, String> kvId =
				new FoldingStateDescriptor<>("id", "Fold-Initial:", new AppendingFold(), String.class);

		FoldingState<Integer, String> state = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		assertNull(state.get());

		state.add(1);
		state.add(2);
		assertEquals("Fold-Initial:,1,2", state.get());

		state.clear();
		assertNull(state.get());

		backend.dispose();
	}


	/**
	 * Verify that an empty {@code ListState} yields {@code null}.
	 */
	@Test
	public void testListStateDefaultValue() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

		ListState<String> state = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		assertNull(state.get());

		state.add("Ciao");
		state.add("Bello");
		assertThat(state.get(), containsInAnyOrder("Ciao", "Bello"));

		state.clear();
		assertNull(state.get());

		backend.dispose();
	}

	/**
	 * Verify that an empty {@code MapState} yields {@code null}.
	 */
	@Test
	public void testMapStateDefaultValue() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		MapStateDescriptor<String, String> kvId = new MapStateDescriptor<>("id", String.class, String.class);

		MapState<String, String> state = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		assertNull(state.entries());

		state.put("Ciao", "Hello");
		state.put("Bello", "Nice");

		assertNotNull(state.entries());
		assertEquals(state.get("Ciao"), "Hello");
		assertEquals(state.get("Bello"), "Nice");

		state.clear();
		assertNull(state.entries());

		backend.dispose();
	}

	/**
	 * This test verifies that state is correctly assigned to key groups and that restore
	 * restores the relevant key groups in the backend.
	 *
	 * <p>We have ten key groups. Initially, one backend is responsible for all ten key groups.
	 * Then we snapshot, split up the state and restore in to backends where each is responsible
	 * for five key groups. Then we make sure that the state is only available in the correct
	 * backend.
	 * @throws Exception
	 */
	@Test
	public void testKeyGroupSnapshotRestore() throws Exception {
		final int MAX_PARALLELISM = 10;

		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(
				IntSerializer.INSTANCE,
				MAX_PARALLELISM,
				new KeyGroupRange(0, MAX_PARALLELISM - 1),
				new DummyEnvironment("test", 1, 0));

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		// keys that fall into the first half/second half of the key groups, respectively
		int keyInFirstHalf = 17;
		int keyInSecondHalf = 42;
		Random rand = new Random(0);

		// for each key, determine into which half of the key-group space they fall
		int firstKeyHalf = KeyGroupRangeAssignment.assignKeyToParallelOperator(keyInFirstHalf, MAX_PARALLELISM, 2);
		int secondKeyHalf = KeyGroupRangeAssignment.assignKeyToParallelOperator(keyInFirstHalf, MAX_PARALLELISM, 2);

		while (firstKeyHalf == secondKeyHalf) {
			keyInSecondHalf = rand.nextInt();
			secondKeyHalf = KeyGroupRangeAssignment.assignKeyToParallelOperator(keyInSecondHalf, MAX_PARALLELISM, 2);
		}

		backend.setCurrentKey(keyInFirstHalf);
		state.update("ShouldBeInFirstHalf");

		backend.setCurrentKey(keyInSecondHalf);
		state.update("ShouldBeInSecondHalf");


		KeyedStateHandle snapshot = FutureUtil.runIfNotDoneAndGet(backend.snapshot(0, 0, streamFactory, CheckpointOptions.forFullCheckpoint()));

		List<KeyedStateHandle> firstHalfKeyGroupStates = StateAssignmentOperation.getKeyedStateHandles(
				Collections.singletonList(snapshot),
				KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(MAX_PARALLELISM, 2, 0));

		List<KeyedStateHandle> secondHalfKeyGroupStates = StateAssignmentOperation.getKeyedStateHandles(
				Collections.singletonList(snapshot),
				KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(MAX_PARALLELISM, 2, 1));

		backend.dispose();

		// backend for the first half of the key group range
		AbstractKeyedStateBackend<Integer> firstHalfBackend = restoreKeyedBackend(
				IntSerializer.INSTANCE,
				MAX_PARALLELISM,
				new KeyGroupRange(0, 4),
				firstHalfKeyGroupStates,
				new DummyEnvironment("test", 1, 0));

		// backend for the second half of the key group range
		AbstractKeyedStateBackend<Integer> secondHalfBackend = restoreKeyedBackend(
				IntSerializer.INSTANCE,
				MAX_PARALLELISM,
				new KeyGroupRange(5, 9),
				secondHalfKeyGroupStates,
				new DummyEnvironment("test", 1, 0));


		ValueState<String> firstHalfState = firstHalfBackend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		firstHalfBackend.setCurrentKey(keyInFirstHalf);
		assertTrue(firstHalfState.value().equals("ShouldBeInFirstHalf"));

		firstHalfBackend.setCurrentKey(keyInSecondHalf);
		assertTrue(firstHalfState.value() == null);

		ValueState<String> secondHalfState = secondHalfBackend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		secondHalfBackend.setCurrentKey(keyInFirstHalf);
		assertTrue(secondHalfState.value() == null);

		secondHalfBackend.setCurrentKey(keyInSecondHalf);
		assertTrue(secondHalfState.value().equals("ShouldBeInSecondHalf"));

		firstHalfBackend.dispose();
		secondHalfBackend.dispose();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testValueStateRestoreWithWrongSerializers() {
		try {
			CheckpointStreamFactory streamFactory = createStreamFactory();
			AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

			ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

			ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.update("1");
			backend.setCurrentKey(2);
			state.update("2");

			// draw a snapshot
			KeyedStateHandle snapshot1 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

			backend.dispose();
			// restore the first snapshot and validate it
			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
			snapshot1.discardState();

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
				(TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				kvId = new ValueStateDescriptor<>("id", fakeStringSerializer);

				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				state.value();

				fail("should recognize wrong serializers");
			} catch (IOException e) {
				if (!e.getMessage().contains("Trying to access state using wrong")) {
					fail("wrong exception " + e);
				}
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}
			backend.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testListStateRestoreWithWrongSerializers() {
		try {
			CheckpointStreamFactory streamFactory = createStreamFactory();
			AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

			ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);
			ListState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.add("1");
			backend.setCurrentKey(2);
			state.add("2");

			// draw a snapshot
			KeyedStateHandle snapshot1 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

			backend.dispose();
			// restore the first snapshot and validate it
			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
			snapshot1.discardState();

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
					(TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				kvId = new ListStateDescriptor<>("id", fakeStringSerializer);

				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				state.get();

				fail("should recognize wrong serializers");
			} catch (IOException e) {
				if (!e.getMessage().contains("Trying to access state using wrong")) {
					fail("wrong exception " + e);
				}
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}
			backend.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReducingStateRestoreWithWrongSerializers() {
		try {
			CheckpointStreamFactory streamFactory = createStreamFactory();
			AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

			ReducingStateDescriptor<String> kvId = new ReducingStateDescriptor<>("id",
					new AppendingReduce(),
					StringSerializer.INSTANCE);
			ReducingState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.add("1");
			backend.setCurrentKey(2);
			state.add("2");

			// draw a snapshot
			KeyedStateHandle snapshot1 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

			backend.dispose();
			// restore the first snapshot and validate it
			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
			snapshot1.discardState();

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
					(TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				kvId = new ReducingStateDescriptor<>("id", new AppendingReduce(), fakeStringSerializer);

				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				state.get();

				fail("should recognize wrong serializers");
			} catch (IOException e) {
				if (!e.getMessage().contains("Trying to access state using wrong ")) {
					fail("wrong exception " + e);
				}
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}
			backend.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMapStateRestoreWithWrongSerializers() {
		try {
			CheckpointStreamFactory streamFactory = createStreamFactory();
			AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

			MapStateDescriptor<String, String> kvId = new MapStateDescriptor<>("id", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
			MapState<String, String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.put("1", "First");
			backend.setCurrentKey(2);
			state.put("2", "Second");

			// draw a snapshot
			KeyedStateHandle snapshot1 = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forFullCheckpoint()));

			backend.dispose();
			// restore the first snapshot and validate it
			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
			snapshot1.discardState();

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
					(TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				kvId = new MapStateDescriptor<>("id", fakeStringSerializer, StringSerializer.INSTANCE);

				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				state.entries();

				fail("should recognize wrong serializers");
			} catch (IOException e) {
				if (!e.getMessage().contains("Trying to access state using wrong ")) {
					fail("wrong exception " + e);
				}
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}
			backend.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	@Test
	public void testCopyDefaultValue() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<IntValue> kvId = new ValueStateDescriptor<>("id", IntValue.class, new IntValue(-1));

		ValueState<IntValue> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		IntValue default1 = state.value();

		backend.setCurrentKey(2);
		IntValue default2 = state.value();

		assertNotNull(default1);
		assertNotNull(default2);
		assertEquals(default1, default2);
		assertFalse(default1 == default2);

		backend.dispose();
	}

	/**
	 * Previously, it was possible to create partitioned state with
	 * <code>null</code> namespace. This test makes sure that this is
	 * prohibited now.
	 */
	@Test
	public void testRequireNonNullNamespace() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<IntValue> kvId = new ValueStateDescriptor<>("id", IntValue.class, new IntValue(-1));

		try {
			backend.getPartitionedState(null, VoidNamespaceSerializer.INSTANCE, kvId);
			fail("Did not throw expected NullPointerException");
		} catch (NullPointerException ignored) {
		}

		try {
			backend.getPartitionedState(VoidNamespace.INSTANCE, null, kvId);
			fail("Did not throw expected NullPointerException");
		} catch (NullPointerException ignored) {
		}

		try {
			backend.getPartitionedState(null, null, kvId);
			fail("Did not throw expected NullPointerException");
		} catch (NullPointerException ignored) {
		}

		backend.dispose();
	}

	/**
	 * Tests that {@link AbstractHeapState} instances respect the queryable
	 * flag and create concurrent variants for internal state structures.
	 */
	@SuppressWarnings("unchecked")
	protected void testConcurrentMapIfQueryable() throws Exception {
		final int numberOfKeyGroups = 1;
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(
				IntSerializer.INSTANCE,
				numberOfKeyGroups,
				new KeyGroupRange(0, 0),
				new DummyEnvironment("test_op", 1, 0));

		{
			// ValueState
			ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>(
					"value-state",
					Integer.class,
					-1);
			desc.setQueryable("my-query");

			ValueState<Integer> state = backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					desc);

			InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;
			assertTrue(kvState instanceof AbstractHeapState);

			kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
			backend.setCurrentKey(1);
			state.update(121818273);

			StateTable<?, ?, ?> stateTable = ((AbstractHeapState<?, ?,? ,?, ?>) kvState).getStateTable();
			checkConcurrentStateTable(stateTable, numberOfKeyGroups);

		}

		{
			// ListState
			ListStateDescriptor<Integer> desc = new ListStateDescriptor<>("list-state", Integer.class);
			desc.setQueryable("my-query");

			ListState<Integer> state = backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					desc);

			InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;
			assertTrue(kvState instanceof AbstractHeapState);

			kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
			backend.setCurrentKey(1);
			state.add(121818273);

			StateTable<?, ?, ?> stateTable = ((AbstractHeapState<?, ?,? ,?, ?>) kvState).getStateTable();
			checkConcurrentStateTable(stateTable, numberOfKeyGroups);
		}

		{
			// ReducingState
			ReducingStateDescriptor<Integer> desc = new ReducingStateDescriptor<>(
					"reducing-state", new ReduceFunction<Integer>() {
				@Override
				public Integer reduce(Integer value1, Integer value2) throws Exception {
					return value1 + value2;
				}
			}, Integer.class);
			desc.setQueryable("my-query");

			ReducingState<Integer> state = backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					desc);

			InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;
			assertTrue(kvState instanceof AbstractHeapState);

			kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
			backend.setCurrentKey(1);
			state.add(121818273);

			StateTable<?, ?, ?> stateTable = ((AbstractHeapState<?, ?,? ,?, ?>) kvState).getStateTable();
			checkConcurrentStateTable(stateTable, numberOfKeyGroups);
		}

		{
			// FoldingState
			FoldingStateDescriptor<Integer, Integer> desc = new FoldingStateDescriptor<>(
					"folding-state", 0, new FoldFunction<Integer, Integer>() {
				@Override
				public Integer fold(Integer accumulator, Integer value) throws Exception {
					return accumulator + value;
				}
			}, Integer.class);
			desc.setQueryable("my-query");

			FoldingState<Integer, Integer> state = backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					desc);

			InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;
			assertTrue(kvState instanceof AbstractHeapState);

			kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
			backend.setCurrentKey(1);
			state.add(121818273);

			StateTable<?, ?, ?> stateTable = ((AbstractHeapState<?, ?,? ,?, ?>) kvState).getStateTable();
			checkConcurrentStateTable(stateTable, numberOfKeyGroups);
		}

		{
			// MapState
			MapStateDescriptor<Integer, String> desc = new MapStateDescriptor<>("map-state", Integer.class, String.class);
			desc.setQueryable("my-query");

			MapState<Integer, String> state = backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					desc);

			InternalKvState<VoidNamespace> kvState = (InternalKvState<VoidNamespace>) state;
			assertTrue(kvState instanceof AbstractHeapState);

			kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
			backend.setCurrentKey(1);
			state.put(121818273, "121818273");

			int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(1, numberOfKeyGroups);
			StateTable stateTable = ((AbstractHeapState) kvState).getStateTable();
			assertNotNull("State not set", stateTable.get(keyGroupIndex));
			checkConcurrentStateTable(stateTable, numberOfKeyGroups);
		}

		backend.dispose();
	}

	private void checkConcurrentStateTable(StateTable<?, ?, ?> stateTable, int numberOfKeyGroups) {
		assertNotNull("State not set", stateTable);
		if (stateTable instanceof NestedMapsStateTable) {
			int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(1, numberOfKeyGroups);
			NestedMapsStateTable<?, ?, ?> nestedMapsStateTable = (NestedMapsStateTable<?, ?, ?>) stateTable;
			assertTrue(nestedMapsStateTable.getState()[keyGroupIndex] instanceof ConcurrentHashMap);
			assertTrue(nestedMapsStateTable.getState()[keyGroupIndex].get(VoidNamespace.INSTANCE) instanceof ConcurrentHashMap);
		}
	}

	/**
	 * Tests registration with the KvStateRegistry.
	 */
	@Test
	public void testQueryableStateRegistration() throws Exception {
		DummyEnvironment env = new DummyEnvironment("test", 1, 0);
		KvStateRegistry registry = env.getKvStateRegistry();

		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, env);
		KeyGroupRange expectedKeyGroupRange = backend.getKeyGroupRange();

		KvStateRegistryListener listener = mock(KvStateRegistryListener.class);
		registry.registerListener(listener);

		ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>(
				"test",
				IntSerializer.INSTANCE);
		desc.setQueryable("banana");

		backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

		// Verify registered
		verify(listener, times(1)).notifyKvStateRegistered(
				eq(env.getJobID()), eq(env.getJobVertexId()), eq(expectedKeyGroupRange), eq("banana"), any(KvStateID.class));


		KeyedStateHandle snapshot = FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forFullCheckpoint()));

		backend.dispose();

		verify(listener, times(1)).notifyKvStateUnregistered(
				eq(env.getJobID()), eq(env.getJobVertexId()), eq(expectedKeyGroupRange), eq("banana"));
		backend.dispose();
		// Initialize again
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);
		snapshot.discardState();

		backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

		// Verify registered again
		verify(listener, times(2)).notifyKvStateRegistered(
				eq(env.getJobID()), eq(env.getJobVertexId()), eq(expectedKeyGroupRange), eq("banana"), any(KvStateID.class));

		backend.dispose();

	}

	@Test
	public void testEmptyStateCheckpointing() {

		try {
			CheckpointStreamFactory streamFactory = createStreamFactory();
			AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

			ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);

			// draw a snapshot
			KeyedStateHandle snapshot =
					FutureUtil.runIfNotDoneAndGet(backend.snapshot(682375462379L, 1, streamFactory, CheckpointOptions.forFullCheckpoint()));
			assertNull(snapshot);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);
			backend.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static class AppendingReduce implements ReduceFunction<String> {
		@Override
		public String reduce(String value1, String value2) throws Exception {
			return value1 + "," + value2;
		}
	}

	@Test
	public void testAsyncSnapshot() throws Exception {
		OneShotLatch waiter = new OneShotLatch();
		BlockerCheckpointStreamFactory streamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
		streamFactory.setWaiterLatch(waiter);

		AbstractKeyedStateBackend<Integer> backend = null;
		KeyedStateHandle stateHandle = null;

		try {
			backend = createKeyedBackend(IntSerializer.INSTANCE);
			InternalValueState<VoidNamespace, Integer> valueState = backend.createValueState(
					VoidNamespaceSerializer.INSTANCE,
					new ValueStateDescriptor<>("test", IntSerializer.INSTANCE));

			valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

			for (int i = 0; i < 10; ++i) {
				backend.setCurrentKey(i);
				valueState.update(i);
			}

			RunnableFuture<KeyedStateHandle> snapshot =
					backend.snapshot(0L, 0L, streamFactory, CheckpointOptions.forFullCheckpoint());
			Thread runner = new Thread(snapshot);
			runner.start();
			for (int i = 0; i < 20; ++i) {
				backend.setCurrentKey(i);
				valueState.update(i + 1);
				if (10 == i) {
					waiter.await();
				}
			}

			runner.join();
			stateHandle = snapshot.get();

			// test isolation
			for (int i = 0; i < 20; ++i) {
				backend.setCurrentKey(i);
				Assert.assertEquals(i + 1, (int) valueState.value());
			}

		} finally {
			if (null != backend) {
				IOUtils.closeQuietly(backend);
				backend.dispose();
			}
		}

		Assert.assertNotNull(stateHandle);

		backend = null;

		try {
			backend = restoreKeyedBackend(IntSerializer.INSTANCE, stateHandle);

			InternalValueState<VoidNamespace, Integer> valueState = backend.createValueState(
					VoidNamespaceSerializer.INSTANCE,
					new ValueStateDescriptor<>("test", IntSerializer.INSTANCE));

			valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

			for (int i = 0; i < 10; ++i) {
				backend.setCurrentKey(i);
				Assert.assertEquals(i, (int) valueState.value());
			}

			backend.setCurrentKey(11);
			Assert.assertEquals(null, valueState.value());
		} finally {
			if (null != backend) {
				IOUtils.closeQuietly(backend);
				backend.dispose();
			}
		}
	}

	@Test
	public void testAsyncSnapshotCancellation() throws Exception {
		OneShotLatch blocker = new OneShotLatch();
		OneShotLatch waiter = new OneShotLatch();
		BlockerCheckpointStreamFactory streamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
		streamFactory.setWaiterLatch(waiter);
		streamFactory.setBlockerLatch(blocker);
		streamFactory.setAfterNumberInvocations(10);

		AbstractKeyedStateBackend<Integer> backend = null;
		try {
			backend = createKeyedBackend(IntSerializer.INSTANCE);

			if (!backend.supportsAsynchronousSnapshots()) {
				return;
			}

			InternalValueState<VoidNamespace, Integer> valueState = backend.createValueState(
					VoidNamespaceSerializer.INSTANCE,
					new ValueStateDescriptor<>("test", IntSerializer.INSTANCE));

			valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

			for (int i = 0; i < 10; ++i) {
				backend.setCurrentKey(i);
				valueState.update(i);
			}

			RunnableFuture<KeyedStateHandle> snapshot =
					backend.snapshot(0L, 0L, streamFactory, CheckpointOptions.forFullCheckpoint());

			Thread runner = new Thread(snapshot);
			runner.start();

			// wait until the code reached some stream read
			waiter.await();

			// close the backend to see if the close is propagated to the stream
			backend.close();

			//unblock the stream so that it can run into the IOException
			blocker.trigger();

			//dispose the backend
			backend.dispose();

			runner.join();

			try {
				snapshot.get();
				fail("Close was not propagated.");
			} catch (ExecutionException ex) {
				//ignore
			}

		} finally {
			if (null != backend) {
				IOUtils.closeQuietly(backend);
				backend.dispose();
			}
		}
	}

	private static class AppendingFold implements FoldFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String acc, Integer value) throws Exception {
			return acc + "," + value;
		}
	}

	/**
	 * Returns the value by getting the serialized value and deserializing it
	 * if it is not null.
	 */
	protected static <V, K, N> V getSerializedValue(
			InternalKvState<N> kvState,
			K key,
			TypeSerializer<K> keySerializer,
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer) throws Exception {

		byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				key, keySerializer, namespace, namespaceSerializer);

		byte[] serializedValue = kvState.getSerializedValue(serializedKeyAndNamespace);

		if (serializedValue == null) {
			return null;
		} else {
			return KvStateRequestSerializer.deserializeValue(serializedValue, valueSerializer);
		}
	}

	/**
	 * Returns the value by getting the serialized value and deserializing it
	 * if it is not null.
	 */
	private static <V, K, N> List<V> getSerializedList(
			InternalKvState<N> kvState,
			K key,
			TypeSerializer<K> keySerializer,
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer) throws Exception {

		byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				key, keySerializer, namespace, namespaceSerializer);

		byte[] serializedValue = kvState.getSerializedValue(serializedKeyAndNamespace);

		if (serializedValue == null) {
			return null;
		} else {
			return KvStateRequestSerializer.deserializeList(serializedValue, valueSerializer);
		}
	}

	/**
	 * Returns the value by getting the serialized value and deserializing it
	 * if it is not null.
	 */
	private static <UK, UV, K, N> Map<UK, UV> getSerializedMap(
			InternalKvState<N> kvState,
			K key,
			TypeSerializer<K> keySerializer,
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<UK> userKeySerializer,
			TypeSerializer<UV> userValueSerializer
	) throws Exception {

		byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				key, keySerializer, namespace, namespaceSerializer);

		byte[] serializedValue = kvState.getSerializedValue(serializedKeyAndNamespace);

		if (serializedValue == null) {
			return null;
		} else {
			return KvStateRequestSerializer.deserializeMap(serializedValue, userKeySerializer, userValueSerializer);
		}
	}

	private KeyedStateHandle runSnapshot(RunnableFuture<KeyedStateHandle> snapshotRunnableFuture) throws Exception {
		if(!snapshotRunnableFuture.isDone()) {
			Thread runner = new Thread(snapshotRunnableFuture);
			runner.start();
		}
		return snapshotRunnableFuture.get();
	}

	private static class TestPojo implements Serializable {
		private String strField;
		private Integer intField;

		public TestPojo() {}

		public TestPojo(String strField, Integer intField) {
			this.strField = strField;
			this.intField = intField;
		}

		public String getStrField() {
			return strField;
		}

		public void setStrField(String strField) {
			this.strField = strField;
		}

		public Integer getIntField() {
			return intField;
		}

		public void setIntField(Integer intField) {
			this.intField = intField;
		}

		@Override
		public String toString() {
			return "TestPojo{" +
					"strField='" + strField + '\'' +
					", intField=" + intField +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestPojo testPojo = (TestPojo) o;

			if (!strField.equals(testPojo.strField)) return false;
			return intField.equals(testPojo.intField);
		}

		@Override
		public int hashCode() {
			int result = strField.hashCode();
			result = 31 * result + intField.hashCode();
			return result;
		}
	}

	/**
	 * We throw this in our {@link ExceptionThrowingTestSerializer}.
	 */
	private static class ExpectedKryoTestException extends RuntimeException {}

	/**
	 * Kryo {@code Serializer} that throws an expected exception. We use this to ensure
	 * that the state backend correctly uses a specified Kryo serializer.
	 */
	public static class ExceptionThrowingTestSerializer extends JavaSerializer {
		@Override
		public void write(Kryo kryo, Output output, Object object) {
			throw new ExpectedKryoTestException();
		}

		@Override
		public Object read(Kryo kryo, Input input, Class type) {
			throw new ExpectedKryoTestException();
		}
	}

	/**
	 * Our custom version of {@link JavaSerializer} for checking whether restore with a registered
	 * serializer works when no serializer was previously registered.
	 *
	 * <p>This {@code Serializer} can only be used for writing, not for reading. With this we
	 * verify that state that was serialized without a registered {@code Serializer} is in fact
	 * not restored with a {@code Serializer} that was later registered.
	 */
	public static class CustomKryoTestSerializer extends JavaSerializer {
		@Override
		public void write(Kryo kryo, Output output, Object object) {
			super.write(kryo, output, object);
		}

		@Override
		public Object read(Kryo kryo, Input input, Class type) {
			throw new ExpectedKryoTestException();
		}
	}
}
