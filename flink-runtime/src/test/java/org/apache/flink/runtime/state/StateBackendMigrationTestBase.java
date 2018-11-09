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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;

/**
 * Tests for the {@link KeyedStateBackend} and {@link OperatorStateBackend} as produced
 * by various {@link StateBackend}s.
 *
 * <p>The tests in this test base focuses on the verification of state serializers usage when they are
 * either compatible or requiring state migration after restoring the state backends.
 */
@SuppressWarnings("serial")
public abstract class StateBackendMigrationTestBase<B extends AbstractStateBackend> extends TestLogger {

	protected abstract B getStateBackend() throws Exception;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	// lazily initialized stream storage
	private CheckpointStorageLocation checkpointStorageLocation;

	/**
	 * The compatibility behaviour of {@link TestSerializer}.
	 * This controls what format the serializer writes in, as well as
	 * the result of the compatibility check against the prior serializer snapshot.
	 */
	public enum SerializerCompatibilityType {
		COMPATIBLE_AS_IS,
		REQUIRES_MIGRATION,
		INCOMPATIBLE
	}

	// -------------------------------------------------------------------------------
	//  Keyed state backend migration tests
	// -------------------------------------------------------------------------------

	@Test
	public void testKeyedValueStateMigration() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		final String stateName = "test-name";
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			ValueStateDescriptor<TestType> kvId = new ValueStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ValueState<TestType> valueState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			valueState.update(new TestType("foo", 1456));
			backend.setCurrentKey(2);
			valueState.update(new TestType("bar", 478));
			backend.setCurrentKey(3);
			valueState.update(new TestType("hello", 189));

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

			// the new serializer is REQUIRES_MIGRATION, and has a completely new serialization schema.
			kvId = new ValueStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION));
			valueState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			snapshot.discardState();

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each key state works with the new serializer
			backend.setCurrentKey(1);
			Assert.assertEquals(new TestType("foo", 1456), valueState.value());
			valueState.update(new TestType("newValue1", 751));

			backend.setCurrentKey(2);
			Assert.assertEquals(new TestType("bar", 478), valueState.value());
			valueState.update(new TestType("newValue2", 167));

			backend.setCurrentKey(3);
			Assert.assertEquals(new TestType("hello", 189), valueState.value());
			valueState.update(new TestType("newValue3", 444));
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testKeyedListStateMigration() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		final String stateName = "test-name";
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			ListStateDescriptor<TestType> kvId = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ListState<TestType> listState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			listState.add(new TestType("key-1", 1));
			listState.add(new TestType("key-1", 2));
			listState.add(new TestType("key-1", 3));

			backend.setCurrentKey(2);
			listState.add(new TestType("key-2", 1));

			backend.setCurrentKey(3);
			listState.add(new TestType("key-3", 1));
			listState.add(new TestType("key-3", 2));

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

			// the new serializer is REQUIRES_MIGRATION, and has a completely new serialization schema.
			kvId = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION));
			listState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			snapshot.discardState();

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each key state works with the new serializer
			backend.setCurrentKey(1);
			Iterator<TestType> iterable1 = listState.get().iterator();
			Assert.assertEquals(new TestType("key-1", 1), iterable1.next());
			Assert.assertEquals(new TestType("key-1", 2), iterable1.next());
			Assert.assertEquals(new TestType("key-1", 3), iterable1.next());
			Assert.assertFalse(iterable1.hasNext());
			listState.add(new TestType("new-key-1", 123));

			backend.setCurrentKey(2);
			Iterator<TestType> iterable2 = listState.get().iterator();
			Assert.assertEquals(new TestType("key-2", 1), iterable2.next());
			Assert.assertFalse(iterable2.hasNext());
			listState.add(new TestType("new-key-2", 456));

			backend.setCurrentKey(3);
			Iterator<TestType> iterable3 = listState.get().iterator();
			Assert.assertEquals(new TestType("key-3", 1), iterable3.next());
			Assert.assertEquals(new TestType("key-3", 2), iterable3.next());
			Assert.assertFalse(iterable3.hasNext());
			listState.add(new TestType("new-key-3", 777));
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testKeyedValueStateRegistrationFailsIfNewStateSerializerIsIncompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		final String stateName = "test-name";
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			ValueStateDescriptor<TestType> kvId = new ValueStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ValueState<TestType> valueState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			valueState.update(new TestType("foo", 1456));
			backend.setCurrentKey(2);
			valueState.update(new TestType("bar", 478));
			backend.setCurrentKey(3);
			valueState.update(new TestType("hello", 189));

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

			kvId = new ValueStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.INCOMPATIBLE));

			// the new serializer is INCOMPATIBLE, so registering the state should fail
			backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		}finally {
			backend.dispose();
		}
	}

	@Test
	public void testKeyedListStateRegistrationFailsIfNewStateSerializerIsIncompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		final String stateName = "test-name";
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			ListStateDescriptor<TestType> kvId = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ListState<TestType> listState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			listState.add(new TestType("key-1", 1));
			listState.add(new TestType("key-1", 2));
			listState.add(new TestType("key-1", 3));

			backend.setCurrentKey(2);
			listState.add(new TestType("key-2", 1));

			backend.setCurrentKey(3);
			listState.add(new TestType("key-3", 1));
			listState.add(new TestType("key-3", 2));

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

			kvId = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION));

			// the new serializer is INCOMPATIBLE, so registering the state should fail
			backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testPriorityQueueStateCreationFailsIfNewSerializerIsNotCompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			InternalPriorityQueue<TestType> internalPriorityQueue = backend.create(
				"testPriorityQueue", new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));

			internalPriorityQueue.add(new TestType("key-1", 123));
			internalPriorityQueue.add(new TestType("key-2", 346));
			internalPriorityQueue.add(new TestType("key-1", 777));

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);
			backend.create(
				"testPriorityQueue", new TestSerializer(SerializerCompatibilityType.INCOMPATIBLE));

			Assert.fail("should have failed");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testStateBackendCreationFailsIfNewKeySerializerIsNotCompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<TestType> backend = createKeyedBackend(
			new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));

		final String stateName = "test-name";
		try {
			ValueStateDescriptor<Integer> kvId = new ValueStateDescriptor<>(stateName, Integer.class);
			ValueState<Integer> valueState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(new TestType("foo", 123));
			valueState.update(1);
			backend.setCurrentKey(new TestType("bar", 456));
			valueState.update(5);

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			try {
				// the new key serializer is incompatible; this should fail the restore
				restoreKeyedBackend(new TestSerializer(SerializerCompatibilityType.INCOMPATIBLE), snapshot);
			} catch (Exception e) {
				Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
			}

			try {
				// the new key serializer requires migration; this should fail the restore
				restoreKeyedBackend(new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION), snapshot);
			} catch (Exception e) {
				Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
			}
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testKeyedStateRegistrationFailsIfNewNamespaceSerializerIsNotCompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		final String stateName = "test-name";
		try {
			ValueStateDescriptor<Integer> kvId = new ValueStateDescriptor<>(stateName, Integer.class);
			ValueState<Integer> valueState = backend
				.getPartitionedState(
					new TestType("namespace", 123),
					new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS),
					kvId);

			backend.setCurrentKey(1);
			valueState.update(10);
			backend.setCurrentKey(5);
			valueState.update(50);

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

			try {
				// the new namespace serializer is incompatible; this should fail the restore
				backend.getPartitionedState(
					new TestType("namespace", 123),
					new TestSerializer(SerializerCompatibilityType.INCOMPATIBLE),
					kvId);
			} catch (Exception e) {
				Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
			}

			try {
				// the new namespace serializer requires migration; this should fail the restore
				backend.getPartitionedState(
					new TestType("namespace", 123),
					new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION),
					kvId);
			} catch (Exception e) {
				Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
			}
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Operator state backend migration tests
	// -------------------------------------------------------------------------------

	@Test
	public void testOperatorParitionableListStateMigration() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "partitionable-list-state";
		try {
			ListStateDescriptor<TestType> descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ListState<TestType> state = backend.getListState(descriptor);

			state.add(new TestType("foo", 13));
			state.add(new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION));
			state = backend.getListState(descriptor);

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each state partition works with the new serializer
			Iterator<TestType> iterator = state.get().iterator();
			Assert.assertEquals(new TestType("foo", 13), iterator.next());
			Assert.assertEquals(new TestType("bar", 278), iterator.next());
			Assert.assertFalse(iterator.hasNext());
			state.add(new TestType("new-entry", 777));
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testUnionListStateMigration() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "union-list-state";
		try {
			ListStateDescriptor<TestType> descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ListState<TestType> state = backend.getUnionListState(descriptor);

			state.add(new TestType("foo", 13));
			state.add(new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION));
			state = backend.getUnionListState(descriptor);

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each state partition works with the new serializer
			Iterator<TestType> iterator = state.get().iterator();
			Assert.assertEquals(new TestType("foo", 13), iterator.next());
			Assert.assertEquals(new TestType("bar", 278), iterator.next());
			Assert.assertFalse(iterator.hasNext());
			state.add(new TestType("new-entry", 777));
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testBroadcastStateValueMigration() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "broadcast-state";
		try {
			MapStateDescriptor<Integer, TestType> descriptor = new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			BroadcastState<Integer, TestType> state = backend.getBroadcastState(descriptor);

			state.put(3, new TestType("foo", 13));
			state.put(5, new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION));
			state = backend.getBroadcastState(descriptor);

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each broadcast entry works with the new serializer
			Assert.assertEquals(new TestType("foo", 13), state.get(3));
			Assert.assertEquals(new TestType("bar", 278), state.get(5));
			state.put(17, new TestType("new-entry", 777));
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testBroadcastStateKeyMigration() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "broadcast-state";
		try {
			MapStateDescriptor<TestType, Integer> descriptor = new MapStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS),
				IntSerializer.INSTANCE);
			BroadcastState<TestType, Integer> state = backend.getBroadcastState(descriptor);

			state.put(new TestType("foo", 13), 3);
			state.put(new TestType("bar", 278), 5);

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new MapStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION),
				IntSerializer.INSTANCE);
			state = backend.getBroadcastState(descriptor);

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each broadcast entry works with the new serializer
			Assert.assertEquals((Integer) 3, state.get(new TestType("foo", 13)));
			Assert.assertEquals((Integer) 5, state.get(new TestType("bar", 278)));
			state.put(new TestType("new-entry", 777), 17);
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testOperatorParitionableListStateRegistrationFailsIfNewSerializerIsIncompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "partitionable-list-state";
		try {
			ListStateDescriptor<TestType> descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ListState<TestType> state = backend.getListState(descriptor);

			state.add(new TestType("foo", 13));
			state.add(new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.INCOMPATIBLE));

			// the new serializer is INCOMPATIBLE, so registering the state should fail
			backend.getListState(descriptor);

			Assert.fail("should have failed.");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testUnionListStateRegistrationFailsIfNewSerializerIsIncompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "union-list-state";
		try {
			ListStateDescriptor<TestType> descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			ListState<TestType> state = backend.getUnionListState(descriptor);

			state.add(new TestType("foo", 13));
			state.add(new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new ListStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.INCOMPATIBLE));

			// the new serializer is INCOMPATIBLE, so registering the state should fail
			backend.getUnionListState(descriptor);

			Assert.fail("should have failed.");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testBroadcastStateRegistrationFailsIfNewValueSerializerIsIncompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "broadcast-state";
		try {
			MapStateDescriptor<Integer, TestType> descriptor = new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS));
			BroadcastState<Integer, TestType> state = backend.getBroadcastState(descriptor);

			state.put(3, new TestType("foo", 13));
			state.put(5, new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION));

			// the new value serializer is INCOMPATIBLE, so registering the state should fail
			backend.getBroadcastState(descriptor);
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testBroadcastStateRegistrationFailsIfNewKeySerializerIsIncompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		final String stateName = "broadcast-state";
		try {
			MapStateDescriptor<TestType, Integer> descriptor = new MapStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS),
				IntSerializer.INSTANCE);
			BroadcastState<TestType, Integer> state = backend.getBroadcastState(descriptor);

			state.put(new TestType("foo", 13), 3);
			state.put(new TestType("bar", 278), 5);

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			descriptor = new MapStateDescriptor<>(
				stateName,
				new TestSerializer(SerializerCompatibilityType.INCOMPATIBLE),
				IntSerializer.INSTANCE);

			// the new key serializer is INCOMPATIBLE, so registering the state should fail
			backend.getBroadcastState(descriptor);
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Test types, serializers, and serializer snapshots
	// -------------------------------------------------------------------------------

	/**
	 * The type used as state under tests.
	 *
	 * <p>This is implemented so that the type can also be used as keyed priority queue state.
	 */
	private static class TestType implements HeapPriorityQueueElement, PriorityComparable<TestType>, Keyed<String> {

		private int index;

		private final int value;
		private final String key;

		public TestType(String key, int value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public int comparePriorityTo(@Nonnull TestType other) {
			return Integer.compare(value, other.value);
		}

		@Override
		public int getInternalIndex() {
			return index;
		}

		@Override
		public void setInternalIndex(int newIndex) {
			this.index = newIndex;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !(obj instanceof StateBackendMigrationTestBase.TestType)) {
				return false;
			}

			if (obj == this) {
				return true;
			}

			TestType other = (TestType) obj;
			return Objects.equals(key, other.key) && value == other.value;
		}

		@Override
		public int hashCode() {
			return 31 * key.hashCode() + value;
		}
	}

	private static class TestSerializer extends TypeSerializer<TestType> {

		private static final String MIGRATION_PAYLOAD = "random-migration-payload";

		private final SerializerCompatibilityType compatibilityType;

		TestSerializer(SerializerCompatibilityType compatibilityType) {
			this.compatibilityType = compatibilityType;
		}

		// --------------------------------------------------------------------------------
		//  State serialization relevant methods
		// --------------------------------------------------------------------------------

		@Override
		public void serialize(TestType record, DataOutputView target) throws IOException {
			switch (compatibilityType) {
				case COMPATIBLE_AS_IS:
					target.writeUTF(record.getKey());
					target.writeInt(record.value);
					break;

				case REQUIRES_MIGRATION:
					target.writeUTF(record.getKey());
					target.writeUTF(MIGRATION_PAYLOAD);
					target.writeInt(record.value);
					target.writeBoolean(true);
					break;

				case INCOMPATIBLE:
					// the serializer shouldn't be used in this case
					throw new UnsupportedOperationException();
			}
		}

		@Override
		public TestType deserialize(DataInputView source) throws IOException {
			String key;
			int value;

			switch (compatibilityType) {
				case COMPATIBLE_AS_IS:
					key = source.readUTF();
					value = source.readInt();
					break;

				case REQUIRES_MIGRATION:
					key = source.readUTF();
					Assert.assertEquals(MIGRATION_PAYLOAD, source.readUTF());
					value = source.readInt();
					Assert.assertTrue(source.readBoolean());
					break;

				default:
				case INCOMPATIBLE:
					// the serializer shouldn't be used in this case
					throw new UnsupportedOperationException();
			}

			return new TestType(key, value);
		}

		@Override
		public TestType copy(TestType from) {
			return new TestType(from.key, from.value);
		}

		@Override
		public TypeSerializerSnapshot<TestType> snapshotConfiguration() {
			return new TestSerializerSnapshot();
		}

		// --------------------------------------------------------------------------------
		//  Miscellaneous serializer methods
		// --------------------------------------------------------------------------------

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			serialize(deserialize(source), target);
		}

		@Override
		public TestType deserialize(TestType reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public TestType copy(TestType from, TestType reuse) {
			return copy(from);
		}

		@Override
		public TestType createInstance() {
			throw new UnsupportedOperationException();
		}

		@Override
		public TypeSerializer<TestType> duplicate() {
			return this;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public boolean canEqual(Object obj) {
			return getClass().equals(obj.getClass());
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this;
		}
	}

	public static class TestSerializerSnapshot implements TypeSerializerSnapshot<TestType> {

		@Override
		public int getCurrentVersion() {
			return 1;
		}

		@Override
		public TypeSerializer<TestType> restoreSerializer() {
			return new TestSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS);
		}

		@Override
		public TypeSerializerSchemaCompatibility<TestType> resolveSchemaCompatibility(TypeSerializer<TestType> newSerializer) {
			switch (((TestSerializer) newSerializer).compatibilityType) {
				case COMPATIBLE_AS_IS:
					return TypeSerializerSchemaCompatibility.compatibleAsIs();
				case REQUIRES_MIGRATION:
					return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
				case INCOMPATIBLE:
					return TypeSerializerSchemaCompatibility.incompatible();
				default:
					throw new UnsupportedOperationException();
			}
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {}
	}

	public static class CustomVoidNamespaceSerializer extends TypeSerializer<VoidNamespace> {

		private static final long serialVersionUID = 1L;

		public static final CustomVoidNamespaceSerializer INSTANCE = new CustomVoidNamespaceSerializer();

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public VoidNamespace createInstance() {
			return VoidNamespace.get();
		}

		@Override
		public VoidNamespace copy(VoidNamespace from) {
			return VoidNamespace.get();
		}

		@Override
		public VoidNamespace copy(VoidNamespace from, VoidNamespace reuse) {
			return VoidNamespace.get();
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(VoidNamespace record, DataOutputView target) throws IOException {
			// Make progress in the stream, write one byte.
			//
			// We could just skip writing anything here, because of the way this is
			// used with the state backends, but if it is ever used somewhere else
			// (even though it is unlikely to happen), it would be a problem.
			target.write(0);
		}

		@Override
		public VoidNamespace deserialize(DataInputView source) throws IOException {
			source.readByte();
			return VoidNamespace.get();
		}

		@Override
		public VoidNamespace deserialize(VoidNamespace reuse, DataInputView source) throws IOException {
			source.readByte();
			return VoidNamespace.get();
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.write(source.readByte());
		}

		@Override
		public TypeSerializer<VoidNamespace> duplicate() {
			return this;
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof CustomVoidNamespaceSerializer;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof CustomVoidNamespaceSerializer;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}

		@Override
		public TypeSerializerSnapshot<VoidNamespace> snapshotConfiguration() {
			return new CustomVoidNamespaceSerializerSnapshot();
		}
	}

	public static class CustomVoidNamespaceSerializerSnapshot implements TypeSerializerSnapshot<VoidNamespace> {

		@Override
		public TypeSerializer<VoidNamespace> restoreSerializer() {
			return new CustomVoidNamespaceSerializer();
		}

		@Override
		public TypeSerializerSchemaCompatibility<VoidNamespace>
		resolveSchemaCompatibility(TypeSerializer<VoidNamespace> newSerializer) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof CustomVoidNamespaceSerializerSnapshot;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public int getCurrentVersion() {
			return 0;
		}
	}

	private CheckpointStreamFactory createStreamFactory() throws Exception {
		if (checkpointStorageLocation == null) {
			checkpointStorageLocation = getStateBackend()
				.createCheckpointStorage(new JobID())
				.initializeLocationForCheckpoint(1L);
		}
		return checkpointStorageLocation;
	}

	// -------------------------------------------------------------------------------
	//  Keyed state backend utilities
	// -------------------------------------------------------------------------------

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer) throws Exception {
		return createKeyedBackend(keySerializer, new DummyEnvironment());
	}

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer, Environment env) throws Exception {
		return createKeyedBackend(
			keySerializer,
			10,
			new KeyGroupRange(0, 9),
			env);
	}

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(
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

	private <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(TypeSerializer<K> keySerializer, KeyedStateHandle state) throws Exception {
		return restoreKeyedBackend(keySerializer, state, new DummyEnvironment());
	}

	private  <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(
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

	private <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(
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
		backend.restore(new StateObjectCollection<>(state));
		return backend;
	}

	private KeyedStateHandle runSnapshot(
		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture,
		SharedStateRegistry sharedStateRegistry) throws Exception {

		if (!snapshotRunnableFuture.isDone()) {
			snapshotRunnableFuture.run();
		}

		SnapshotResult<KeyedStateHandle> snapshotResult = snapshotRunnableFuture.get();
		KeyedStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();
		if (jobManagerOwnedSnapshot != null) {
			jobManagerOwnedSnapshot.registerSharedStates(sharedStateRegistry);
		}
		return jobManagerOwnedSnapshot;
	}

	// -------------------------------------------------------------------------------
	//  Operator state backend utilities
	// -------------------------------------------------------------------------------

	private OperatorStateBackend createOperatorStateBackend() throws Exception {
		return getStateBackend().createOperatorStateBackend(new DummyEnvironment(), "test_op");
	}

	private OperatorStateBackend restoreOperatorStateBackend(OperatorStateHandle state) throws Exception {
		OperatorStateBackend operatorStateBackend = createOperatorStateBackend();
		operatorStateBackend.restore(StateObjectCollection.singleton(state));
		return operatorStateBackend;
	}

	private OperatorStateHandle runSnapshot(
		RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshotRunnableFuture) throws Exception {

		if (!snapshotRunnableFuture.isDone()) {
			snapshotRunnableFuture.run();
		}

		return snapshotRunnableFuture.get().getJobManagerOwnedSnapshot();
	}
}
