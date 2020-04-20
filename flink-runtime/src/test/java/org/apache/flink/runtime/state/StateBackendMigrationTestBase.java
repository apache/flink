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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

	@Before
	public void before() {
		env = MockEnvironment.builder().build();
	}

	@After
	public void after() {
		IOUtils.closeQuietly(env);
	}

	// lazily initialized stream storage
	private CheckpointStorageLocation checkpointStorageLocation;

	private MockEnvironment env;

	// -------------------------------------------------------------------------------
	//  Tests for keyed ValueState
	// -------------------------------------------------------------------------------

	@Test
	public void testKeyedValueStateMigration() throws Exception {
		final String stateName = "test-name";

		testKeyedValueStateUpgrade(
			new ValueStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ValueStateDescriptor<>(
				stateName,
				// restore with a V2 serializer that has a different schema
				new TestType.V2TestTypeSerializer()));
	}

	@Test
	public void testKeyedValueStateSerializerReconfiguration() throws Exception {
		final String stateName = "test-name";

		testKeyedValueStateUpgrade(
			new ValueStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ValueStateDescriptor<>(
				stateName,
				// the test fails if this serializer is used instead of a reconfigured new serializer
				new TestType.ReconfigurationRequiringTestTypeSerializer()));
	}

	@Test
	public void testKeyedValueStateRegistrationFailsIfNewStateSerializerIsIncompatible() {
		final String stateName = "test-name";

		try {
			testKeyedValueStateUpgrade(
				new ValueStateDescriptor<>(
					stateName,
					new TestType.V1TestTypeSerializer()),
				new ValueStateDescriptor<>(
					stateName,
					new TestType.IncompatibleTestTypeSerializer()));

			fail("should have failed");
		} catch (Exception expected) {
			Assert.assertTrue(ExceptionUtils.findThrowable(expected, StateMigrationException.class).isPresent());
		}
	}

	private void testKeyedValueStateUpgrade(
			ValueStateDescriptor<TestType> initialAccessDescriptor,
			ValueStateDescriptor<TestType> newAccessDescriptorAfterRestore) throws Exception {

		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			ValueState<TestType> valueState = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				CustomVoidNamespaceSerializer.INSTANCE,
				initialAccessDescriptor);

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

			valueState = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				CustomVoidNamespaceSerializer.INSTANCE,
				newAccessDescriptorAfterRestore);

			// make sure that reading and writing each key state works with the new serializer
			backend.setCurrentKey(1);
			assertEquals(new TestType("foo", 1456), valueState.value());
			valueState.update(new TestType("newValue1", 751));

			backend.setCurrentKey(2);
			assertEquals(new TestType("bar", 478), valueState.value());
			valueState.update(new TestType("newValue2", 167));

			backend.setCurrentKey(3);
			assertEquals(new TestType("hello", 189), valueState.value());
			valueState.update(new TestType("newValue3", 444));

			// do another snapshot to verify the snapshot logic after migration
			snapshot = runSnapshot(
				backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			snapshot.discardState();
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Tests for keyed ListState
	// -------------------------------------------------------------------------------

	@Test
	public void testKeyedListStateMigration() throws Exception {
		final String stateName = "test-name";

		testKeyedListStateUpgrade(
			new ListStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ListStateDescriptor<>(
				stateName,
				// restore with a V2 serializer that has a different schema
				new TestType.V2TestTypeSerializer()));
	}

	@Test
	public void testKeyedListStateSerializerReconfiguration() throws Exception {
		final String stateName = "test-name";

		testKeyedListStateUpgrade(
			new ListStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ListStateDescriptor<>(
				stateName,
				// the test fails if this serializer is used instead of a reconfigured new serializer
				new TestType.ReconfigurationRequiringTestTypeSerializer()));
	}

	@Test
	public void testKeyedListStateRegistrationFailsIfNewStateSerializerIsIncompatible() {
		final String stateName = "test-name";

		try {
			testKeyedListStateUpgrade(
				new ListStateDescriptor<>(
					stateName,
					new TestType.V1TestTypeSerializer()),
				new ListStateDescriptor<>(
					stateName,
					new TestType.IncompatibleTestTypeSerializer()));

			fail("should have failed");
		} catch (Exception expected) {
			Assert.assertTrue(ExceptionUtils.findThrowable(expected, StateMigrationException.class).isPresent());
		}
	}

	private void testKeyedListStateUpgrade(
			ListStateDescriptor<TestType> initialAccessDescriptor,
			ListStateDescriptor<TestType> newAccessDescriptorAfterRestore) throws Exception {

		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			ListState<TestType> listState = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				CustomVoidNamespaceSerializer.INSTANCE,
				initialAccessDescriptor);

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

			listState = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				CustomVoidNamespaceSerializer.INSTANCE,
				newAccessDescriptorAfterRestore);

			// make sure that reading and writing each key state works with the new serializer
			backend.setCurrentKey(1);
			Iterator<TestType> iterable1 = listState.get().iterator();
			assertEquals(new TestType("key-1", 1), iterable1.next());
			assertEquals(new TestType("key-1", 2), iterable1.next());
			assertEquals(new TestType("key-1", 3), iterable1.next());
			Assert.assertFalse(iterable1.hasNext());
			listState.add(new TestType("new-key-1", 123));

			backend.setCurrentKey(2);
			Iterator<TestType> iterable2 = listState.get().iterator();
			assertEquals(new TestType("key-2", 1), iterable2.next());
			Assert.assertFalse(iterable2.hasNext());
			listState.add(new TestType("new-key-2", 456));

			backend.setCurrentKey(3);
			Iterator<TestType> iterable3 = listState.get().iterator();
			assertEquals(new TestType("key-3", 1), iterable3.next());
			assertEquals(new TestType("key-3", 2), iterable3.next());
			Assert.assertFalse(iterable3.hasNext());
			listState.add(new TestType("new-key-3", 777));

			// do another snapshot to verify the snapshot logic after migration
			snapshot = runSnapshot(
				backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			snapshot.discardState();

		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Tests for keyed MapState
	// -------------------------------------------------------------------------------

	@Test
	public void testKeyedMapStateAsIs() throws Exception {
		final String stateName = "test-name";

		testKeyedMapStateUpgrade(
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestType.V1TestTypeSerializer()),
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestType.V1TestTypeSerializer()));
	}

	@Test
	public void testKeyedMapStateStateMigration() throws Exception {
		final String stateName = "test-name";

		testKeyedMapStateUpgrade(
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestType.V1TestTypeSerializer()),
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				// restore with a V2 serializer that has a different schema
				new TestType.V2TestTypeSerializer()));
	}

	@Test
	public void testKeyedMapStateSerializerReconfiguration() throws Exception {
		final String stateName = "test-name";

		testKeyedMapStateUpgrade(
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestType.V1TestTypeSerializer()),
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				// restore with a V2 serializer that has a different schema
				new TestType.ReconfigurationRequiringTestTypeSerializer()));
	}

	@Test
	public void testKeyedMapStateRegistrationFailsIfNewStateSerializerIsIncompatible() {
		final String stateName = "test-name";

		try {
			testKeyedMapStateUpgrade(
				new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestType.V1TestTypeSerializer()),
				new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				// restore with a V2 serializer that has a different schema
				new TestType.IncompatibleTestTypeSerializer()));
			fail("should have failed");
		} catch (Exception expected) {
			Assert.assertTrue(ExceptionUtils.findThrowable(expected, StateMigrationException.class).isPresent());
		}
	}

	private void testKeyedMapStateUpgrade(
		MapStateDescriptor<Integer, TestType> initialAccessDescriptor,
		MapStateDescriptor<Integer, TestType> newAccessDescriptorAfterRestore) throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			MapState<Integer, TestType> mapState = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				CustomVoidNamespaceSerializer.INSTANCE,
				initialAccessDescriptor);

			backend.setCurrentKey(1);
			mapState.put(1, new TestType("key-1", 1));
			mapState.put(2, new TestType("key-1", 2));
			mapState.put(3, new TestType("key-1", 3));

			backend.setCurrentKey(2);
			mapState.put(1, new TestType("key-2", 1));

			backend.setCurrentKey(3);
			mapState.put(1, new TestType("key-3", 1));
			mapState.put(2, new TestType("key-3", 2));

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

			mapState = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				CustomVoidNamespaceSerializer.INSTANCE,
				newAccessDescriptorAfterRestore);

			// make sure that reading and writing each key state works with the new serializer
			backend.setCurrentKey(1);
			Iterator<Map.Entry<Integer, TestType>> iterable1 = mapState.iterator();
			Map.Entry<Integer, TestType> actual = iterable1.next();
			assertEquals((Integer) 1, actual.getKey());
			assertEquals(new TestType("key-1", 1), actual.getValue());

			actual = iterable1.next();
			assertEquals((Integer) 2, actual.getKey());
			assertEquals(new TestType("key-1", 2), actual.getValue());

			actual = iterable1.next();
			assertEquals((Integer) 3, actual.getKey());
			assertEquals(new TestType("key-1", 3), actual.getValue());

			Assert.assertFalse(iterable1.hasNext());

			mapState.put(123, new TestType("new-key-1", 123));

			backend.setCurrentKey(2);
			Iterator<Map.Entry<Integer, TestType>> iterable2 = mapState.iterator();

			actual = iterable2.next();
			assertEquals((Integer) 1, actual.getKey());
			assertEquals(new TestType("key-2", 1), actual.getValue());
			Assert.assertFalse(iterable2.hasNext());

			mapState.put(456, new TestType("new-key-2", 456));

			backend.setCurrentKey(3);
			Iterator<Map.Entry<Integer, TestType>> iterable3 = mapState.iterator();

			actual = iterable3.next();
			assertEquals((Integer) 1, actual.getKey());
			assertEquals(new TestType("key-3", 1), actual.getValue());

			actual = iterable3.next();
			assertEquals((Integer) 2, actual.getKey());
			assertEquals(new TestType("key-3", 2), actual.getValue());

			Assert.assertFalse(iterable3.hasNext());
			mapState.put(777, new TestType("new-key-3", 777));

			// do another snapshot to verify the snapshot logic after migration
			snapshot = runSnapshot(
				backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			snapshot.discardState();

		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Tests for keyed priority queue state
	// -------------------------------------------------------------------------------

	@Test
	public void testPriorityQueueStateCreationFailsIfNewSerializerIsNotCompatible() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		try {
			InternalPriorityQueue<TestType> internalPriorityQueue = backend.create(
				"testPriorityQueue", new TestType.V1TestTypeSerializer());

			internalPriorityQueue.add(new TestType("key-1", 123));
			internalPriorityQueue.add(new TestType("key-2", 346));
			internalPriorityQueue.add(new TestType("key-1", 777));

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			backend.dispose();

			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);
			backend.create(
				"testPriorityQueue", new TestType.IncompatibleTestTypeSerializer());

			fail("should have failed");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Tests for key serializer in keyed state backends
	// -------------------------------------------------------------------------------

	@Test
	public void testStateBackendRestoreFailsIfNewKeySerializerRequiresMigration() throws Exception {
		try {
			testKeySerializerUpgrade(
				new TestType.V1TestTypeSerializer(),
				new TestType.V2TestTypeSerializer());

			fail("should have failed");
		} catch (Exception expected) {
			// the new key serializer requires migration; this should fail the restore
			Assert.assertTrue(ExceptionUtils.findThrowable(expected, StateMigrationException.class).isPresent());
		}
	}

	@Test
	public void testStateBackendRestoreSucceedsIfNewKeySerializerRequiresReconfiguration() throws Exception {
		testKeySerializerUpgrade(
			new TestType.V1TestTypeSerializer(),
			new TestType.ReconfigurationRequiringTestTypeSerializer());
	}

	@Test
	public void testStateBackendRestoreFailsIfNewKeySerializerIsIncompatible() throws Exception {
		try {
			testKeySerializerUpgrade(
				new TestType.V1TestTypeSerializer(),
				new TestType.IncompatibleTestTypeSerializer());

			fail("should have failed");
		} catch (Exception expected) {
			// the new key serializer is incompatible; this should fail the restore
			Assert.assertTrue(ExceptionUtils.findThrowable(expected, StateMigrationException.class).isPresent());
		}
	}

	private void testKeySerializerUpgrade(
			TypeSerializer<TestType> initialKeySerializer,
			TypeSerializer<TestType> newKeySerializer) throws Exception {

		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<TestType> backend = createKeyedBackend(initialKeySerializer);

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

			backend = restoreKeyedBackend(newKeySerializer, snapshot);

			valueState = backend
				.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

			// access and check previous state
			backend.setCurrentKey(new TestType("foo", 123));
			assertEquals(1, valueState.value().intValue());
			backend.setCurrentKey(new TestType("bar", 456));
			assertEquals(5, valueState.value().intValue());

			// do another snapshot to verify the snapshot logic after migration
			snapshot = runSnapshot(
				backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			snapshot.discardState();
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Tests for namespace serializer in keyed state backends
	// -------------------------------------------------------------------------------

	@Test
	public void testKeyedStateRegistrationFailsIfNewNamespaceSerializerRequiresMigration() throws Exception {
		try {
			testNamespaceSerializerUpgrade(
				new TestType.V1TestTypeSerializer(),
				new TestType.V2TestTypeSerializer());

			fail("should have failed");
		} catch (Exception expected) {
			// the new namespace serializer requires migration; this should fail the restore
			Assert.assertTrue(ExceptionUtils.findThrowable(expected, StateMigrationException.class).isPresent());
		}
	}

	@Test
	public void testKeyedStateRegistrationSucceedsIfNewNamespaceSerializerRequiresReconfiguration() throws Exception {
		testNamespaceSerializerUpgrade(
			new TestType.V1TestTypeSerializer(),
			new TestType.ReconfigurationRequiringTestTypeSerializer());
	}

	@Test
	public void testKeyedStateRegistrationFailsIfNewNamespaceSerializerIsIncompatible() throws Exception {
		try {
			testNamespaceSerializerUpgrade(
				new TestType.V1TestTypeSerializer(),
				new TestType.IncompatibleTestTypeSerializer());

			fail("should have failed");
		} catch (Exception expected) {
			// the new namespace serializer is incompatible; this should fail the restore
			Assert.assertTrue(ExceptionUtils.findThrowable(expected, StateMigrationException.class).isPresent());
		}
	}

	private void testNamespaceSerializerUpgrade(
			TypeSerializer<TestType> initialNamespaceSerializer,
			TypeSerializer<TestType> newNamespaceSerializerAfterRestore) throws Exception {

		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		final String stateName = "test-name";
		try {
			ValueStateDescriptor<Integer> kvId = new ValueStateDescriptor<>(stateName, Integer.class);
			ValueState<Integer> valueState = backend
				.getPartitionedState(
					new TestType("namespace", 123),
					initialNamespaceSerializer,
					kvId);

			backend.setCurrentKey(1);
			valueState.update(10);
			backend.setCurrentKey(5);
			valueState.update(50);

			KeyedStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);

			// test incompatible namespace serializer; start with a freshly restored backend
			backend.dispose();
			backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot);

			valueState = backend.getPartitionedState(
				new TestType("namespace", 123),
				newNamespaceSerializerAfterRestore,
				kvId);

			// access and check previous state
			backend.setCurrentKey(1);
			assertEquals(10, valueState.value().intValue());
			valueState.update(10);
			backend.setCurrentKey(5);
			assertEquals(50, valueState.value().intValue());

			// do another snapshot to verify the snapshot logic after migration
			snapshot = runSnapshot(
				backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
				sharedStateRegistry);
			snapshot.discardState();
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Operator state backend partitionable list state tests
	// -------------------------------------------------------------------------------

	@Test
	public void testOperatorParitionableListStateMigration() throws Exception {
		final String stateName = "partitionable-list-state";

		testOperatorPartitionableListStateUpgrade(
			new ListStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ListStateDescriptor<>(
				stateName,
				// restore with a V2 serializer that has a different schema
				new TestType.V2TestTypeSerializer()));
	}

	@Test
	public void testOperatorParitionableListStateSerializerReconfiguration() throws Exception {
		final String stateName = "partitionable-list-state";

		testOperatorPartitionableListStateUpgrade(
			new ListStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ListStateDescriptor<>(
				stateName,
				// restore with a new serializer that requires reconfiguration
				new TestType.ReconfigurationRequiringTestTypeSerializer()));
	}

	@Test
	public void testOperatorParitionableListStateRegistrationFailsIfNewSerializerIsIncompatible() throws Exception {
		final String stateName = "partitionable-list-state";

		try {
			testOperatorPartitionableListStateUpgrade(
				new ListStateDescriptor<>(
					stateName,
					new TestType.V1TestTypeSerializer()),
				new ListStateDescriptor<>(
					stateName,
					// restore with a new incompatible serializer
					new TestType.IncompatibleTestTypeSerializer()));

			fail("should have failed.");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		}
	}

	private void testOperatorPartitionableListStateUpgrade(
			ListStateDescriptor<TestType> initialAccessDescriptor,
			ListStateDescriptor<TestType> newAccessDescriptorAfterRestore) throws Exception {

		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		try {
			ListState<TestType> state = backend.getListState(initialAccessDescriptor);

			state.add(new TestType("foo", 13));
			state.add(new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			state = backend.getListState(newAccessDescriptorAfterRestore);

			// make sure that reading and writing each state partition works with the new serializer
			Iterator<TestType> iterator = state.get().iterator();
			assertEquals(new TestType("foo", 13), iterator.next());
			assertEquals(new TestType("bar", 278), iterator.next());
			Assert.assertFalse(iterator.hasNext());
			state.add(new TestType("new-entry", 777));
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Operator state backend union list state tests
	// -------------------------------------------------------------------------------

	@Test
	public void testOperatorUnionListStateMigration() throws Exception {
		final String stateName = "union-list-state";

		testOperatorUnionListStateUpgrade(
			new ListStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ListStateDescriptor<>(
				stateName,
				// restore with a V2 serializer that has a different schema
				new TestType.V2TestTypeSerializer()));
	}

	@Test
	public void testOperatorUnionListStateSerializerReconfiguration() throws Exception {
		final String stateName = "union-list-state";

		testOperatorUnionListStateUpgrade(
			new ListStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer()),
			new ListStateDescriptor<>(
				stateName,
				// restore with a new serializer that requires reconfiguration
				new TestType.ReconfigurationRequiringTestTypeSerializer()));
	}


	@Test
	public void testOperatorUnionListStateRegistrationFailsIfNewSerializerIsIncompatible() {
		final String stateName = "union-list-state";

		try {
			testOperatorUnionListStateUpgrade(
				new ListStateDescriptor<>(
					stateName,
					new TestType.V1TestTypeSerializer()),
				new ListStateDescriptor<>(
					stateName,
					// restore with a new incompatible serializer
					new TestType.IncompatibleTestTypeSerializer()));

			fail("should have failed.");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		}
	}

	private void testOperatorUnionListStateUpgrade(
			ListStateDescriptor<TestType> initialAccessDescriptor,
			ListStateDescriptor<TestType> newAccessDescriptorAfterRestore) throws Exception {

		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		try {
			ListState<TestType> state = backend.getUnionListState(initialAccessDescriptor);

			state.add(new TestType("foo", 13));
			state.add(new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			state = backend.getUnionListState(newAccessDescriptorAfterRestore);

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each state partition works with the new serializer
			Iterator<TestType> iterator = state.get().iterator();
			assertEquals(new TestType("foo", 13), iterator.next());
			assertEquals(new TestType("bar", 278), iterator.next());
			Assert.assertFalse(iterator.hasNext());
			state.add(new TestType("new-entry", 777));
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Operator state backend broadcast state tests
	// -------------------------------------------------------------------------------

	@Test
	public void testBroadcastStateValueMigration() throws Exception {
		final String stateName = "broadcast-state";

		testBroadcastStateValueUpgrade(
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestType.V1TestTypeSerializer()),
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				// new value serializer is a V2 serializer with a different schema
				new TestType.V2TestTypeSerializer()));
	}


	@Test
	public void testBroadcastStateKeyMigration() throws Exception {
		final String stateName = "broadcast-state";

		testBroadcastStateKeyUpgrade(
			new MapStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer(),
				IntSerializer.INSTANCE),
			new MapStateDescriptor<>(
				stateName,
				// new key serializer is a V2 serializer with a different schema
				new TestType.V2TestTypeSerializer(),
				IntSerializer.INSTANCE));
	}

	@Test
	public void testBroadcastStateValueSerializerReconfiguration() throws Exception {
		final String stateName = "broadcast-state";

		testBroadcastStateValueUpgrade(
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				new TestType.V1TestTypeSerializer()),
			new MapStateDescriptor<>(
				stateName,
				IntSerializer.INSTANCE,
				// new value serializer is a new serializer that requires reconfiguration
				new TestType.ReconfigurationRequiringTestTypeSerializer()));
	}

	@Test
	public void testBroadcastStateKeySerializerReconfiguration() throws Exception {
		final String stateName = "broadcast-state";

		testBroadcastStateKeyUpgrade(
			new MapStateDescriptor<>(
				stateName,
				new TestType.V1TestTypeSerializer(),
				IntSerializer.INSTANCE),
			new MapStateDescriptor<>(
				stateName,
				// new key serializer is a new serializer that requires reconfiguration
				new TestType.ReconfigurationRequiringTestTypeSerializer(),
				IntSerializer.INSTANCE));
	}

	@Test
	public void testBroadcastStateRegistrationFailsIfNewValueSerializerIsIncompatible() {
		final String stateName = "broadcast-state";

		try {
			testBroadcastStateValueUpgrade(
				new MapStateDescriptor<>(
					stateName,
					IntSerializer.INSTANCE,
					new TestType.V1TestTypeSerializer()),
				new MapStateDescriptor<>(
					stateName,
					IntSerializer.INSTANCE,
					// new value serializer is incompatible
					new TestType.IncompatibleTestTypeSerializer()));

			fail("should have failed.");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		}
	}

	@Test
	public void testBroadcastStateRegistrationFailsIfNewKeySerializerIsIncompatible() {
		final String stateName = "broadcast-state";

		try {
			testBroadcastStateKeyUpgrade(
				new MapStateDescriptor<>(
					stateName,
					new TestType.V1TestTypeSerializer(),
					IntSerializer.INSTANCE),
				new MapStateDescriptor<>(
					stateName,
					// new key serializer is incompatible
					new TestType.IncompatibleTestTypeSerializer(),
					IntSerializer.INSTANCE));

			fail("should have failed.");
		} catch (Exception e) {
			Assert.assertTrue(ExceptionUtils.findThrowable(e, StateMigrationException.class).isPresent());
		}
	}

	private void testBroadcastStateValueUpgrade(
			MapStateDescriptor<Integer, TestType> initialAccessDescriptor,
			MapStateDescriptor<Integer, TestType> newAccessDescriptorAfterRestore) throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		try {
			BroadcastState<Integer, TestType> state = backend.getBroadcastState(initialAccessDescriptor);

			state.put(3, new TestType("foo", 13));
			state.put(5, new TestType("bar", 278));

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			state = backend.getBroadcastState(newAccessDescriptorAfterRestore);

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each broadcast entry works with the new serializer
			assertEquals(new TestType("foo", 13), state.get(3));
			assertEquals(new TestType("bar", 278), state.get(5));
			state.put(17, new TestType("new-entry", 777));
		} finally {
			backend.dispose();
		}
	}

	private void testBroadcastStateKeyUpgrade(
			MapStateDescriptor<TestType, Integer> initialAccessDescriptor,
			MapStateDescriptor<TestType, Integer> newAccessDescriptorAfterRestore) throws Exception {

		CheckpointStreamFactory streamFactory = createStreamFactory();

		OperatorStateBackend backend = createOperatorStateBackend();

		try {
			BroadcastState<TestType, Integer> state = backend.getBroadcastState(initialAccessDescriptor);

			state.put(new TestType("foo", 13), 3);
			state.put(new TestType("bar", 278), 5);

			OperatorStateHandle snapshot = runSnapshot(
				backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
			backend.dispose();

			backend = restoreOperatorStateBackend(snapshot);

			state = backend.getBroadcastState(newAccessDescriptorAfterRestore);

			// the state backend should have decided whether or not it needs to perform state migration;
			// make sure that reading and writing each broadcast entry works with the new serializer
			assertEquals((Integer) 3, state.get(new TestType("foo", 13)));
			assertEquals((Integer) 5, state.get(new TestType("bar", 278)));
			state.put(new TestType("new-entry", 777), 17);
		} finally {
			backend.dispose();
		}
	}

	// -------------------------------------------------------------------------------
	//  Test types, serializers, and serializer snapshots
	// -------------------------------------------------------------------------------

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
			CheckpointStorage checkpointStorage = getStateBackend()
				.createCheckpointStorage(new JobID());
			checkpointStorage.initializeBaseLocations();
			checkpointStorageLocation = checkpointStorage
				.initializeLocationForCheckpoint(1L);
		}
		return checkpointStorageLocation;
	}

	// -------------------------------------------------------------------------------
	//  Keyed state backend utilities
	// -------------------------------------------------------------------------------

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer) throws Exception {
		return createKeyedBackend(keySerializer, env);
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
			env.getTaskKvStateRegistry(),
			TtlTimeProvider.DEFAULT,
			new UnregisteredMetricsGroup(),
			Collections.emptyList(),
			new CloseableRegistry());
		return backend;
	}

	private <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(TypeSerializer<K> keySerializer, KeyedStateHandle state) throws Exception {
		return restoreKeyedBackend(keySerializer, state, env);
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
			env.getTaskKvStateRegistry(),
			TtlTimeProvider.DEFAULT,
			new UnregisteredMetricsGroup(),
			state,
			new CloseableRegistry());
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
		return getStateBackend().createOperatorStateBackend(
			env, "test_op", Collections.emptyList(), new CloseableRegistry());
	}

	private OperatorStateBackend createOperatorStateBackend(Collection<OperatorStateHandle> state) throws Exception {
		return getStateBackend().createOperatorStateBackend(
			env, "test_op", state, new CloseableRegistry());
	}

	private OperatorStateBackend restoreOperatorStateBackend(OperatorStateHandle state) throws Exception {
		OperatorStateBackend operatorStateBackend = createOperatorStateBackend(StateObjectCollection.singleton(state));
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
