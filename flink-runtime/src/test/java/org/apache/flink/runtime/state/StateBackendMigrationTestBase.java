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
import org.apache.flink.types.StringValue;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link KeyedStateBackend} and {@link OperatorStateBackend} as produced
 * by various {@link StateBackend}s.
 */
@SuppressWarnings("serial")
public abstract class StateBackendMigrationTestBase<B extends AbstractStateBackend> extends TestLogger {

	@Rule
	public final ExpectedException expectedException = ExpectedException.none();

	// lazily initialized stream storage
	private CheckpointStorageLocation checkpointStorageLocation;

	/**
	 * Different "personalities" of {@link CustomStringSerializer}. Instead of creating
	 * different classes we parameterize the serializer with this and
	 * {@link CustomStringSerializerSnapshot} will instantiate serializers with the correct
	 * personality.
	 */
	public enum SerializerVersion {
		INITIAL,
		RESTORE,
		NEW
	}

	/**
	 * The compatibility behaviour of {@link CustomStringSerializer}. This controls what
	 * type of serializer {@link CustomStringSerializerSnapshot} will create for
	 * the different methods that return/create serializers.
	 */
	public enum SerializerCompatibilityType {
		COMPATIBLE_AS_IS,
		REQUIRES_MIGRATION
	}

	/**
	 * The serialization timeliness behaviour of the state backend under test.
	 */
	public enum BackendSerializationTimeliness {
		ON_ACCESS,
		ON_CHECKPOINTS
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testValueStateWithSerializerRequiringMigration() throws Exception {
		CustomStringSerializer.resetCountingMaps();

		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>(
			"id",
			new CustomStringSerializer(SerializerCompatibilityType.REQUIRES_MIGRATION, SerializerVersion.INITIAL));
		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

		// ============ Modifications to the state ============
		//  For eager serialization backends:
		//    This should result in serializer personality INITIAL having 2 serialize calls
		//
		//  For lazy serialization backends:
		//    This should not result in any serialize / deserialize calls

		backend.setCurrentKey(1);
		state.update("1");
		backend.setCurrentKey(2);
		state.update("2");
		backend.setCurrentKey(1);

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ Snapshot #1 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality INITIAL having 2 serialize calls
		KeyedStateHandle snapshot1 = runSnapshot(
			backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);
		backend.dispose();

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		} else {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ Restore from snapshot #1 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality RESTORE having 2 deserialize calls
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals((Integer) 2, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));
		}
		CustomStringSerializer.resetCountingMaps();

		ValueStateDescriptor<String> newKvId = new ValueStateDescriptor<>("id",
			new CustomStringSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));

		// ============ State registration that triggers state migration ============
		//  For eager serialization backends:
		//    This should result in serializer personality RESTORE having 2 deserialize calls, and NEW having 2 serialize calls
		//
		//  For lazy serialization backends:
		//    This should not result in any serialize / deserialize calls
		ValueState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId);

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals((Integer) 2, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));

			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));

			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ More modifications to the state ============
		//  For eager serialization backends:
		//    This should result in serializer personality NEW having 2 serialize calls and 3 deserialize calls
		//
		//  For lazy serialization backends:
		//    This should not result in any serialize / deserialize calls
		backend.setCurrentKey(1);
		assertEquals("1", restored1.value());
		restored1.update("1"); // s, NEW
		backend.setCurrentKey(2);
		assertEquals("2", restored1.value());
		restored1.update("3"); // s, NEW
		assertEquals("3", restored1.value());

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals((Integer) 3, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ Snapshot #2 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality NEW having 2 serialize calls
		KeyedStateHandle snapshot2 = runSnapshot(
			backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);
		backend.dispose();

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		} else {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		}
		CustomStringSerializer.resetCountingMaps();

		// and restore once with NEW from NEW so that we see a read using the NEW serializer
		// on the file backend
		ValueStateDescriptor<String> newKvId2 = new ValueStateDescriptor<>(
			"id",
			new CustomStringSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));

		// ============ Restore from snapshot #2 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality RESTORE having 2 deserialize calls
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals((Integer) 2, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));
		}
		CustomStringSerializer.resetCountingMaps();

		backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId2);
		snapshot2.discardState();
		snapshot1.discardState();

		backend.dispose();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testValueStateWithNewSerializer() throws Exception {
		CustomStringSerializer.resetCountingMaps();

		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>(
			"id",
			new CustomStringSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.INITIAL));
		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

		// ============ Modifications to the state ============
		//  For eager serialization backends:
		//    This should result in serializer personality INITIAL having 2 serialize calls
		//
		//  For lazy serialization backends:
		//    This should not result in any serialize / deserialize calls

		backend.setCurrentKey(1);
		state.update("1");
		backend.setCurrentKey(2);
		state.update("2");
		backend.setCurrentKey(1);

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ Snapshot #1 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality INITIAL having 2 serialize calls
		KeyedStateHandle snapshot1 = runSnapshot(
			backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);
		backend.dispose();

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		} else {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ Restore from snapshot #1 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality RESTORE having 2 deserialize calls
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.INITIAL));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.INITIAL));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals((Integer) 2, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));
		}
		CustomStringSerializer.resetCountingMaps();

		ValueStateDescriptor<String> newKvId = new ValueStateDescriptor<>("id",
			new CustomStringSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));
		ValueState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId);

		// ============ More modifications to the state ============
		//  For eager serialization backends:
		//    This should result in serializer personality NEW having 2 serialize calls and 3 deserialize calls
		//
		//  For lazy serialization backends:
		//    This should not result in any serialize / deserialize calls

		backend.setCurrentKey(1);
		assertEquals("1", restored1.value());
		restored1.update("1");
		backend.setCurrentKey(2);
		assertEquals("2", restored1.value());
		restored1.update("3");
		assertEquals("3", restored1.value());

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals((Integer) 3, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ Snapshot #2 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality NEW having 2 serialize calls
		KeyedStateHandle snapshot2 = runSnapshot(
			backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);
		snapshot1.discardState();
		backend.dispose();

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		} else {
			assertEquals((Integer) 2, CustomStringSerializer.serializeCalled.get(SerializerVersion.NEW));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.NEW));
		}
		CustomStringSerializer.resetCountingMaps();

		// ============ Restore from snapshot #2 ============
		//  For eager serialization backends:
		//    This should not result in any serialize / deserialize calls
		//
		//  For lazy serialization backends:
		//    This should result in serializer personality RESTORE having 2 deserialize calls
		ValueStateDescriptor<String> newKvId2 = new ValueStateDescriptor<>("id",
			new CustomStringSerializer(SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);

		if (getStateBackendSerializationTimeliness() == BackendSerializationTimeliness.ON_ACCESS) {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals(null, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));
		} else {
			assertEquals(null, CustomStringSerializer.serializeCalled.get(SerializerVersion.RESTORE));
			assertEquals((Integer) 2, CustomStringSerializer.deserializeCalled.get(SerializerVersion.RESTORE));
		}
		CustomStringSerializer.resetCountingMaps();

		backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId2);
		snapshot2.discardState();
		backend.dispose();
	}

	public static class CustomStringSerializer extends TypeSerializer<String> {

		private static final long serialVersionUID = 1L;

		private static final String EMPTY = "";

		private SerializerCompatibilityType compatibilityType;
		private SerializerVersion serializerVersion;

		// for counting how often the methods were called from serializers of the different personalities
		public static Map<SerializerVersion, Integer> serializeCalled = new HashMap<>();
		public static Map<SerializerVersion, Integer> deserializeCalled = new HashMap<>();

		static void resetCountingMaps() {
			serializeCalled = new HashMap<>();
			deserializeCalled = new HashMap<>();
		}

		CustomStringSerializer(
			SerializerCompatibilityType compatibilityType,
			SerializerVersion serializerVersion) {
			this.compatibilityType = compatibilityType;
			this.serializerVersion = serializerVersion;
		}

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public String createInstance() {
			return EMPTY;
		}

		@Override
		public String copy(String from) {
			return from;
		}

		@Override
		public String copy(String from, String reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(String record, DataOutputView target) throws IOException {
			serializeCalled.compute(serializerVersion, (k, v) -> v == null ? 1 : v + 1);
			StringValue.writeString(record, target);
		}

		@Override
		public String deserialize(DataInputView source) throws IOException {
			deserializeCalled.compute(serializerVersion, (k, v) -> v == null ? 1 : v + 1);
			return StringValue.readString(source);
		}

		@Override
		public String deserialize(String record, DataInputView source) throws IOException {
			deserializeCalled.compute(serializerVersion, (k, v) -> v == null ? 1 : v + 1);
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			StringValue.copyString(source, target);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof CustomStringSerializer;
		}

		@Override
		public TypeSerializer<String> duplicate() {
			return this;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof CustomStringSerializer;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}

		@Override
		public TypeSerializerSnapshot<String> snapshotConfiguration() {
			return new CustomStringSerializerSnapshot(compatibilityType);
		}
	}

	public static class CustomStringSerializerSnapshot implements TypeSerializerSnapshot<String> {

		private SerializerCompatibilityType compatibilityType;

		public CustomStringSerializerSnapshot() {}

		public CustomStringSerializerSnapshot(SerializerCompatibilityType compatibilityType) {
			this.compatibilityType = compatibilityType;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeUTF(compatibilityType.toString());
		}

		@Override
		public void read(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
			compatibilityType = SerializerCompatibilityType.valueOf(in.readUTF());
		}

		@Override
		public TypeSerializer<String> restoreSerializer() {
			return new CustomStringSerializer(compatibilityType, SerializerVersion.RESTORE);

		}

		@Override
		public <NS extends TypeSerializer<String>> TypeSerializerSchemaCompatibility<String, NS> resolveSchemaCompatibility(NS newSerializer) {
			if (compatibilityType == SerializerCompatibilityType.COMPATIBLE_AS_IS) {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
			}
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof CustomStringSerializerSnapshot;
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
		public <NS extends TypeSerializer<VoidNamespace>> TypeSerializerSchemaCompatibility<VoidNamespace, NS> resolveSchemaCompatibility(NS newSerializer) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}

		@Override
		public void write(DataOutputView out) throws IOException {}

		@Override
		public void read(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {}

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

	protected abstract B getStateBackend() throws Exception;

	protected abstract BackendSerializationTimeliness getStateBackendSerializationTimeliness();

	private CheckpointStreamFactory createStreamFactory() throws Exception {
		if (checkpointStorageLocation == null) {
			checkpointStorageLocation = getStateBackend()
				.createCheckpointStorage(new JobID())
				.initializeLocationForCheckpoint(1L);
		}
		return checkpointStorageLocation;
	}

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer) throws Exception {
		return createKeyedBackend(keySerializer, new DummyEnvironment());
	}

	private  <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer, Environment env) throws Exception {
		return createKeyedBackend(
			keySerializer,
			10,
			new KeyGroupRange(0, 9),
			env);
	}

	private  <K> AbstractKeyedStateBackend<K> createKeyedBackend(
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

	private  <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(TypeSerializer<K> keySerializer, KeyedStateHandle state) throws Exception {
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

	private  <K> AbstractKeyedStateBackend<K> restoreKeyedBackend(
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
}
