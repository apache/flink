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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
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

// TODO: remove print statements

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

	protected abstract B getStateBackend() throws Exception;

	protected CheckpointStreamFactory createStreamFactory() throws Exception {
		if (checkpointStorageLocation == null) {
			checkpointStorageLocation = getStateBackend()
				.createCheckpointStorage(new JobID())
				.initializeLocationForCheckpoint(1L);
		}

		return checkpointStorageLocation;
	}

	protected <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer) throws Exception {
		return createKeyedBackend(keySerializer, new DummyEnvironment());
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
		return restoreKeyedBackend(keySerializer, state, new DummyEnvironment());
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

		backend.restore(new StateObjectCollection<>(state));

		return backend;
	}

	protected KeyedStateHandle runSnapshot(
		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture) throws Exception {

		if (!snapshotRunnableFuture.isDone()) {
			snapshotRunnableFuture.run();
		}

		SnapshotResult<KeyedStateHandle> snapshotResult = snapshotRunnableFuture.get();
		return snapshotResult.getJobManagerOwnedSnapshot();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testValueStateWithIncompatibleSerializer() throws Exception {
		CustomStringSerializer.resetCountingMaps();
		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>(
			"id",
			new CustomStringSerializer(
				SerializerCompatibilityType.INCOMPATIBLE, SerializerVersion.INITIAL));

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

		// some modifications to the state
		backend.setCurrentKey(1);
		state.update("1");
		backend.setCurrentKey(2);
		state.update("2");
		backend.setCurrentKey(1);

		System.out.println("SNAPSHOT");

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));

		backend.dispose();

		System.out.println("RESTORE");

		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

		ValueStateDescriptor<String> newKvId = new ValueStateDescriptor<>("id",
			new CustomStringSerializer(
				SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));

		ValueState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId);

		System.out.println("MESSING WITH STATE");

		// some modifications to the state
		backend.setCurrentKey(1);
		assertEquals("1", restored1.value());
		restored1.update("1");
		backend.setCurrentKey(2);
		assertEquals("2", restored1.value());
		restored1.update("3");
		assertEquals("3", restored1.value());

		// draw another snapshot so that we see serialization from the NEW serialize on
		// the file backend
		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));

		backend.dispose();

		// and restore once with NEW from NEW so that we see a read using the NEW serializer
		// on the file backend
		ValueStateDescriptor<String> newKvId2 = new ValueStateDescriptor<>("id",
			new CustomStringSerializer(
				SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));

		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
		backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId2);

		snapshot2.discardState();
		snapshot1.discardState();
		backend.dispose();

		System.out.println(CustomStringSerializer.serializeCalled);
		System.out.println(CustomStringSerializer.deserializeCalled);
		verifyValueStateWithIncompatibleSerializerCounts();
	}

	protected abstract void verifyValueStateWithIncompatibleSerializerCounts();

	@Test
	@SuppressWarnings("unchecked")
	public void testValueStateWithReconfiguredSerializer() throws Exception {
		CustomStringSerializer.resetCountingMaps();
		CheckpointStreamFactory streamFactory = createStreamFactory();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>(
			"id",
			new CustomStringSerializer(
				SerializerCompatibilityType.COMPATIBLE_AFTER_RECONFIGURATION, SerializerVersion.INITIAL));

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);

		// some modifications to the state
		backend.setCurrentKey(1);
		state.update("1");
		backend.setCurrentKey(2);
		state.update("2");
		backend.setCurrentKey(1);

		System.out.println("SNAPSHOT");

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));

		backend.dispose();

		System.out.println("RESTORE");

		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
		snapshot1.discardState();

		ValueStateDescriptor<String> newKvId = new ValueStateDescriptor<>("id",
			new CustomStringSerializer(
				SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));

		ValueState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId);

		System.out.println("MESSING WITH STATE");

		// some modifications to the state
		backend.setCurrentKey(1);
		assertEquals("1", restored1.value());
		restored1.update("1");
		backend.setCurrentKey(2);
		assertEquals("2", restored1.value());
		restored1.update("3");
		assertEquals("3", restored1.value());

		// draw another snapshot so that we see serialization from the NEW serialize on
		// the file backend
		KeyedStateHandle snapshot2 = runSnapshot(backend.snapshot(2L, 3L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));

		backend.dispose();

		// and restore once with NEW from NEW so that we see a read using the NEW serializer
		// on the file backend
		ValueStateDescriptor<String> newKvId2 = new ValueStateDescriptor<>("id",
			new CustomStringSerializer(
				SerializerCompatibilityType.COMPATIBLE_AS_IS, SerializerVersion.NEW));

		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);
		backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, newKvId2);
		snapshot2.discardState();
		backend.dispose();

		System.out.println(CustomStringSerializer.serializeCalled);
		System.out.println(CustomStringSerializer.deserializeCalled);
		verifyValueStateWithReconfiguredSerializerCounts();
	}

	protected abstract void verifyValueStateWithReconfiguredSerializerCounts();


//	@Test
//	@SuppressWarnings("unchecked")
//	public void testListState() throws Exception {
//		CustomStringSerializer.resetCountingMaps();
//		CheckpointStreamFactory streamFactory = createStreamFactory();
//		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);
//
//		ListStateDescriptor<String> kvId = new ListStateDescriptor<>(
//			"id",
//			new CustomStringSerializer(
//				SerializerCompatibilityType.COMPATIBLE_AFTER_RECONFIGURATION, SerializerVersion.INITIAL));
//
//		ListState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, CustomVoidNamespaceSerializer.INSTANCE, kvId);
//
//		// some modifications to the state
//		backend.setCurrentKey(1);
//		assertNull(state.get());
//		state.add("1");
//		backend.setCurrentKey(2);
//		assertNull(state.get());
//		state.add("2");
//		backend.setCurrentKey(1);
//		assertThat(state.get(), containsInAnyOrder("1"));
//
//		System.out.println("SNAPSHOT");
//
//		// draw a snapshot
//		KeyedStateHandle snapshot1 = runSnapshot(backend.snapshot(1L, 2L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()));
//
//		backend.dispose();
//
//		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
//		snapshot1.discardState();
//
//		ListStateDescriptor<String> newKvId = new ListStateDescriptor<>(
//			"id",
//			new CustomStringSerializer(
//				SerializerCompatibilityType.COMPATIBLE_AFTER_RECONFIGURATION, SerializerVersion.NEW));
//
//		System.out.println("RESTORE");
//		ListState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, newKvId);
//
//		System.out.println("MESSING WITH STATE");
//
//		// some modifications to the state
//		backend.setCurrentKey(1);
//		assertThat(state.get(), containsInAnyOrder("1"));
//		restored1.add("1");
//		backend.setCurrentKey(2);
//		assertThat(state.get(), containsInAnyOrder("2"));
//		restored1.add("3");
//		assertThat(state.get(), containsInAnyOrder("2", "3"));
//
//		backend.dispose();
//	}

	/**
	 * Different "personalities" of {@link CustomStringSerializer}. Instead of creating
	 * different classes we parameterize the serializer with this and
	 * {@link CustomStringSerializerConfigSnapshot} will instantiate serializers with the correct
	 * personality.
	 */
	public enum SerializerVersion {
		INITIAL,
		RECONFIGURED,
		RESTORE,
		NEW
	}

	/**
	 * The compatibility behaviour of {@link CustomStringSerializer}. This controls what
	 * type of serializer {@link CustomStringSerializerConfigSnapshot} will create for
	 * the different methods that return/create serializers.
	 */
	public enum SerializerCompatibilityType {
		COMPATIBLE_AS_IS,
		COMPATIBLE_AFTER_RECONFIGURATION,
		INCOMPATIBLE
	}

	public static class CustomStringSerializer extends TypeSerializerSingleton<String> {
		private static final long serialVersionUID = 1L;

		private static final String EMPTY = "";

		private SerializerCompatibilityType compatibilityType;
		private SerializerVersion serializerVersion;

		// for counting how often the methods were called from serializers of the different
		// personalities
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
			System.out.println("SER " + serializerVersion);
			serializeCalled.compute(serializerVersion, (k, v) -> v == null ? 1 : v + 1);
			StringValue.writeString(record, target);
		}

		@Override
		public String deserialize(DataInputView source) throws IOException {
			System.out.println("DESER " + serializerVersion);
			deserializeCalled.compute(serializerVersion, (k, v) -> v == null ? 1 : v + 1);
			return StringValue.readString(source);
		}

		@Override
		public String deserialize(String record, DataInputView source) throws IOException {
			System.out.println("DESER " + serializerVersion);
			deserializeCalled.compute(serializerVersion, (k, v) -> v == null ? 1 : v + 1);
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			StringValue.copyString(source, target);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof StateBackendMigrationTestBase.CustomStringSerializer;
		}

		@Override
		protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
			return super.isCompatibleSerializationFormatIdentifier(identifier)
				|| identifier.equals(StringValue.class.getCanonicalName());
		}

		@Override
		public TypeSerializerConfigSnapshot<String> snapshotConfiguration() {
			return new CustomStringSerializerConfigSnapshot(compatibilityType);
		}
	}

	public static class CustomStringSerializerConfigSnapshot extends TypeSerializerConfigSnapshot<String> {

		private SerializerCompatibilityType compatibilityType;

		public CustomStringSerializerConfigSnapshot() {
		}

		public CustomStringSerializerConfigSnapshot(SerializerCompatibilityType compatibilityType) {
			this.compatibilityType = compatibilityType;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);
			System.out.println("WRITE SNAPSHOT: " + compatibilityType);
			out.writeUTF(compatibilityType.toString());
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);
			compatibilityType = SerializerCompatibilityType.valueOf(in.readUTF());
			System.out.println("READ SNAPSHOT: " + compatibilityType);
		}

		@Override
		public TypeSerializer<String> restoreSerializer() {
			if (compatibilityType == SerializerCompatibilityType.COMPATIBLE_AS_IS) {
				return new CustomStringSerializer(compatibilityType, SerializerVersion.NEW);
			} else {
				return new CustomStringSerializer(compatibilityType, SerializerVersion.RESTORE);
			}
		}

		@Override
		public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(TypeSerializer<?> newSerializer) {
			if (compatibilityType == SerializerCompatibilityType.COMPATIBLE_AFTER_RECONFIGURATION) {
				System.out.println(compatibilityType);
				return TypeSerializerSchemaCompatibility.compatibleAfterReconfiguration(
					new CustomStringSerializer(compatibilityType, SerializerVersion.RECONFIGURED));
			} else {
				System.out.println(compatibilityType);
				return TypeSerializerSchemaCompatibility.incompatible();
			}
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof CustomStringSerializerConfigSnapshot;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public int getVersion() {
			return 0;
		}
	}

	public static class CustomVoidNamespaceSerializer extends TypeSerializerSingleton<VoidNamespace> {
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
		public boolean canEqual(Object obj) {
			return obj instanceof CustomVoidNamespaceSerializer;
		}

		@Override
		public TypeSerializerConfigSnapshot<VoidNamespace> snapshotConfiguration() {
			return new CustomVoidNamespaceSerializerConfigSnapshot();
		}
	}

	public static class CustomVoidNamespaceSerializerConfigSnapshot extends TypeSerializerConfigSnapshot<VoidNamespace> {

		@Override
		public TypeSerializer<VoidNamespace> restoreSerializer() {
			return new CustomVoidNamespaceSerializer();
		}

		@Override
		public TypeSerializerSchemaCompatibility<VoidNamespace> resolveSchemaCompatibility(TypeSerializer<?> newSerializer) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof CustomVoidNamespaceSerializerConfigSnapshot;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public int getVersion() {
			return 0;
		}
	}
}
