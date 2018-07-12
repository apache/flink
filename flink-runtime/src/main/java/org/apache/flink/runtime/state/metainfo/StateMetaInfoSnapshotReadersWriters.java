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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static factory that gives out the write and readers for different versions of {@link StateMetaInfoSnapshot}.
 */
public class StateMetaInfoSnapshotReadersWriters {

	/**
	 * Current version for the serialization format of {@link StateMetaInfoSnapshotReadersWriters}.
	 * - v5: Flink 1.6.x
	 */
	public static final int CURRENT_STATE_META_INFO_SNAPSHOT_VERSION = 5;

	/**
	 * Enum for backeards compatibility. This gives a hint about the expected state type for which a
	 * {@link StateMetaInfoSnapshot} should be deserialized.
	 *
	 * TODO this can go away after we eventually drop backwards compatibility with all versions < 5.
	 */
	public enum StateTypeHint {
		KEYED_STATE,
		OPERATOR_STATE
	}

	/**
	 * Returns the writer for {@link StateMetaInfoSnapshot}.
	 */
	@Nonnull
	public static StateMetaInfoWriter getWriter() {
		return CurrentWriterImpl.INSTANCE;
	}

	/**
	 * Returns a reader for {@link StateMetaInfoSnapshot} with the requested state type and version number.
	 *
	 * @param readVersion the format version to read.
	 * @param stateTypeHint a hint about the expected type to read.
	 * @return the requested reader.
	 */
	@Nonnull
	public static StateMetaInfoReader getReader(int readVersion, @Nonnull StateTypeHint stateTypeHint) {

		if (readVersion == CURRENT_STATE_META_INFO_SNAPSHOT_VERSION) {
			// latest version shortcut
			return CurrentReaderImpl.INSTANCE;
		}

		if (readVersion > CURRENT_STATE_META_INFO_SNAPSHOT_VERSION) {
			throw new IllegalArgumentException("Unsupported read version for state meta info: " + readVersion);
		}

		switch (stateTypeHint) {
			case KEYED_STATE:
				return getLegacyKeyedStateMetaInfoReader(readVersion);
			case OPERATOR_STATE:
				return getLegacyOperatorStateMetaInfoReader(readVersion);
			default:
				throw new IllegalArgumentException("Unsupported state type hint: " + stateTypeHint);
		}
	}

	@Nonnull
	private static StateMetaInfoReader getLegacyKeyedStateMetaInfoReader(int readVersion) {
		switch (readVersion) {
			case 1:
			case 2:
				return KeyedBackendStateMetaInfoReaderV1V2.INSTANCE;
			case 3:
			case 4:
				return KeyedBackendStateMetaInfoReaderV3V4.INSTANCE;
			default:
				// guard for future
				throw new IllegalStateException(
					"Unrecognized keyed backend state meta info writer version: " + readVersion);
		}
	}

	@Nonnull
	private static StateMetaInfoReader getLegacyOperatorStateMetaInfoReader(int readVersion) {
		switch (readVersion) {
			case 1:
				return OperatorBackendStateMetaInfoReaderV1.INSTANCE;
			case 2:
			case 3:
				return OperatorBackendStateMetaInfoReaderV2V3.INSTANCE;
			default:
				// guard for future
				throw new IllegalStateException(
					"Unrecognized operator backend state meta info writer version: " + readVersion);
		}
	}

	//----------------------------------------------------------

	/**
	 * Implementation of {@link StateMetaInfoWriter}.
	 */
	static class CurrentWriterImpl implements StateMetaInfoWriter {

		private static final CurrentWriterImpl INSTANCE = new CurrentWriterImpl();

		@Override
		public void writeStateMetaInfoSnapshot(
			@Nonnull StateMetaInfoSnapshot snapshot,
			@Nonnull DataOutputView outputView) throws IOException {
			final Map<String, String> optionsMap = snapshot.getOptionsImmutable();
			final Map<String, TypeSerializer<?>> serializerMap = snapshot.getSerializersImmutable();
			final Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshotsMap =
				snapshot.getSerializerConfigSnapshotsImmutable();
			Preconditions.checkState(serializerMap.size() == serializerConfigSnapshotsMap.size());

			outputView.writeUTF(snapshot.getName());
			outputView.writeInt(snapshot.getBackendStateType().ordinal());
			outputView.writeInt(optionsMap.size());
			for (Map.Entry<String, String> entry : optionsMap.entrySet()) {
				outputView.writeUTF(entry.getKey());
				outputView.writeUTF(entry.getValue());
			}

			outputView.writeInt(serializerMap.size());
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersWithConfig =
				new ArrayList<>(serializerMap.size());

			for (Map.Entry<String, TypeSerializer<?>> entry : serializerMap.entrySet()) {
				final String key = entry.getKey();
				outputView.writeUTF(key);

				TypeSerializerConfigSnapshot configForSerializer =
					Preconditions.checkNotNull(serializerConfigSnapshotsMap.get(key));

				serializersWithConfig.add(new Tuple2<>(entry.getValue(), configForSerializer));
			}

			TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(outputView, serializersWithConfig);
		}
	}

	/**
	 * Implementation of {@link StateMetaInfoReader} for the current version and generic for all state types.
	 */
	static class CurrentReaderImpl implements StateMetaInfoReader {

		private static final CurrentReaderImpl INSTANCE = new CurrentReaderImpl();

		@Nonnull
		@Override
		public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
			@Nonnull DataInputView inputView,
			@Nonnull ClassLoader userCodeClassLoader) throws IOException {

			final String stateName = inputView.readUTF();
			final StateMetaInfoSnapshot.BackendStateType stateType =
				StateMetaInfoSnapshot.BackendStateType.values()[inputView.readInt()];
			final int numOptions = inputView.readInt();
			HashMap<String, String> optionsMap = new HashMap<>(numOptions);
			for (int i = 0; i < numOptions; ++i) {
				String key = inputView.readUTF();
				String value = inputView.readUTF();
				optionsMap.put(key, value);
			}
			final int numSerializer = inputView.readInt();
			final ArrayList<String> serializerKeys = new ArrayList<>(numSerializer);
			final HashMap<String, TypeSerializer<?>> serializerMap = new HashMap<>(numSerializer);
			final HashMap<String, TypeSerializerConfigSnapshot> serializerConfigsMap = new HashMap<>(numSerializer);

			for (int i = 0; i < numSerializer; ++i) {
				serializerKeys.add(inputView.readUTF());
			}
			final List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersWithConfig =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(inputView, userCodeClassLoader);

			for (int i = 0; i < numSerializer; ++i) {
				String key = serializerKeys.get(i);
				final Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> serializerConfigTuple =
					serializersWithConfig.get(i);
				serializerMap.put(key, serializerConfigTuple.f0);
				serializerConfigsMap.put(key, serializerConfigTuple.f1);
			}

			return new StateMetaInfoSnapshot(stateName, stateType, optionsMap, serializerConfigsMap, serializerMap);
		}
	}

	//----------------------------------------------------------

	/**
	 * Implementation of {@link StateMetaInfoReader} for version 3 of keyed state.
	 * - v3: Flink 1.4.x, 1.5.x
	 */
	static class KeyedBackendStateMetaInfoReaderV3V4 implements StateMetaInfoReader {

		static final KeyedBackendStateMetaInfoReaderV3V4 INSTANCE = new KeyedBackendStateMetaInfoReaderV3V4();

		@Nonnull
		@Override
		public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
			@Nonnull DataInputView in, @Nonnull ClassLoader userCodeClassLoader) throws IOException {

			final StateDescriptor.Type stateDescType = StateDescriptor.Type.values()[in.readInt()];
			final String stateName = in.readUTF();
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader);

			Map<String, String> optionsMap = Collections.singletonMap(
				StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
				stateDescType.toString());


			Map<String, TypeSerializer<?>> serializerMap = new HashMap<>(2);
			serializerMap.put(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
				serializersAndConfigs.get(0).f0);
			serializerMap.put(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
				serializersAndConfigs.get(1).f0);

			Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshotMap = new HashMap<>(2);
			serializerConfigSnapshotMap.put(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
				serializersAndConfigs.get(0).f1);
			serializerConfigSnapshotMap.put(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
				serializersAndConfigs.get(1).f1);

			return new StateMetaInfoSnapshot(
				stateName,
				StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
				optionsMap,
				serializerConfigSnapshotMap,
				serializerMap);
		}
	}

	/**
	 * Implementation of {@link StateMetaInfoReader} for version 1 and 2 of keyed state.
	 * - v1: Flink 1.2.x
	 * - v2: Flink 1.3.x
	 */
	static class KeyedBackendStateMetaInfoReaderV1V2 implements StateMetaInfoReader {

		static final KeyedBackendStateMetaInfoReaderV1V2 INSTANCE = new KeyedBackendStateMetaInfoReaderV1V2();

		@Nonnull
		@Override
		public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
			@Nonnull DataInputView in,
			@Nonnull ClassLoader userCodeClassLoader) throws IOException {

			final StateDescriptor.Type stateDescType = StateDescriptor.Type.values()[in.readInt()];
			final String stateName = in.readUTF();

			Map<String, String> optionsMap = Collections.singletonMap(
				StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
				stateDescType.toString());


			Map<String, TypeSerializer<?>> serializerMap = new HashMap<>(2);
			serializerMap.put(
				StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
				TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true));
			serializerMap.put(
				StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
				TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true));
			return new StateMetaInfoSnapshot(
				stateName,
				StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
				optionsMap,
				Collections.emptyMap(),
				serializerMap);
		}
	}

	/**
	 * Unified reader for older versions of operator (version 2 and 3) AND broadcast state (version 3).
	 *
	 * - v2: Flink 1.3.x, 1.4.x
	 * - v3: Flink 1.5.x
	 */
	static class OperatorBackendStateMetaInfoReaderV2V3 implements StateMetaInfoReader {

		static final  OperatorBackendStateMetaInfoReaderV2V3 INSTANCE = new OperatorBackendStateMetaInfoReaderV2V3();

		private static final String[] ORDERED_KEY_STRINGS =
			new String[]{
				StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString(),
				StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString()};

		@Nonnull
		@Override
		public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
			@Nonnull DataInputView in,
			@Nonnull ClassLoader userCodeClassLoader) throws IOException {

			final String name = in.readUTF();
			final OperatorStateHandle.Mode mode = OperatorStateHandle.Mode.values()[in.readByte()];

			Map<String, String> optionsMap = Collections.singletonMap(
				StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString(),
				mode.toString());

			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> stateSerializerAndConfigList =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader);

			final int listSize = stateSerializerAndConfigList.size();
			StateMetaInfoSnapshot.BackendStateType stateType = listSize == 1 ?
				StateMetaInfoSnapshot.BackendStateType.OPERATOR : StateMetaInfoSnapshot.BackendStateType.BROADCAST;
			Map<String, TypeSerializer<?>> serializerMap = new HashMap<>(listSize);
			Map<String, TypeSerializerConfigSnapshot> serializerConfigsMap = new HashMap<>(listSize);
			for (int i = 0; i < listSize; ++i) {
				Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> serializerAndConf =
					stateSerializerAndConfigList.get(i);

				// this particular mapping happens to support both, V2 and V3
				String serializerKey = ORDERED_KEY_STRINGS[ORDERED_KEY_STRINGS.length - 1 - i];

				serializerMap.put(
					serializerKey,
					serializerAndConf.f0);
				serializerConfigsMap.put(
					serializerKey,
					serializerAndConf.f1);
			}

			return new StateMetaInfoSnapshot(
				name,
				stateType,
				optionsMap,
				serializerConfigsMap,
				serializerMap);
		}
	}

	/**
	 * Reader for older versions of operator state (version 1).
	 * - v1: Flink 1.2.x
	 */
	public static class OperatorBackendStateMetaInfoReaderV1 implements StateMetaInfoReader {

		static final  OperatorBackendStateMetaInfoReaderV1 INSTANCE = new OperatorBackendStateMetaInfoReaderV1();

		@Nonnull
		@Override
		public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
			@Nonnull DataInputView in,
			@Nonnull ClassLoader userCodeClassLoader) throws IOException {

			final String name = in.readUTF();
			final OperatorStateHandle.Mode mode = OperatorStateHandle.Mode.values()[in.readByte()];
			final Map<String, String> optionsMap = Collections.singletonMap(
				StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString(),
				mode.toString());

			DataInputViewStream dis = new DataInputViewStream(in);
			ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();

			try (
				InstantiationUtil.FailureTolerantObjectInputStream ois =
					new InstantiationUtil.FailureTolerantObjectInputStream(dis, userCodeClassLoader)) {
				Thread.currentThread().setContextClassLoader(userCodeClassLoader);
				TypeSerializer<?> stateSerializer = (TypeSerializer<?>) ois.readObject();
				return new StateMetaInfoSnapshot(
					name,
					StateMetaInfoSnapshot.BackendStateType.OPERATOR,
					optionsMap,
					Collections.emptyMap(),
					Collections.singletonMap(
						StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
						stateSerializer));
			} catch (ClassNotFoundException exception) {
				throw new IOException(exception);
			} finally {
				Thread.currentThread().setContextClassLoader(previousClassLoader);
			}

		}
	}
}
