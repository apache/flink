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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static factory that gives out the write and readers for different versions of {@link StateMetaInfoSnapshot}.
 */
public class StateMetaInfoSnapshotReadersWriters {

	private StateMetaInfoSnapshotReadersWriters() {}

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

		if (readVersion < CURRENT_STATE_META_INFO_SNAPSHOT_VERSION) {
			switch (stateTypeHint) {
				case KEYED_STATE:
					return getLegacyKeyedStateMetaInfoReader(readVersion);
				case OPERATOR_STATE:
					return getLegacyOperatorStateMetaInfoReader(readVersion);
				default:
					throw new IllegalArgumentException("Unsupported state type hint: " + stateTypeHint +
						" with version " + readVersion);
			}
		} else {
			return getReader(readVersion);
		}
	}

	/**
	 * Returns a reader for {@link StateMetaInfoSnapshot} with the requested state type and version number.
	 *
	 * @param readVersion the format version to read.
	 * @return the requested reader.
	 */
	@Nonnull
	public static StateMetaInfoReader getReader(int readVersion) {
		if (readVersion == CURRENT_STATE_META_INFO_SNAPSHOT_VERSION) {
			// latest version shortcut
			return CurrentReaderImpl.INSTANCE;
		} else {
			throw new IllegalArgumentException("Unsupported read version for state meta info: " + readVersion);
		}
	}

	@Nonnull
	private static StateMetaInfoReader getLegacyKeyedStateMetaInfoReader(int readVersion) {
		switch (readVersion) {
			case 1:
			case 2:
				return LegacyStateMetaInfoReaders.KeyedBackendStateMetaInfoReaderV1V2.INSTANCE;
			case 3:
			case 4:
				return LegacyStateMetaInfoReaders.KeyedBackendStateMetaInfoReaderV3V4.INSTANCE;
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
				return LegacyStateMetaInfoReaders.OperatorBackendStateMetaInfoReaderV1.INSTANCE;
			case 2:
			case 3:
				return LegacyStateMetaInfoReaders.OperatorBackendStateMetaInfoReaderV2V3.INSTANCE;
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
}
