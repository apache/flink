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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationProxy;
import org.apache.flink.api.common.typeutils.TypeSerializerUtil;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Readers and writers for different versions of the {@link RegisteredKeyedBackendStateMetaInfo.Snapshot}.
 * Outdated formats are also kept here for documentation of history backlog.
 */
public class KeyedBackendStateMetaInfoSnapshotReaderWriters {

	// -------------------------------------------------------------------------------
	//  Writers
	//   - v1: Flink 1.2.x
	//   - v2: Flink 1.3.x
	// -------------------------------------------------------------------------------

	public static <N, S> KeyedBackendStateMetaInfoWriter getWriterForVersion(
		int version, RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> stateMetaInfo) {

		switch (version) {
			case 1:
			case 2:
				return new KeyedBackendStateMetaInfoWriterV1V2<>(stateMetaInfo);

			// current version
			case KeyedBackendSerializationProxy.VERSION:
				return new KeyedBackendStateMetaInfoWriterV3<>(stateMetaInfo);

			default:
				// guard for future
				throw new IllegalStateException(
							"Unrecognized keyed backend state meta info writer version: " + version);
		}
	}

	public interface KeyedBackendStateMetaInfoWriter {
		void writeStateMetaInfo(DataOutputView out) throws IOException;
	}

	static abstract class AbstractKeyedBackendStateMetaInfoWriter<N, S> implements KeyedBackendStateMetaInfoWriter {

		protected final RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> stateMetaInfo;

		public AbstractKeyedBackendStateMetaInfoWriter(RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> stateMetaInfo) {
			this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
		}

	}

	static class KeyedBackendStateMetaInfoWriterV1V2<N, S> extends AbstractKeyedBackendStateMetaInfoWriter<N, S> {

		public KeyedBackendStateMetaInfoWriterV1V2(RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> stateMetaInfo) {
			super(stateMetaInfo);
		}

		@Override
		public void writeStateMetaInfo(DataOutputView out) throws IOException {
			out.writeInt(stateMetaInfo.getStateType().ordinal());
			out.writeUTF(stateMetaInfo.getName());

			new TypeSerializerSerializationProxy<>(stateMetaInfo.getNamespaceSerializer()).write(out);
			new TypeSerializerSerializationProxy<>(stateMetaInfo.getStateSerializer()).write(out);
		}
	}

	static class KeyedBackendStateMetaInfoWriterV3<N, S> extends AbstractKeyedBackendStateMetaInfoWriter<N, S> {

		public KeyedBackendStateMetaInfoWriterV3(RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> stateMetaInfo) {
			super(stateMetaInfo);
		}

		@Override
		public void writeStateMetaInfo(DataOutputView out) throws IOException {
			out.writeInt(stateMetaInfo.getStateType().ordinal());
			out.writeUTF(stateMetaInfo.getName());

			// write in a way that allows us to be fault-tolerant and skip blocks in the case of java serialization failures
			try (
				ByteArrayOutputStreamWithPos outWithPos = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outViewWrapper = new DataOutputViewStreamWrapper(outWithPos)) {

				new TypeSerializerSerializationProxy<>(stateMetaInfo.getNamespaceSerializer()).write(outViewWrapper);

				// write current offset, which represents the start offset of the state serializer
				out.writeInt(outWithPos.getPosition());
				new TypeSerializerSerializationProxy<>(stateMetaInfo.getStateSerializer()).write(outViewWrapper);

				// write current offset, which represents the start of the configuration snapshots
				out.writeInt(outWithPos.getPosition());
				TypeSerializerUtil.writeSerializerConfigSnapshot(outViewWrapper, stateMetaInfo.getNamespaceSerializerConfigSnapshot());
				TypeSerializerUtil.writeSerializerConfigSnapshot(outViewWrapper, stateMetaInfo.getStateSerializerConfigSnapshot());

				// write total number of bytes and then flush
				out.writeInt(outWithPos.getPosition());
				out.write(outWithPos.getBuf(), 0, outWithPos.getPosition());
			}
		}
	}


	// -------------------------------------------------------------------------------
	//  Readers
	//   - v1: Flink 1.2.x
	//   - v2: Flink 1.3.x
	// -------------------------------------------------------------------------------

	public static KeyedBackendStateMetaInfoReader getReaderForVersion(
			int version, ClassLoader userCodeClassLoader) {

		switch (version) {
			case 1:
			case 2:
				return new KeyedBackendStateMetaInfoReaderV1V2<>(userCodeClassLoader);

			// current version
			case KeyedBackendSerializationProxy.VERSION:
				return new KeyedBackendStateMetaInfoReaderV3<>(userCodeClassLoader);

			default:
				// guard for future
				throw new IllegalStateException(
							"Unrecognized keyed backend state meta info reader version: " + version);
		}
	}

	public interface KeyedBackendStateMetaInfoReader<N, S> {
		RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> readStateMetaInfo(DataInputView in) throws IOException;
	}

	static abstract class AbstractKeyedBackendStateMetaInfoReader implements KeyedBackendStateMetaInfoReader {

		protected final ClassLoader userCodeClassLoader;

		public AbstractKeyedBackendStateMetaInfoReader(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		}

	}

	static class KeyedBackendStateMetaInfoReaderV1V2<N, S> extends AbstractKeyedBackendStateMetaInfoReader {

		public KeyedBackendStateMetaInfoReaderV1V2(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@Override
		public RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> readStateMetaInfo(DataInputView in) throws IOException {
			RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> metaInfo =
				new RegisteredKeyedBackendStateMetaInfo.Snapshot<>();

			metaInfo.setStateType(StateDescriptor.Type.values()[in.readInt()]);
			metaInfo.setName(in.readUTF());

			final TypeSerializerSerializationProxy<N> namespaceSerializerProxy =
				new TypeSerializerSerializationProxy<>(userCodeClassLoader);
			namespaceSerializerProxy.read(in);
			metaInfo.setNamespaceSerializer(namespaceSerializerProxy.getTypeSerializer());

			final TypeSerializerSerializationProxy<S> stateSerializerProxy =
				new TypeSerializerSerializationProxy<>(userCodeClassLoader);
			stateSerializerProxy.read(in);
			metaInfo.setStateSerializer(stateSerializerProxy.getTypeSerializer());

			// older versions do not contain the configuration snapshot
			metaInfo.setNamespaceSerializerConfigSnapshot(null);
			metaInfo.setStateSerializerConfigSnapshot(null);

			return metaInfo;
		}
	}

	static class KeyedBackendStateMetaInfoReaderV3<N, S> extends AbstractKeyedBackendStateMetaInfoReader {

		public KeyedBackendStateMetaInfoReaderV3(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@Override
		public RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> readStateMetaInfo(DataInputView in) throws IOException {
			RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> metaInfo =
				new RegisteredKeyedBackendStateMetaInfo.Snapshot<>();

			metaInfo.setStateType(StateDescriptor.Type.values()[in.readInt()]);
			metaInfo.setName(in.readUTF());

			// read offsets
			int stateSerializerStartOffset = in.readInt();
			int configSnapshotsStartOffset = in.readInt();

			int totalBytes = in.readInt();

			byte[] buffer = new byte[totalBytes];
			in.readFully(buffer);

			ByteArrayInputStreamWithPos inWithPos = new ByteArrayInputStreamWithPos(buffer);
			DataInputViewStreamWrapper inViewWrapper = new DataInputViewStreamWrapper(inWithPos);

			try {
				final TypeSerializerSerializationProxy<N> namespaceSerializerProxy =
					new TypeSerializerSerializationProxy<>(userCodeClassLoader);
				namespaceSerializerProxy.read(inViewWrapper);
				metaInfo.setNamespaceSerializer(namespaceSerializerProxy.getTypeSerializer());
			} catch (IOException e) {
				metaInfo.setNamespaceSerializer(null);
			}

			// make sure we start from the state serializer bytes position
			inWithPos.setPosition(stateSerializerStartOffset);
			try {
				final TypeSerializerSerializationProxy<S> stateSerializerProxy =
					new TypeSerializerSerializationProxy<>(userCodeClassLoader);
				stateSerializerProxy.read(inViewWrapper);
				metaInfo.setStateSerializer(stateSerializerProxy.getTypeSerializer());
			} catch (IOException e) {
				metaInfo.setStateSerializer(null);
			}

			// make sure we start from the config snapshot bytes position
			inWithPos.setPosition(configSnapshotsStartOffset);
			metaInfo.setNamespaceSerializerConfigSnapshot(
				TypeSerializerUtil.readSerializerConfigSnapshot(inViewWrapper, userCodeClassLoader));
			metaInfo.setStateSerializerConfigSnapshot(
				TypeSerializerUtil.readSerializerConfigSnapshot(inViewWrapper, userCodeClassLoader));

			return metaInfo;
		}
	}
}
