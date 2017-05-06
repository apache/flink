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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationProxy;
import org.apache.flink.api.common.typeutils.TypeSerializerUtil;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Readers and writers for different versions of the {@link RegisteredOperatorBackendStateMetaInfo.Snapshot}.
 * Outdated formats are also kept here for documentation of history backlog.
 */
public class OperatorBackendStateMetaInfoSnapshotReaderWriters {

	// -------------------------------------------------------------------------------
	//  Writers
	//   - v1: Flink 1.2.x
	//   - v2: Flink 1.3.x
	// -------------------------------------------------------------------------------

	public static <S> OperatorBackendStateMetaInfoWriter getWriterForVersion(
			int version, RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {

		switch (version) {
			case 1:
				return new OperatorBackendStateMetaInfoWriterV1<>(stateMetaInfo);

			// current version
			case OperatorBackendSerializationProxy.VERSION:
				return new OperatorBackendStateMetaInfoWriterV2<>(stateMetaInfo);

			default:
				// guard for future
				throw new IllegalStateException(
						"Unrecognized operator backend state meta info writer version: " + version);
		}
	}

	public interface OperatorBackendStateMetaInfoWriter {
		void writeStateMetaInfo(DataOutputView out) throws IOException;
	}

	public static abstract class AbstractOperatorBackendStateMetaInfoWriter<S>
			implements OperatorBackendStateMetaInfoWriter {

		protected final RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo;

		public AbstractOperatorBackendStateMetaInfoWriter(RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {
			this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
		}
	}

	public static class OperatorBackendStateMetaInfoWriterV1<S> extends AbstractOperatorBackendStateMetaInfoWriter<S> {

		public OperatorBackendStateMetaInfoWriterV1(RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {
			super(stateMetaInfo);
		}

		@Override
		public void writeStateMetaInfo(DataOutputView out) throws IOException {
			out.writeUTF(stateMetaInfo.getName());
			out.writeByte(stateMetaInfo.getAssignmentMode().ordinal());
			new TypeSerializerSerializationProxy<>(stateMetaInfo.getPartitionStateSerializer()).write(out);
		}
	}

	public static class OperatorBackendStateMetaInfoWriterV2<S> extends AbstractOperatorBackendStateMetaInfoWriter<S> {

		public OperatorBackendStateMetaInfoWriterV2(RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {
			super(stateMetaInfo);
		}

		@Override
		public void writeStateMetaInfo(DataOutputView out) throws IOException {
			out.writeUTF(stateMetaInfo.getName());
			out.writeByte(stateMetaInfo.getAssignmentMode().ordinal());

			// write in a way that allows us to be fault-tolerant and skip blocks in the case of java serialization failures
			try (
				ByteArrayOutputStreamWithPos outWithPos = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outViewWrapper = new DataOutputViewStreamWrapper(outWithPos)) {

				new TypeSerializerSerializationProxy<>(stateMetaInfo.getPartitionStateSerializer()).write(outViewWrapper);

				// write the start offset of the config snapshot
				out.writeInt(outWithPos.getPosition());
				TypeSerializerUtil.writeSerializerConfigSnapshot(
					outViewWrapper,
					stateMetaInfo.getPartitionStateSerializerConfigSnapshot());

				// write the total number of bytes and flush
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

	public static <S> OperatorBackendStateMetaInfoReader<S> getReaderForVersion(
			int version, ClassLoader userCodeClassLoader) {

		switch (version) {
			case 1:
				return new OperatorBackendStateMetaInfoReaderV1<>(userCodeClassLoader);

			// current version
			case OperatorBackendSerializationProxy.VERSION:
				return new OperatorBackendStateMetaInfoReaderV2<>(userCodeClassLoader);

			default:
				// guard for future
				throw new IllegalStateException(
					"Unrecognized operator backend state meta info reader version: " + version);
		}
	}

	public interface OperatorBackendStateMetaInfoReader<S> {
		RegisteredOperatorBackendStateMetaInfo.Snapshot<S> readStateMetaInfo(DataInputView in) throws IOException;
	}

	public static abstract class AbstractOperatorBackendStateMetaInfoReader<S>
		implements OperatorBackendStateMetaInfoReader<S> {

		protected final ClassLoader userCodeClassLoader;

		public AbstractOperatorBackendStateMetaInfoReader(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		}
	}

	public static class OperatorBackendStateMetaInfoReaderV1<S> extends AbstractOperatorBackendStateMetaInfoReader<S> {

		public OperatorBackendStateMetaInfoReaderV1(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@Override
		public RegisteredOperatorBackendStateMetaInfo.Snapshot<S> readStateMetaInfo(DataInputView in) throws IOException {
			RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo =
				new RegisteredOperatorBackendStateMetaInfo.Snapshot<>();

			stateMetaInfo.setName(in.readUTF());
			stateMetaInfo.setAssignmentMode(OperatorStateHandle.Mode.values()[in.readByte()]);
			DataInputViewStream dis = new DataInputViewStream(in);
			try {
				TypeSerializer<S> stateSerializer = InstantiationUtil.deserializeObject(dis, userCodeClassLoader);
				stateMetaInfo.setPartitionStateSerializer(stateSerializer);
			} catch (ClassNotFoundException exception) {
				throw new IOException(exception);
			}

			// old versions do not contain the partition state serializer's configuration snapshot
			stateMetaInfo.setPartitionStateSerializerConfigSnapshot(null);

			return stateMetaInfo;
		}
	}

	public static class OperatorBackendStateMetaInfoReaderV2<S> extends AbstractOperatorBackendStateMetaInfoReader<S> {

		public OperatorBackendStateMetaInfoReaderV2(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@Override
		public RegisteredOperatorBackendStateMetaInfo.Snapshot<S> readStateMetaInfo(DataInputView in) throws IOException {
			RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo =
				new RegisteredOperatorBackendStateMetaInfo.Snapshot<>();

			stateMetaInfo.setName(in.readUTF());
			stateMetaInfo.setAssignmentMode(OperatorStateHandle.Mode.values()[in.readByte()]);

			// read start offset of configuration snapshot
			int configSnapshotStartOffset = in.readInt();

			int totalBytes = in.readInt();

			byte[] buffer = new byte[totalBytes];
			in.readFully(buffer);

			ByteArrayInputStreamWithPos inWithPos = new ByteArrayInputStreamWithPos(buffer);
			DataInputViewStreamWrapper inViewWrapper = new DataInputViewStreamWrapper(inWithPos);

			try {
				final TypeSerializerSerializationProxy<S> partitionStateSerializerProxy =
					new TypeSerializerSerializationProxy<>(userCodeClassLoader);
				partitionStateSerializerProxy.read(inViewWrapper);
				stateMetaInfo.setPartitionStateSerializer(partitionStateSerializerProxy.getTypeSerializer());
			} catch (IOException e) {
				stateMetaInfo.setPartitionStateSerializer(null);
			}

			// make sure we start from the partition state serializer bytes position
			inWithPos.setPosition(configSnapshotStartOffset);
			stateMetaInfo.setPartitionStateSerializerConfigSnapshot(
				TypeSerializerUtil.readSerializerConfigSnapshot(inViewWrapper, userCodeClassLoader));

			return stateMetaInfo;
		}
	}
}
