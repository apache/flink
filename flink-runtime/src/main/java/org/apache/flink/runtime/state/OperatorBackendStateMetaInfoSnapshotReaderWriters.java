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
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Readers and writers for different versions of the {@link RegisteredOperatorBackendStateMetaInfo.Snapshot}.
 * Outdated formats are also kept here for documentation of history backlog.
 */
public class OperatorBackendStateMetaInfoSnapshotReaderWriters {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorBackendStateMetaInfoSnapshotReaderWriters.class);

	// -------------------------------------------------------------------------------
	//  Writers
	//   - v1: Flink 1.2.x
	//   - v2: Flink 1.3.x
	//   - v3: Flink 1.5.x
	// -------------------------------------------------------------------------------

	public static <S> OperatorBackendStateMetaInfoWriter getOperatorStateWriterForVersion(
			int version, RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {

		switch (version) {
			case 1:
				return new OperatorBackendStateMetaInfoWriterV1<>(stateMetaInfo);

			// current version
			case 2:
			case OperatorBackendSerializationProxy.VERSION:
				return new OperatorBackendStateMetaInfoWriterV2<>(stateMetaInfo);

			default:
				// guard for future
				throw new IllegalStateException(
						"Unrecognized operator backend state meta info writer version: " + version);
		}
	}

	public static <K, V> BroadcastStateMetaInfoWriter getBroadcastStateWriterForVersion(
			final int version,
			final RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> broadcastStateMetaInfo) {

		switch (version) {
			// current version
			case OperatorBackendSerializationProxy.VERSION:
				return new BroadcastStateMetaInfoWriterV3<>(broadcastStateMetaInfo);

			default:
				// guard for future
				throw new IllegalStateException(
						"Unrecognized broadcast state meta info writer version: " + version);
		}
	}

	public interface OperatorBackendStateMetaInfoWriter {
		void writeOperatorStateMetaInfo(DataOutputView out) throws IOException;
	}

	public interface BroadcastStateMetaInfoWriter {
		void writeBroadcastStateMetaInfo(final DataOutputView out) throws IOException;
	}

	public static abstract class AbstractOperatorBackendStateMetaInfoWriter<S>
			implements OperatorBackendStateMetaInfoWriter {

		protected final RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo;

		public AbstractOperatorBackendStateMetaInfoWriter(RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {
			this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
		}
	}

	public abstract static class AbstractBroadcastStateMetaInfoWriter<K, V>
			implements BroadcastStateMetaInfoWriter {

		protected final RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> broadcastStateMetaInfo;

		public AbstractBroadcastStateMetaInfoWriter(final RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> broadcastStateMetaInfo) {
			this.broadcastStateMetaInfo = Preconditions.checkNotNull(broadcastStateMetaInfo);
		}
	}

	public static class OperatorBackendStateMetaInfoWriterV1<S> extends AbstractOperatorBackendStateMetaInfoWriter<S> {

		public OperatorBackendStateMetaInfoWriterV1(RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {
			super(stateMetaInfo);
		}

		@Override
		public void writeOperatorStateMetaInfo(DataOutputView out) throws IOException {
			out.writeUTF(stateMetaInfo.getName());
			out.writeByte(stateMetaInfo.getAssignmentMode().ordinal());
			TypeSerializerSerializationUtil.writeSerializer(out, stateMetaInfo.getPartitionStateSerializer());
		}
	}

	public static class OperatorBackendStateMetaInfoWriterV2<S> extends AbstractOperatorBackendStateMetaInfoWriter<S> {

		public OperatorBackendStateMetaInfoWriterV2(RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo) {
			super(stateMetaInfo);
		}

		@Override
		public void writeOperatorStateMetaInfo(DataOutputView out) throws IOException {
			out.writeUTF(stateMetaInfo.getName());
			out.writeByte(stateMetaInfo.getAssignmentMode().ordinal());

			// write in a way that allows us to be fault-tolerant and skip blocks in the case of java serialization failures
			TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
				out,
				Collections.singletonList(new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
					stateMetaInfo.getPartitionStateSerializer(),
					stateMetaInfo.getPartitionStateSerializerConfigSnapshot())));
		}
	}

	public static class BroadcastStateMetaInfoWriterV3<K, V> extends AbstractBroadcastStateMetaInfoWriter<K, V> {

		public BroadcastStateMetaInfoWriterV3(
				final RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> broadcastStateMetaInfo) {
			super(broadcastStateMetaInfo);
		}

		@Override
		public void writeBroadcastStateMetaInfo(final DataOutputView out) throws IOException {
			out.writeUTF(broadcastStateMetaInfo.getName());
			out.writeByte(broadcastStateMetaInfo.getAssignmentMode().ordinal());

			// write in a way that allows us to be fault-tolerant and skip blocks in the case of java serialization failures
			TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
					out,
					Arrays.asList(
							Tuple2.of(
									broadcastStateMetaInfo.getKeySerializer(),
									broadcastStateMetaInfo.getKeySerializerConfigSnapshot()
							),
							Tuple2.of(
									broadcastStateMetaInfo.getValueSerializer(),
									broadcastStateMetaInfo.getValueSerializerConfigSnapshot()
							)
					)
			);
		}
	}

	// -------------------------------------------------------------------------------
	//  Readers
	//   - v1: Flink 1.2.x
	//   - v2: Flink 1.3.x
	//   - v3: Flink 1.5.x
	// -------------------------------------------------------------------------------

	public static <S> OperatorBackendStateMetaInfoReader<S> getOperatorStateReaderForVersion(
			int version, ClassLoader userCodeClassLoader) {

		switch (version) {
			case 1:
				return new OperatorBackendStateMetaInfoReaderV1<>(userCodeClassLoader);

			// version 2 and version 3 (current)
			case 2:
			case OperatorBackendSerializationProxy.VERSION:
				return new OperatorBackendStateMetaInfoReaderV2<>(userCodeClassLoader);

			default:
				// guard for future
				throw new IllegalStateException(
					"Unrecognized operator backend state meta info reader version: " + version);
		}
	}

	public static <K, V> BroadcastStateMetaInfoReader<K, V> getBroadcastStateReaderForVersion(
			int version, ClassLoader userCodeClassLoader) {

		switch (version) {
			// current version
			case OperatorBackendSerializationProxy.VERSION:
				return new BroadcastStateMetaInfoReaderV3<>(userCodeClassLoader);

			default:
				// guard for future
				throw new IllegalStateException(
						"Unrecognized broadcast state meta info reader version: " + version);
		}
	}

	public interface OperatorBackendStateMetaInfoReader<S> {
		RegisteredOperatorBackendStateMetaInfo.Snapshot<S> readOperatorStateMetaInfo(DataInputView in) throws IOException;
	}

	public interface BroadcastStateMetaInfoReader<K, V> {
		RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> readBroadcastStateMetaInfo(final DataInputView in) throws IOException;
	}

	public static abstract class AbstractOperatorBackendStateMetaInfoReader<S>
		implements OperatorBackendStateMetaInfoReader<S> {

		protected final ClassLoader userCodeClassLoader;

		public AbstractOperatorBackendStateMetaInfoReader(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		}
	}

	public abstract static class AbstractBroadcastStateMetaInfoReader<K, V>
			implements BroadcastStateMetaInfoReader<K, V> {

		protected final ClassLoader userCodeClassLoader;

		public AbstractBroadcastStateMetaInfoReader(final ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		}
	}

	public static class OperatorBackendStateMetaInfoReaderV1<S> extends AbstractOperatorBackendStateMetaInfoReader<S> {

		public OperatorBackendStateMetaInfoReaderV1(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@SuppressWarnings("unchecked")
		@Override
		public RegisteredOperatorBackendStateMetaInfo.Snapshot<S> readOperatorStateMetaInfo(DataInputView in) throws IOException {
			RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo =
				new RegisteredOperatorBackendStateMetaInfo.Snapshot<>();

			stateMetaInfo.setName(in.readUTF());
			stateMetaInfo.setAssignmentMode(OperatorStateHandle.Mode.values()[in.readByte()]);

			DataInputViewStream dis = new DataInputViewStream(in);
			ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
			try (
				InstantiationUtil.FailureTolerantObjectInputStream ois =
					new InstantiationUtil.FailureTolerantObjectInputStream(dis, userCodeClassLoader)) {

				Thread.currentThread().setContextClassLoader(userCodeClassLoader);
				TypeSerializer<S> stateSerializer = (TypeSerializer<S>) ois.readObject();
				stateMetaInfo.setPartitionStateSerializer(stateSerializer);
			} catch (ClassNotFoundException exception) {
				throw new IOException(exception);
			} finally {
				Thread.currentThread().setContextClassLoader(previousClassLoader);
			}

			// old versions do not contain the partition state serializer's configuration snapshot
			stateMetaInfo.setPartitionStateSerializerConfigSnapshot(null);

			return stateMetaInfo;
		}
	}

	@SuppressWarnings("unchecked")
	public static class OperatorBackendStateMetaInfoReaderV2<S> extends AbstractOperatorBackendStateMetaInfoReader<S> {

		public OperatorBackendStateMetaInfoReaderV2(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@Override
		public RegisteredOperatorBackendStateMetaInfo.Snapshot<S> readOperatorStateMetaInfo(DataInputView in) throws IOException {
			RegisteredOperatorBackendStateMetaInfo.Snapshot<S> stateMetaInfo =
				new RegisteredOperatorBackendStateMetaInfo.Snapshot<>();

			stateMetaInfo.setName(in.readUTF());
			stateMetaInfo.setAssignmentMode(OperatorStateHandle.Mode.values()[in.readByte()]);

			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> stateSerializerAndConfig =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader).get(0);

			stateMetaInfo.setPartitionStateSerializer((TypeSerializer<S>) stateSerializerAndConfig.f0);
			stateMetaInfo.setPartitionStateSerializerConfigSnapshot(stateSerializerAndConfig.f1);

			return stateMetaInfo;
		}
	}

	public static class BroadcastStateMetaInfoReaderV3<K, V> extends AbstractBroadcastStateMetaInfoReader<K, V> {

		public BroadcastStateMetaInfoReaderV3(final ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@Override
		public RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> readBroadcastStateMetaInfo(final DataInputView in) throws IOException {
			RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> stateMetaInfo =
					new RegisteredBroadcastBackendStateMetaInfo.Snapshot<>();

			stateMetaInfo.setName(in.readUTF());
			stateMetaInfo.setAssignmentMode(OperatorStateHandle.Mode.values()[in.readByte()]);

			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializers =
					TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader);

			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> keySerializerAndConfig = serializers.get(0);
			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> valueSerializerAndConfig = serializers.get(1);

			stateMetaInfo.setKeySerializer((TypeSerializer<K>) keySerializerAndConfig.f0);
			stateMetaInfo.setKeySerializerConfigSnapshot(keySerializerAndConfig.f1);
			
			stateMetaInfo.setValueSerializer((TypeSerializer<V>) valueSerializerAndConfig.f0);
			stateMetaInfo.setValueSerializerConfigSnapshot(valueSerializerAndConfig.f1);

			return stateMetaInfo;
		}
	}
}
