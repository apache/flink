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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.BackwardsCompatibleConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshotSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Readers and writers for different versions of the {@link InternalTimersSnapshot}.
 * Outdated formats are also kept here for documentation of history backlog.
 */
@Internal
public class InternalTimersSnapshotReaderWriters {

	public static final int NO_VERSION = Integer.MIN_VALUE;

	// -------------------------------------------------------------------------------
	//  Writers
	//   - pre-versioned: Flink 1.4.0
	//   - v1: Flink 1.4.1, 1.5.x
	//   - v2: Flink 1.6.x
	// -------------------------------------------------------------------------------

	public static <K, N> InternalTimersSnapshotWriter getWriterForVersion(int version, InternalTimersSnapshot<K, N> timersSnapshot) {

		switch (version) {
			case NO_VERSION:
				return new InternalTimersSnapshotWriterPreVersioned<>(timersSnapshot);

			case 1:
				return new InternalTimersSnapshotWriterV1<>(timersSnapshot);

			// current version:
			case InternalTimerServiceSerializationProxy.VERSION:
				return new InternalTimersSnapshotWriterV2<>(timersSnapshot);

			default:
				// guard for future
				throw new IllegalStateException(
					"Unrecognized internal timers snapshot writer version: " + version);
		}
	}

	/**
	 * A writer for a {@link InternalTimersSnapshot}.
	 */
	public interface InternalTimersSnapshotWriter {

		/**
		 * Writes the timers snapshot to the output view.
		 *
		 * @param out the output view to write to
		 * @throws IOException
		 */
		void writeTimersSnapshot(DataOutputView out) throws IOException;
	}

	private abstract static class AbstractInternalTimersSnapshotWriter<K, N> implements InternalTimersSnapshotWriter {

		protected final InternalTimersSnapshot<K, N> timersSnapshot;

		public AbstractInternalTimersSnapshotWriter(InternalTimersSnapshot<K, N> timersSnapshot) {
			this.timersSnapshot = checkNotNull(timersSnapshot);
		}

		protected abstract void writeKeyAndNamespaceSerializers(DataOutputView out) throws IOException;

		@Override
		public final void writeTimersSnapshot(DataOutputView out) throws IOException {
			writeKeyAndNamespaceSerializers(out);

			TimerHeapInternalTimer.TimerSerializer<K, N> timerSerializer = timersSnapshot.createTimerSerializer();

			// write the event time timers
			Set<InternalTimer<K, N>> eventTimers = timersSnapshot.getEventTimeTimers();
			if (eventTimers != null) {
				out.writeInt(eventTimers.size());
				for (InternalTimer<K, N> eventTimer : eventTimers) {
					timerSerializer.serialize(eventTimer, out);
				}
			} else {
				out.writeInt(0);
			}

			// write the processing time timers
			Set<InternalTimer<K, N>> processingTimers = timersSnapshot.getProcessingTimeTimers();
			if (processingTimers != null) {
				out.writeInt(processingTimers.size());
				for (InternalTimer<K, N> processingTimer : processingTimers) {
					timerSerializer.serialize(processingTimer, out);
				}
			} else {
				out.writeInt(0);
			}
		}
	}

	private static class InternalTimersSnapshotWriterPreVersioned<K, N> extends AbstractInternalTimersSnapshotWriter<K, N> {

		public InternalTimersSnapshotWriterPreVersioned(InternalTimersSnapshot<K, N> timersSnapshot) {
			super(timersSnapshot);
		}

		@Override
		protected void writeKeyAndNamespaceSerializers(DataOutputView out) throws IOException {
			// the pre-versioned format only serializes the serializers, without their configuration snapshots
			try (ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos()) {
				// state meta info snapshots no longer contain serializers, so we use null just as a placeholder;
				// this is maintained here to keep track of previous versions' serialization formats
				InstantiationUtil.serializeObject(stream, null);
				InstantiationUtil.serializeObject(stream, null);

				out.write(stream.getBuf(), 0, stream.getPosition());
			}
		}
	}

	private static class InternalTimersSnapshotWriterV1<K, N> extends AbstractInternalTimersSnapshotWriter<K, N> {

		public InternalTimersSnapshotWriterV1(InternalTimersSnapshot<K, N> timersSnapshot) {
			super(timersSnapshot);
		}

		@Override
		protected void writeKeyAndNamespaceSerializers(DataOutputView out) throws IOException {
			// write key / namespace serializers, and their configuration snapshots
			TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
				out,
				// state meta info snapshots no longer contain serializers, so we use null just as a placeholder;
				// this is maintained here to keep track of previous versions' serialization formats
				Arrays.asList(
					Tuple2.of(null, timersSnapshot.getKeySerializerConfigSnapshot()),
					Tuple2.of(null, timersSnapshot.getNamespaceSerializerConfigSnapshot())));
		}
	}

	private static class InternalTimersSnapshotWriterV2<K, N> extends AbstractInternalTimersSnapshotWriter<K, N> {

		public InternalTimersSnapshotWriterV2(InternalTimersSnapshot<K, N> timersSnapshot) {
			super(timersSnapshot);
		}

		@Override
		protected void writeKeyAndNamespaceSerializers(DataOutputView out) throws IOException {
			TypeSerializerConfigSnapshotSerializationUtil.writeSerializerConfigSnapshot(
				out, timersSnapshot.getKeySerializerConfigSnapshot());
			TypeSerializerConfigSnapshotSerializationUtil.writeSerializerConfigSnapshot(
				out, timersSnapshot.getNamespaceSerializerConfigSnapshot());
		}
	}

	// -------------------------------------------------------------------------------
	//  Readers
	//   - pre-versioned: Flink 1.4.0
	//   - v1: Flink 1.4.1, 1.5.x
	//   - v2: Flink 1.6.x
	// -------------------------------------------------------------------------------

	public static <K, N> InternalTimersSnapshotReader<K, N> getReaderForVersion(
		int version, ClassLoader userCodeClassLoader) {

		switch (version) {
			case NO_VERSION:
				return new InternalTimersSnapshotReaderPreVersioned<>(userCodeClassLoader);

			case 1:
				return new InternalTimersSnapshotReaderV1<>(userCodeClassLoader);

			// current version
			case InternalTimerServiceSerializationProxy.VERSION:
				return new InternalTimersSnapshotReaderV2<>(userCodeClassLoader);

			default:
				// guard for future
				throw new IllegalStateException(
					"Unrecognized internal timers snapshot writer version: " + version);
		}
	}

	/**
	 * A reader for a {@link InternalTimersSnapshot}.
	 */
	public interface InternalTimersSnapshotReader<K, N> {

		/**
		 * Reads a timers snapshot from the provided input view.
		 *
		 * @param in the input view
		 * @return the read timers snapshot
		 * @throws IOException
		 */
		InternalTimersSnapshot<K, N> readTimersSnapshot(DataInputView in) throws IOException;
	}

	private abstract static class AbstractInternalTimersSnapshotReader<K, N> implements InternalTimersSnapshotReader<K, N> {

		protected final ClassLoader userCodeClassLoader;

		public AbstractInternalTimersSnapshotReader(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		}

		protected abstract void restoreKeyAndNamespaceSerializers(
				InternalTimersSnapshot<K, N> restoredTimersSnapshot,
				DataInputView in) throws IOException;

		@Override
		public final InternalTimersSnapshot<K, N> readTimersSnapshot(DataInputView in) throws IOException {
			InternalTimersSnapshot<K, N> restoredTimersSnapshot = new InternalTimersSnapshot<>();

			restoreKeyAndNamespaceSerializers(restoredTimersSnapshot, in);

			TimerHeapInternalTimer.TimerSerializer<K, N> timerSerializer = restoredTimersSnapshot.createTimerSerializer();

			// read the event time timers
			int sizeOfEventTimeTimers = in.readInt();
			Set<InternalTimer<K, N>> restoredEventTimers = new HashSet<>(sizeOfEventTimeTimers);
			if (sizeOfEventTimeTimers > 0) {
				for (int i = 0; i < sizeOfEventTimeTimers; i++) {
					InternalTimer<K, N> timer = timerSerializer.deserialize(in);
					restoredEventTimers.add(timer);
				}
			}
			restoredTimersSnapshot.setEventTimeTimers(restoredEventTimers);

			// read the processing time timers
			int sizeOfProcessingTimeTimers = in.readInt();
			Set<InternalTimer<K, N>> restoredProcessingTimers = new HashSet<>(sizeOfProcessingTimeTimers);
			if (sizeOfProcessingTimeTimers > 0) {
				for (int i = 0; i < sizeOfProcessingTimeTimers; i++) {
					InternalTimer<K, N> timer = timerSerializer.deserialize(in);
					restoredProcessingTimers.add(timer);
				}
			}
			restoredTimersSnapshot.setProcessingTimeTimers(restoredProcessingTimers);

			return restoredTimersSnapshot;
		}
	}

	private static class InternalTimersSnapshotReaderPreVersioned<K, N> extends AbstractInternalTimersSnapshotReader<K, N> {

		public InternalTimersSnapshotReaderPreVersioned(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void restoreKeyAndNamespaceSerializers(
				InternalTimersSnapshot<K, N> restoredTimersSnapshot,
				DataInputView in) throws IOException {

			DataInputViewStream dis = new DataInputViewStream(in);
			try {
				// Flink 1.4.0 and below did not write the serializer config snapshots for timers;
				// we deserialize the written serializers, and then simply take a snapshot of them now

				TypeSerializer<K> keySerializer = InstantiationUtil.deserializeObject(dis, userCodeClassLoader, true);
				restoredTimersSnapshot.setKeySerializerConfigSnapshot(keySerializer.snapshotConfiguration());

				TypeSerializer<N> namespaceSerializer = InstantiationUtil.deserializeObject(dis, userCodeClassLoader, true);
				restoredTimersSnapshot.setNamespaceSerializerConfigSnapshot(namespaceSerializer.snapshotConfiguration());
			} catch (ClassNotFoundException exception) {
				throw new IOException(exception);
			}
		}
	}

	private static class InternalTimersSnapshotReaderV1<K, N> extends AbstractInternalTimersSnapshotReader<K, N> {

		public InternalTimersSnapshotReaderV1(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void restoreKeyAndNamespaceSerializers(
				InternalTimersSnapshot<K, N> restoredTimersSnapshot,
				DataInputView in) throws IOException {

			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader);

			restoredTimersSnapshot.setKeySerializerConfigSnapshot(
				new BackwardsCompatibleConfigSnapshot(serializersAndConfigs.get(0).f1, serializersAndConfigs.get(0).f0));

			restoredTimersSnapshot.setNamespaceSerializerConfigSnapshot(
				new BackwardsCompatibleConfigSnapshot(serializersAndConfigs.get(1).f1, serializersAndConfigs.get(1).f0));
		}
	}

	private static class InternalTimersSnapshotReaderV2<K, N> extends AbstractInternalTimersSnapshotReader<K, N> {

		public InternalTimersSnapshotReaderV2(ClassLoader userCodeClassLoader) {
			super(userCodeClassLoader);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void restoreKeyAndNamespaceSerializers(
				InternalTimersSnapshot<K, N> restoredTimersSnapshot,
				DataInputView in) throws IOException {

			restoredTimersSnapshot.setKeySerializerConfigSnapshot(
				TypeSerializerConfigSnapshotSerializationUtil.readSerializerConfigSnapshot(in, userCodeClassLoader));

			restoredTimersSnapshot.setNamespaceSerializerConfigSnapshot(
				TypeSerializerConfigSnapshotSerializationUtil.readSerializerConfigSnapshot(in, userCodeClassLoader));
		}
	}
}
