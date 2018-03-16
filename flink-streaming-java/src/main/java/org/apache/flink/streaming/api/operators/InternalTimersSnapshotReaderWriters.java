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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
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
	//   - v1: Flink 1.4.1
	// -------------------------------------------------------------------------------

	public static <K, N> InternalTimersSnapshotWriter getWriterForVersion(int version, InternalTimersSnapshot<K, N> timersSnapshot) {

		switch (version) {
			case NO_VERSION:
				return new InternalTimersSnapshotWriterPreVersioned<>(timersSnapshot);

			case InternalTimerServiceSerializationProxy.VERSION:
				return new InternalTimersSnapshotWriterV1<>(timersSnapshot);

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

			InternalTimer.TimerSerializer<K, N> timerSerializer = new InternalTimer.TimerSerializer<>(
				timersSnapshot.getKeySerializer(),
				timersSnapshot.getNamespaceSerializer());

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
				InstantiationUtil.serializeObject(stream, timersSnapshot.getKeySerializer());
				InstantiationUtil.serializeObject(stream, timersSnapshot.getNamespaceSerializer());

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
				Arrays.asList(
					Tuple2.of(timersSnapshot.getKeySerializer(), timersSnapshot.getKeySerializerConfigSnapshot()),
					Tuple2.of(timersSnapshot.getNamespaceSerializer(), timersSnapshot.getNamespaceSerializerConfigSnapshot())));
		}
	}

	// -------------------------------------------------------------------------------
	//  Readers
	//   - pre-versioned: Flink 1.4.0
	//   - v1: Flink 1.4.1
	// -------------------------------------------------------------------------------

	public static <K, N> InternalTimersSnapshotReader<K, N> getReaderForVersion(
		int version, ClassLoader userCodeClassLoader) {

		switch (version) {
			case NO_VERSION:
				return new InternalTimersSnapshotReaderPreVersioned<>(userCodeClassLoader);

			case InternalTimerServiceSerializationProxy.VERSION:
				return new InternalTimersSnapshotReaderV1<>(userCodeClassLoader);

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

			InternalTimer.TimerSerializer<K, N> timerSerializer = new InternalTimer.TimerSerializer<>(
				restoredTimersSnapshot.getKeySerializer(),
				restoredTimersSnapshot.getNamespaceSerializer());

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
				restoredTimersSnapshot.setKeySerializer(InstantiationUtil.deserializeObject(dis, userCodeClassLoader, true));
				restoredTimersSnapshot.setNamespaceSerializer(InstantiationUtil.deserializeObject(dis, userCodeClassLoader, true));
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

			restoredTimersSnapshot.setKeySerializer((TypeSerializer<K>) serializersAndConfigs.get(0).f0);
			restoredTimersSnapshot.setKeySerializerConfigSnapshot(serializersAndConfigs.get(0).f1);
			restoredTimersSnapshot.setNamespaceSerializer((TypeSerializer<N>) serializersAndConfigs.get(1).f0);
			restoredTimersSnapshot.setNamespaceSerializerConfigSnapshot(serializersAndConfigs.get(1).f1);
		}
	}
}
