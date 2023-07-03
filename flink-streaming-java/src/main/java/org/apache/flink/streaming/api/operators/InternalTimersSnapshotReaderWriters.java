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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CollectionUtil;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Readers and writers for different versions of the {@link InternalTimersSnapshot}. Outdated
 * formats are also kept here for documentation of history backlog.
 */
@Internal
public class InternalTimersSnapshotReaderWriters {

    public static final int NO_VERSION = Integer.MIN_VALUE;

    // -------------------------------------------------------------------------------
    //  Writers
    //   - pre-versioned: Flink 1.4.0
    //   - v1: Flink 1.4.1
    //   - v2: Flink 1.8.0
    // -------------------------------------------------------------------------------

    public static <K, N> InternalTimersSnapshotWriter getWriterForVersion(
            int version,
            InternalTimersSnapshot<K, N> timersSnapshot,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer) {

        switch (version) {
            case NO_VERSION:
            case 1:
                throw new IllegalStateException(
                        "Since Flink 1.17 not versioned (<= Flink 1.4.0) and version 1 (< Flink 1.8.0) of "
                                + "InternalTimersSnapshotWriter is no longer supported.");

            case InternalTimerServiceSerializationProxy.VERSION:
                return new InternalTimersSnapshotWriterV2<>(
                        timersSnapshot, keySerializer, namespaceSerializer);

            default:
                // guard for future
                throw new IllegalStateException(
                        "Unrecognized internal timers snapshot writer version: " + version);
        }
    }

    /** A writer for a {@link InternalTimersSnapshot}. */
    public interface InternalTimersSnapshotWriter {

        /**
         * Writes the timers snapshot to the output view.
         *
         * @param out the output view to write to
         * @throws IOException
         */
        void writeTimersSnapshot(DataOutputView out) throws IOException;
    }

    private abstract static class AbstractInternalTimersSnapshotWriter<K, N>
            implements InternalTimersSnapshotWriter {

        protected final InternalTimersSnapshot<K, N> timersSnapshot;

        protected final TypeSerializer<K> keySerializer;
        protected final TypeSerializer<N> namespaceSerializer;

        public AbstractInternalTimersSnapshotWriter(
                InternalTimersSnapshot<K, N> timersSnapshot,
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer) {
            this.timersSnapshot = checkNotNull(timersSnapshot);
            this.keySerializer = checkNotNull(keySerializer);
            this.namespaceSerializer = checkNotNull(namespaceSerializer);
        }

        protected abstract void writeKeyAndNamespaceSerializers(DataOutputView out)
                throws IOException;

        @Override
        public final void writeTimersSnapshot(DataOutputView out) throws IOException {
            writeKeyAndNamespaceSerializers(out);

            LegacyTimerSerializer<K, N> timerSerializer =
                    new LegacyTimerSerializer<>(keySerializer, namespaceSerializer);

            // write the event time timers
            Set<TimerHeapInternalTimer<K, N>> eventTimers = timersSnapshot.getEventTimeTimers();
            if (eventTimers != null) {
                out.writeInt(eventTimers.size());
                for (TimerHeapInternalTimer<K, N> eventTimer : eventTimers) {
                    timerSerializer.serialize(eventTimer, out);
                }
            } else {
                out.writeInt(0);
            }

            // write the processing time timers
            Set<TimerHeapInternalTimer<K, N>> processingTimers =
                    timersSnapshot.getProcessingTimeTimers();
            if (processingTimers != null) {
                out.writeInt(processingTimers.size());
                for (TimerHeapInternalTimer<K, N> processingTimer : processingTimers) {
                    timerSerializer.serialize(processingTimer, out);
                }
            } else {
                out.writeInt(0);
            }
        }
    }

    private static class InternalTimersSnapshotWriterV2<K, N>
            extends AbstractInternalTimersSnapshotWriter<K, N> {

        public InternalTimersSnapshotWriterV2(
                InternalTimersSnapshot<K, N> timersSnapshot,
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer) {
            super(timersSnapshot, keySerializer, namespaceSerializer);
        }

        @Override
        protected void writeKeyAndNamespaceSerializers(DataOutputView out) throws IOException {
            TypeSerializerSnapshot.writeVersionedSnapshot(
                    out, timersSnapshot.getKeySerializerSnapshot());
            TypeSerializerSnapshot.writeVersionedSnapshot(
                    out, timersSnapshot.getNamespaceSerializerSnapshot());
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
            case 1:
                throw new UnsupportedOperationException(
                        "Since Flink 1.17 not versioned (<= Flink 1.4.0) and version 1 (< Flink 1.8.0) of "
                                + "InternalTimersSnapshotReader is no longer supported.");

            case InternalTimerServiceSerializationProxy.VERSION:
                return new InternalTimersSnapshotReaderV2<>(userCodeClassLoader);

            default:
                // guard for future
                throw new IllegalStateException(
                        "Unrecognized internal timers snapshot writer version: " + version);
        }
    }

    /** A reader for a {@link InternalTimersSnapshot}. */
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

    private abstract static class AbstractInternalTimersSnapshotReader<K, N>
            implements InternalTimersSnapshotReader<K, N> {

        protected final ClassLoader userCodeClassLoader;

        public AbstractInternalTimersSnapshotReader(ClassLoader userCodeClassLoader) {
            this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
        }

        protected abstract void restoreKeyAndNamespaceSerializers(
                InternalTimersSnapshot<K, N> restoredTimersSnapshot, DataInputView in)
                throws IOException;

        @Override
        public final InternalTimersSnapshot<K, N> readTimersSnapshot(DataInputView in)
                throws IOException {
            InternalTimersSnapshot<K, N> restoredTimersSnapshot = new InternalTimersSnapshot<>();

            restoreKeyAndNamespaceSerializers(restoredTimersSnapshot, in);

            LegacyTimerSerializer<K, N> timerSerializer =
                    new LegacyTimerSerializer<>(
                            restoredTimersSnapshot.getKeySerializerSnapshot().restoreSerializer(),
                            restoredTimersSnapshot
                                    .getNamespaceSerializerSnapshot()
                                    .restoreSerializer());

            // read the event time timers
            int sizeOfEventTimeTimers = in.readInt();
            Set<TimerHeapInternalTimer<K, N>> restoredEventTimers =
                    CollectionUtil.newHashSetWithExpectedSize(sizeOfEventTimeTimers);
            if (sizeOfEventTimeTimers > 0) {
                for (int i = 0; i < sizeOfEventTimeTimers; i++) {
                    TimerHeapInternalTimer<K, N> timer = timerSerializer.deserialize(in);
                    restoredEventTimers.add(timer);
                }
            }
            restoredTimersSnapshot.setEventTimeTimers(restoredEventTimers);

            // read the processing time timers
            int sizeOfProcessingTimeTimers = in.readInt();
            Set<TimerHeapInternalTimer<K, N>> restoredProcessingTimers =
                    CollectionUtil.newHashSetWithExpectedSize(sizeOfProcessingTimeTimers);
            if (sizeOfProcessingTimeTimers > 0) {
                for (int i = 0; i < sizeOfProcessingTimeTimers; i++) {
                    TimerHeapInternalTimer<K, N> timer = timerSerializer.deserialize(in);
                    restoredProcessingTimers.add(timer);
                }
            }
            restoredTimersSnapshot.setProcessingTimeTimers(restoredProcessingTimers);

            return restoredTimersSnapshot;
        }
    }

    private static class InternalTimersSnapshotReaderV2<K, N>
            extends AbstractInternalTimersSnapshotReader<K, N> {

        public InternalTimersSnapshotReaderV2(ClassLoader userCodeClassLoader) {
            super(userCodeClassLoader);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void restoreKeyAndNamespaceSerializers(
                InternalTimersSnapshot<K, N> restoredTimersSnapshot, DataInputView in)
                throws IOException {

            restoredTimersSnapshot.setKeySerializerSnapshot(
                    TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader));
            restoredTimersSnapshot.setNamespaceSerializerSnapshot(
                    TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader));
        }
    }

    /** A {@link TypeSerializer} used to serialize/deserialize a {@link TimerHeapInternalTimer}. */
    public static class LegacyTimerSerializer<K, N>
            extends TypeSerializer<TimerHeapInternalTimer<K, N>> {

        private static final long serialVersionUID = 1119562170939152304L;

        @Nonnull private final TypeSerializer<K> keySerializer;

        @Nonnull private final TypeSerializer<N> namespaceSerializer;

        LegacyTimerSerializer(
                @Nonnull TypeSerializer<K> keySerializer,
                @Nonnull TypeSerializer<N> namespaceSerializer) {
            this.keySerializer = keySerializer;
            this.namespaceSerializer = namespaceSerializer;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TimerHeapInternalTimer<K, N>> duplicate() {

            final TypeSerializer<K> keySerializerDuplicate = keySerializer.duplicate();
            final TypeSerializer<N> namespaceSerializerDuplicate = namespaceSerializer.duplicate();

            if (keySerializerDuplicate == keySerializer
                    && namespaceSerializerDuplicate == namespaceSerializer) {
                // all delegate serializers seem stateless, so this is also stateless.
                return this;
            } else {
                // at least one delegate serializer seems to be stateful, so we return a new
                // instance.
                return new LegacyTimerSerializer<>(
                        keySerializerDuplicate, namespaceSerializerDuplicate);
            }
        }

        @Override
        public TimerHeapInternalTimer<K, N> createInstance() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TimerHeapInternalTimer<K, N> copy(TimerHeapInternalTimer<K, N> from) {
            return new TimerHeapInternalTimer<>(
                    from.getTimestamp(), from.getKey(), from.getNamespace());
        }

        @Override
        public TimerHeapInternalTimer<K, N> copy(
                TimerHeapInternalTimer<K, N> from, TimerHeapInternalTimer<K, N> reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            // we do not have fixed length
            return -1;
        }

        @Override
        public void serialize(TimerHeapInternalTimer<K, N> record, DataOutputView target)
                throws IOException {
            keySerializer.serialize(record.getKey(), target);
            namespaceSerializer.serialize(record.getNamespace(), target);
            LongSerializer.INSTANCE.serialize(record.getTimestamp(), target);
        }

        @Override
        public TimerHeapInternalTimer<K, N> deserialize(DataInputView source) throws IOException {
            K key = keySerializer.deserialize(source);
            N namespace = namespaceSerializer.deserialize(source);
            Long timestamp = LongSerializer.INSTANCE.deserialize(source);
            return new TimerHeapInternalTimer<>(timestamp, key, namespace);
        }

        @Override
        public TimerHeapInternalTimer<K, N> deserialize(
                TimerHeapInternalTimer<K, N> reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            keySerializer.copy(source, target);
            namespaceSerializer.copy(source, target);
            LongSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || (obj != null
                            && obj.getClass() == getClass()
                            && keySerializer.equals(((LegacyTimerSerializer) obj).keySerializer)
                            && namespaceSerializer.equals(
                                    ((LegacyTimerSerializer) obj).namespaceSerializer));
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public TypeSerializerSnapshot<TimerHeapInternalTimer<K, N>> snapshotConfiguration() {
            throw new UnsupportedOperationException(
                    "This serializer is not registered for managed state.");
        }
    }
}
