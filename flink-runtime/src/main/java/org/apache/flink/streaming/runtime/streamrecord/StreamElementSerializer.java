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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.watermark.InternalWatermarkDeclaration;
import org.apache.flink.streaming.api.watermark.InternalWatermark;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Serializer for {@link StreamRecord}, {@link Watermark}, {@link LatencyMarker}, and {@link
 * WatermarkStatus}.
 *
 * <p>This does not behave like a normal {@link TypeSerializer}, instead, this is only used at the
 * stream task/operator level for transmitting StreamRecords and Watermarks.
 *
 * @param <T> The type of value in the StreamRecord
 */
@Internal
public final class StreamElementSerializer<T> extends TypeSerializer<StreamElement> {

    private static final long serialVersionUID = 1L;

    private static final int TAG_REC_WITH_TIMESTAMP = 0;
    private static final int TAG_REC_WITHOUT_TIMESTAMP = 1;
    private static final int TAG_WATERMARK = 2;
    private static final int TAG_LATENCY_MARKER = 3;
    private static final int TAG_STREAM_STATUS = 4;
    private static final int TAG_RECORD_ATTRIBUTES = 5;
    private static final int TAG_INTERNAL_WATERMARK = 6;
    private static final int TAG_GENERALIZED_WATERMARK = 7;

    private final TypeSerializer<T> typeSerializer;

    private final Map<String, InternalWatermarkDeclaration.WatermarkSerde> watermarkDeclarationMap;

    public StreamElementSerializer(TypeSerializer<T> serializer) {
        this(serializer, new ArrayList<>());
    }

    public StreamElementSerializer(
            TypeSerializer<T> serializer,
            List<InternalWatermarkDeclaration.WatermarkSerde> watermarkSerdes) {
        if (serializer instanceof StreamElementSerializer) {
            throw new RuntimeException(
                    "StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: "
                            + serializer);
        }
        this.typeSerializer = requireNonNull(serializer);
        this.watermarkDeclarationMap = new HashMap<>();

        for (InternalWatermarkDeclaration.WatermarkSerde genericWatermarkDeclaration :
                watermarkSerdes) {
            this.watermarkDeclarationMap.put(
                    genericWatermarkDeclaration.watermarkClass().getName(),
                    genericWatermarkDeclaration);
        }
    }

    public TypeSerializer<T> getContainedTypeSerializer() {
        return this.typeSerializer;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public StreamElementSerializer<T> duplicate() {
        TypeSerializer<T> copy = typeSerializer.duplicate();
        return (copy == typeSerializer) ? this : new StreamElementSerializer<T>(copy);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public StreamRecord<T> createInstance() {
        return new StreamRecord<T>(typeSerializer.createInstance());
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public StreamElement copy(StreamElement from) {
        // we can reuse the timestamp since Instant is immutable
        if (from.isRecord()) {
            StreamRecord<T> fromRecord = from.asRecord();
            return fromRecord.copy(typeSerializer.copy(fromRecord.getValue()));
        } else if (from.isWatermark() || from.isWatermarkStatus() || from.isLatencyMarker()) {
            // is immutable
            return from;
        } else {
            throw new RuntimeException();
        }
    }

    @Override
    public StreamElement copy(StreamElement from, StreamElement reuse) {
        if (from.isRecord() && reuse.isRecord()) {
            StreamRecord<T> fromRecord = from.asRecord();
            StreamRecord<T> reuseRecord = reuse.asRecord();

            T valueCopy = typeSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
            fromRecord.copyTo(valueCopy, reuseRecord);
            return reuse;
        } else if (from.isWatermark() || from.isWatermarkStatus() || from.isLatencyMarker()) {
            // is immutable
            return from;
        } else {
            throw new RuntimeException("Cannot copy " + from + " -> " + reuse);
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int tag = source.readByte();
        target.write(tag);

        if (tag == TAG_REC_WITH_TIMESTAMP) {
            // move timestamp
            target.writeLong(source.readLong());
            typeSerializer.copy(source, target);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            typeSerializer.copy(source, target);
        } else if (tag == TAG_WATERMARK) {
            target.writeLong(source.readLong());
        } else if (tag == TAG_INTERNAL_WATERMARK) {
            target.writeInt(source.readInt());
            target.writeLong(source.readLong());
        } else if (tag == TAG_STREAM_STATUS) {
            target.writeInt(source.readInt());
        } else if (tag == TAG_LATENCY_MARKER) {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        } else if (tag == TAG_RECORD_ATTRIBUTES) {
            target.writeBoolean(source.readBoolean());
        } else if (tag == TAG_GENERALIZED_WATERMARK) {
            String watermarkClazz = source.readUTF();
            InternalWatermarkDeclaration.WatermarkSerde watermarkSerde =
                    watermarkDeclarationMap.get(watermarkClazz);
            GeneralizedWatermark watermark = watermarkSerde.deserialize(source);
            target.writeUTF(watermarkClazz);
            watermarkSerde.serialize(watermark, target);
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public void serialize(StreamElement value, DataOutputView target) throws IOException {
        if (value.isRecord()) {
            StreamRecord<T> record = value.asRecord();

            if (record.hasTimestamp()) {
                target.write(TAG_REC_WITH_TIMESTAMP);
                target.writeLong(record.getTimestamp());
            } else {
                target.write(TAG_REC_WITHOUT_TIMESTAMP);
            }
            typeSerializer.serialize(record.getValue(), target);
        } else if (value.isWatermark()) {
            if (value instanceof InternalWatermark) {
                target.write(TAG_INTERNAL_WATERMARK);
                target.writeInt(((InternalWatermark) value).getSubpartitionIndex());
            } else {
                target.write(TAG_WATERMARK);
            }
            target.writeLong(value.asWatermark().getTimestamp());
        } else if (value.isWatermarkStatus()) {
            target.write(TAG_STREAM_STATUS);
            target.writeInt(value.asWatermarkStatus().getStatus());
        } else if (value.isLatencyMarker()) {
            target.write(TAG_LATENCY_MARKER);
            target.writeLong(value.asLatencyMarker().getMarkedTime());
            target.writeLong(value.asLatencyMarker().getOperatorId().getLowerPart());
            target.writeLong(value.asLatencyMarker().getOperatorId().getUpperPart());
            target.writeInt(value.asLatencyMarker().getSubtaskIndex());
        } else if (value.isRecordAttributes()) {
            target.write(TAG_RECORD_ATTRIBUTES);
            target.writeBoolean(value.asRecordAttributes().isBacklog());
        } else if (value.isGeneralizedWatermark()) {
            target.write(TAG_GENERALIZED_WATERMARK);
            GeneralizedWatermark genericWatermark = value.asGeneralizedWatermark().getWatermark();
            String identifier = genericWatermark.getClass().getName();
            target.writeUTF(identifier);
            watermarkDeclarationMap.get(identifier).serialize(genericWatermark, target);
            // TODOJEY
        } else {
            throw new RuntimeException();
        }
    }

    @Override
    public StreamElement deserialize(DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            return new StreamRecord<T>(typeSerializer.deserialize(source), timestamp);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            return new StreamRecord<T>(typeSerializer.deserialize(source));
        } else if (tag == TAG_WATERMARK) {
            return new Watermark(source.readLong());
        } else if (tag == TAG_INTERNAL_WATERMARK) {
            int subpartitionIndex = source.readInt();
            return new InternalWatermark(source.readLong(), subpartitionIndex);
        } else if (tag == TAG_STREAM_STATUS) {
            return new WatermarkStatus(source.readInt());
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else if (tag == TAG_RECORD_ATTRIBUTES) {
            return new RecordAttributes(source.readBoolean());
        } else if (tag == TAG_GENERALIZED_WATERMARK) {
            String identifier = source.readUTF();
            GeneralizedWatermark watermark =
                    watermarkDeclarationMap.get(identifier).deserialize(source);
            return new GeneralizedWatermarkEvent(watermark);
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public StreamElement deserialize(StreamElement reuse, DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            T value = typeSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = reuse.asRecord();
            reuseRecord.replace(value, timestamp);
            return reuseRecord;
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            T value = typeSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = reuse.asRecord();
            reuseRecord.replace(value);
            return reuseRecord;
        } else if (tag == TAG_WATERMARK) {
            return new Watermark(source.readLong());
        } else if (tag == TAG_INTERNAL_WATERMARK) {
            int subpartitionIndex = source.readInt();
            return new InternalWatermark(source.readLong(), subpartitionIndex);
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else if (tag == TAG_RECORD_ATTRIBUTES) {
            return new RecordAttributes(source.readBoolean());
        } else if (tag == TAG_GENERALIZED_WATERMARK) {
            // TODOJEY
            throw new IOException("CCCCC");
            //            return new Watermark(source.readLong());
            //        } else if (tag == TAG_INTERNAL_WATERMARK) {
            //            int subpartitionIndex = source.readInt();
            //            return new InternalWatermark(source.readLong(), subpartitionIndex);
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StreamElementSerializer) {
            StreamElementSerializer<?> other = (StreamElementSerializer<?>) obj;

            return typeSerializer.equals(other.typeSerializer);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return typeSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    //
    // This serializer may be used by Flink internal operators that need to checkpoint
    // buffered records. Therefore, it may be part of managed state and need to implement
    // the configuration snapshot and compatibility methods.
    // --------------------------------------------------------------------------------------------

    @Override
    public StreamElementSerializerSnapshot<T> snapshotConfiguration() {
        return new StreamElementSerializerSnapshot<>(this);
    }

    /** Configuration snapshot specific to the {@link StreamElementSerializer}. */
    public static final class StreamElementSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<StreamElement, StreamElementSerializer<T>> {

        private static final int VERSION = 2;

        @SuppressWarnings("WeakerAccess")
        public StreamElementSerializerSnapshot() {}

        StreamElementSerializerSnapshot(StreamElementSerializer<T> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                StreamElementSerializer<T> outerSerializer) {
            return new TypeSerializer[] {outerSerializer.getContainedTypeSerializer()};
        }

        @Override
        protected StreamElementSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> casted = (TypeSerializer<T>) nestedSerializers[0];

            return new StreamElementSerializer<>(casted);
        }
    }
}
