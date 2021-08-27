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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.getDataType;

/** Utility class to serialize and deserialize task events. */
public class EventSerializer {

    // ------------------------------------------------------------------------
    //  Constants
    // ------------------------------------------------------------------------

    private static final int END_OF_PARTITION_EVENT = 0;

    private static final int CHECKPOINT_BARRIER_EVENT = 1;

    private static final int END_OF_SUPERSTEP_EVENT = 2;

    private static final int OTHER_EVENT = 3;

    private static final int CANCEL_CHECKPOINT_MARKER_EVENT = 4;

    private static final int END_OF_CHANNEL_STATE_EVENT = 5;

    private static final int ANNOUNCEMENT_EVENT = 6;

    private static final int VIRTUAL_CHANNEL_SELECTOR_EVENT = 7;

    private static final int END_OF_USER_RECORDS_EVENT = 8;

    private static final int CHECKPOINT_TYPE_CHECKPOINT = 0;

    private static final int CHECKPOINT_TYPE_SAVEPOINT = 1;

    private static final int CHECKPOINT_TYPE_SAVEPOINT_SUSPEND = 2;

    private static final int CHECKPOINT_TYPE_SAVEPOINT_TERMINATE = 3;

    // ------------------------------------------------------------------------
    //  Serialization Logic
    // ------------------------------------------------------------------------

    public static ByteBuffer toSerializedEvent(AbstractEvent event) throws IOException {
        final Class<?> eventClass = event.getClass();
        if (eventClass == EndOfPartitionEvent.class) {
            return ByteBuffer.wrap(new byte[] {0, 0, 0, END_OF_PARTITION_EVENT});
        } else if (eventClass == CheckpointBarrier.class) {
            return serializeCheckpointBarrier((CheckpointBarrier) event);
        } else if (eventClass == EndOfSuperstepEvent.class) {
            return ByteBuffer.wrap(new byte[] {0, 0, 0, END_OF_SUPERSTEP_EVENT});
        } else if (eventClass == EndOfChannelStateEvent.class) {
            return ByteBuffer.wrap(new byte[] {0, 0, 0, END_OF_CHANNEL_STATE_EVENT});
        } else if (eventClass == EndOfData.class) {
            return ByteBuffer.wrap(new byte[] {0, 0, 0, END_OF_USER_RECORDS_EVENT});
        } else if (eventClass == CancelCheckpointMarker.class) {
            CancelCheckpointMarker marker = (CancelCheckpointMarker) event;

            ByteBuffer buf = ByteBuffer.allocate(12);
            buf.putInt(0, CANCEL_CHECKPOINT_MARKER_EVENT);
            buf.putLong(4, marker.getCheckpointId());
            return buf;
        } else if (eventClass == EventAnnouncement.class) {
            EventAnnouncement announcement = (EventAnnouncement) event;
            ByteBuffer serializedAnnouncedEvent =
                    toSerializedEvent(announcement.getAnnouncedEvent());
            ByteBuffer serializedAnnouncement =
                    ByteBuffer.allocate(2 * Integer.BYTES + serializedAnnouncedEvent.capacity());

            serializedAnnouncement.putInt(0, ANNOUNCEMENT_EVENT);
            serializedAnnouncement.putInt(4, announcement.getSequenceNumber());
            serializedAnnouncement.position(8);
            serializedAnnouncement.put(serializedAnnouncedEvent);
            serializedAnnouncement.flip();
            return serializedAnnouncement;
        } else if (eventClass == SubtaskConnectionDescriptor.class) {
            SubtaskConnectionDescriptor selector = (SubtaskConnectionDescriptor) event;
            ByteBuffer buf = ByteBuffer.allocate(12);
            buf.putInt(VIRTUAL_CHANNEL_SELECTOR_EVENT);
            buf.putInt(selector.getInputSubtaskIndex());
            buf.putInt(selector.getOutputSubtaskIndex());
            buf.flip();
            return buf;
        } else {
            try {
                final DataOutputSerializer serializer = new DataOutputSerializer(128);
                serializer.writeInt(OTHER_EVENT);
                serializer.writeUTF(event.getClass().getName());
                event.write(serializer);
                return serializer.wrapAsByteBuffer();
            } catch (IOException e) {
                throw new IOException("Error while serializing event.", e);
            }
        }
    }

    public static AbstractEvent fromSerializedEvent(ByteBuffer buffer, ClassLoader classLoader)
            throws IOException {
        if (buffer.remaining() < 4) {
            throw new IOException("Incomplete event");
        }

        final ByteOrder bufferOrder = buffer.order();
        buffer.order(ByteOrder.BIG_ENDIAN);

        try {
            final int type = buffer.getInt();

            if (type == END_OF_PARTITION_EVENT) {
                return EndOfPartitionEvent.INSTANCE;
            } else if (type == CHECKPOINT_BARRIER_EVENT) {
                return deserializeCheckpointBarrier(buffer);
            } else if (type == END_OF_SUPERSTEP_EVENT) {
                return EndOfSuperstepEvent.INSTANCE;
            } else if (type == END_OF_CHANNEL_STATE_EVENT) {
                return EndOfChannelStateEvent.INSTANCE;
            } else if (type == END_OF_USER_RECORDS_EVENT) {
                return EndOfData.INSTANCE;
            } else if (type == CANCEL_CHECKPOINT_MARKER_EVENT) {
                long id = buffer.getLong();
                return new CancelCheckpointMarker(id);
            } else if (type == ANNOUNCEMENT_EVENT) {
                int sequenceNumber = buffer.getInt();
                AbstractEvent announcedEvent = fromSerializedEvent(buffer, classLoader);
                return new EventAnnouncement(announcedEvent, sequenceNumber);
            } else if (type == VIRTUAL_CHANNEL_SELECTOR_EVENT) {
                return new SubtaskConnectionDescriptor(buffer.getInt(), buffer.getInt());
            } else if (type == OTHER_EVENT) {
                try {
                    final DataInputDeserializer deserializer = new DataInputDeserializer(buffer);
                    final String className = deserializer.readUTF();

                    final Class<? extends AbstractEvent> clazz;
                    try {
                        clazz = classLoader.loadClass(className).asSubclass(AbstractEvent.class);
                    } catch (ClassNotFoundException e) {
                        throw new IOException("Could not load event class '" + className + "'.", e);
                    } catch (ClassCastException e) {
                        throw new IOException(
                                "The class '"
                                        + className
                                        + "' is not a valid subclass of '"
                                        + AbstractEvent.class.getName()
                                        + "'.",
                                e);
                    }

                    final AbstractEvent event =
                            InstantiationUtil.instantiate(clazz, AbstractEvent.class);
                    event.read(deserializer);

                    return event;
                } catch (Exception e) {
                    throw new IOException("Error while deserializing or instantiating event.", e);
                }
            } else {
                throw new IOException("Corrupt byte stream for event");
            }
        } finally {
            buffer.order(bufferOrder);
        }
    }

    private static ByteBuffer serializeCheckpointBarrier(CheckpointBarrier barrier)
            throws IOException {
        final CheckpointOptions checkpointOptions = barrier.getCheckpointOptions();
        final CheckpointType checkpointType = checkpointOptions.getCheckpointType();

        final byte[] locationBytes =
                checkpointOptions.getTargetLocation().isDefaultReference()
                        ? null
                        : checkpointOptions.getTargetLocation().getReferenceBytes();

        final ByteBuffer buf =
                ByteBuffer.allocate(38 + (locationBytes == null ? 0 : locationBytes.length));

        // we do not use checkpointType.ordinal() here to make the serialization robust
        // against changes in the enum (such as changes in the order of the values)
        final int typeInt;
        if (checkpointType == CheckpointType.CHECKPOINT) {
            typeInt = CHECKPOINT_TYPE_CHECKPOINT;
        } else if (checkpointType == CheckpointType.SAVEPOINT) {
            typeInt = CHECKPOINT_TYPE_SAVEPOINT;
        } else if (checkpointType == CheckpointType.SAVEPOINT_SUSPEND) {
            typeInt = CHECKPOINT_TYPE_SAVEPOINT_SUSPEND;
        } else if (checkpointType == CheckpointType.SAVEPOINT_TERMINATE) {
            typeInt = CHECKPOINT_TYPE_SAVEPOINT_TERMINATE;
        } else {
            throw new IOException("Unknown checkpoint type: " + checkpointType);
        }

        buf.putInt(CHECKPOINT_BARRIER_EVENT);
        buf.putLong(barrier.getId());
        buf.putLong(barrier.getTimestamp());
        buf.putInt(typeInt);

        if (locationBytes == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(locationBytes.length);
            buf.put(locationBytes);
        }
        buf.put((byte) checkpointOptions.getAlignment().ordinal());
        buf.putLong(checkpointOptions.getAlignedCheckpointTimeout());

        buf.flip();
        return buf;
    }

    private static CheckpointBarrier deserializeCheckpointBarrier(ByteBuffer buffer)
            throws IOException {
        final long id = buffer.getLong();
        final long timestamp = buffer.getLong();

        final int checkpointTypeCode = buffer.getInt();
        final int locationRefLen = buffer.getInt();

        final CheckpointType checkpointType;
        if (checkpointTypeCode == CHECKPOINT_TYPE_CHECKPOINT) {
            checkpointType = CheckpointType.CHECKPOINT;
        } else if (checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT) {
            checkpointType = CheckpointType.SAVEPOINT;
        } else if (checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT_SUSPEND) {
            checkpointType = CheckpointType.SAVEPOINT_SUSPEND;
        } else if (checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT_TERMINATE) {
            checkpointType = CheckpointType.SAVEPOINT_TERMINATE;
        } else {
            throw new IOException("Unknown checkpoint type code: " + checkpointTypeCode);
        }

        final CheckpointStorageLocationReference locationRef;
        if (locationRefLen == -1) {
            locationRef = CheckpointStorageLocationReference.getDefault();
        } else {
            byte[] bytes = new byte[locationRefLen];
            buffer.get(bytes);
            locationRef = new CheckpointStorageLocationReference(bytes);
        }
        final CheckpointOptions.AlignmentType alignmentType =
                CheckpointOptions.AlignmentType.values()[buffer.get()];
        final long alignmentTimeout = buffer.getLong();

        return new CheckpointBarrier(
                id,
                timestamp,
                new CheckpointOptions(
                        checkpointType, locationRef, alignmentType, alignmentTimeout));
    }

    // ------------------------------------------------------------------------
    // Buffer helpers
    // ------------------------------------------------------------------------

    public static Buffer toBuffer(AbstractEvent event, boolean hasPriority) throws IOException {
        final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

        MemorySegment data = MemorySegmentFactory.wrap(serializedEvent.array());

        final Buffer buffer =
                new NetworkBuffer(
                        data, FreeingBufferRecycler.INSTANCE, getDataType(event, hasPriority));
        buffer.setSize(serializedEvent.remaining());

        return buffer;
    }

    public static BufferConsumer toBufferConsumer(AbstractEvent event, boolean hasPriority)
            throws IOException {
        final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

        MemorySegment data = MemorySegmentFactory.wrap(serializedEvent.array());

        return new BufferConsumer(
                new NetworkBuffer(
                        data, FreeingBufferRecycler.INSTANCE, getDataType(event, hasPriority)),
                data.size());
    }

    public static AbstractEvent fromBuffer(Buffer buffer, ClassLoader classLoader)
            throws IOException {
        return fromSerializedEvent(buffer.getNioBufferReadable(), classLoader);
    }
}
