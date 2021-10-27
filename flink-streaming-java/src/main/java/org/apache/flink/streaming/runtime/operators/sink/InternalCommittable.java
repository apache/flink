package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class InternalCommittable<CommT> {
    private final CommT committable;
    private final int subtaskId;
    /** May change after recovery. */
    private final int numberOfSubtasks;

    private final long checkpointId;
    private final int committableIndex;
    private final int numberCommittablesOfSubtask;

    public InternalCommittable(
            CommT committable,
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            int committableIndex,
            int numberCommittablesOfSubtask) {
        this.committable = checkNotNull(committable);
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.committableIndex = committableIndex;
        this.numberCommittablesOfSubtask = numberCommittablesOfSubtask;
    }

    public CommT getCommittable() {
        return committable;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public int getCommittableIndex() {
        return committableIndex;
    }

    public int getNumberCommittablesOfSubtask() {
        return numberCommittablesOfSubtask;
    }

    public static class Serializer<CommT>
            implements SimpleVersionedSerializer<InternalCommittable<CommT>> {
        private final SimpleVersionedSerializer<CommT> committableSerializer;

        public Serializer(SimpleVersionedSerializer<CommT> committableSerializer) {
            this.committableSerializer = checkNotNull(committableSerializer);
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(InternalCommittable<CommT> obj) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    committableSerializer, obj.committable, out);
            out.writeInt(obj.subtaskId);
            out.writeInt(obj.numberOfSubtasks);
            out.writeLong(obj.checkpointId);
            out.writeInt(obj.committableIndex);
            out.writeInt(obj.numberCommittablesOfSubtask);
            return out.getCopyOfBuffer();
        }

        @Override
        public InternalCommittable<CommT> deserialize(int version, byte[] serialized)
                throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            return new InternalCommittable<>(
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            committableSerializer, in),
                    in.readInt(),
                    in.readInt(),
                    in.readLong(),
                    in.readInt(),
                    in.readInt());
        }
    }
}
