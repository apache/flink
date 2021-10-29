package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class CommittableWrapper {
    final int subtaskId;
    /** May change after recovery. */
    final int numberOfSubtasks;

    final long checkpointId;
    final int committableIndex;
    final int numberCommittablesOfSubtask;

    public CommittableWrapper(
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            int committableIndex,
            int numberCommittablesOfSubtask) {
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.committableIndex = committableIndex;
        this.numberCommittablesOfSubtask = numberCommittablesOfSubtask;
    }

    static class Committable<CommT> extends CommittableWrapper {
        private final CommT committable;

        public Committable(
                CommT committable,
                int subtaskId,
                int numberOfSubtasks,
                long checkpointId,
                int committableIndex,
                int numberCommittablesOfSubtask) {
            super(
                    subtaskId,
                    numberOfSubtasks,
                    checkpointId,
                    committableIndex,
                    numberCommittablesOfSubtask);
            this.committable = checkNotNull(committable);
        }

        public CommT getCommittable() {
            return committable;
        }

        public static class Serializer<CommT>
                implements SimpleVersionedSerializer<Committable<CommT>> {
            private final SimpleVersionedSerializer<CommT> committableSerializer;

            public Serializer(SimpleVersionedSerializer<CommT> committableSerializer) {
                this.committableSerializer = checkNotNull(committableSerializer);
            }

            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Committable<CommT> obj) throws IOException {
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
            public Committable<CommT> deserialize(int version, byte[] serialized)
                    throws IOException {
                DataInputDeserializer in = new DataInputDeserializer(serialized);
                return new Committable<>(
                        in.readInt(),
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                committableSerializer, in),
                        in.readInt(),
                        in.readInt(),
                        in.readLong(),
                        in.readInt());
            }
        }
    }

    public static <CommT> List<Committable<CommT>> singletonList(CommT committable) {
        return Collections.singletonList(wrap(committable));
    }

    @VisibleForTesting
    static <CommT> Committable<CommT> wrap(CommT committable) {
        return new Committable<>(committable, 0, 1, Long.MAX_VALUE, 0, 1);
    }

    static <StateT> MatchResult<Committable<StateT>> match(
            List<Committable<StateT>> committableWrappers, List<StateT> innerCommittables) {
        // Assume that (Global)Committer#commit does not create a new instance for returned
        // committables. This assumption is documented in the respective JavaDoc.
        Set<StateT> lookup =
                Collections.newSetFromMap(new IdentityHashMap<>(committableWrappers.size()));
        lookup.addAll(innerCommittables);

        Map<Boolean, List<Committable<StateT>>> matched =
                committableWrappers.stream()
                        .collect(Collectors.groupingBy(c -> lookup.contains(c.getCommittable())));
        return new MatchResult<>(
                matched.getOrDefault(true, Collections.emptyList()),
                matched.getOrDefault(false, Collections.emptyList()));
    }

    static <CommT> List<CommT> unwrap(List<Committable<CommT>> committables) {
        return committables.stream().map(Committable::getCommittable).collect(Collectors.toList());
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

    public static class Serializer<CommT> implements SimpleVersionedSerializer<Committable<CommT>> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Committable<CommT> obj) throws IOException {
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
        public Committable<CommT> deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            return new Committable<>(
                    in.readInt(),
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            committableSerializer, in),
                    in.readInt(),
                    in.readInt(),
                    in.readLong(),
                    in.readInt());
        }
    }

    static class MatchResult<T> {
        private final List<T> matched;
        private final List<T> unmatched;

        MatchResult(List<T> matched, List<T> unmatched) {
            this.unmatched = checkNotNull(unmatched);
            this.matched = checkNotNull(matched);
        }

        public List<T> getMatched() {
            return matched;
        }

        public List<T> getUnmatched() {
            return unmatched;
        }
    }
}
