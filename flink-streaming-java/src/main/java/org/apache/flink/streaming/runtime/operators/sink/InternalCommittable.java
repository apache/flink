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

    public static <CommT> List<InternalCommittable<CommT>> singletonList(CommT committable) {
        return Collections.singletonList(wrap(committable));
    }

    @VisibleForTesting
    static <CommT> InternalCommittable<CommT> wrap(CommT committable) {
        return new InternalCommittable<>(committable, 0, 1, Long.MAX_VALUE, 0, 1);
    }

    static <StateT> MatchResult<InternalCommittable<StateT>> match(
            List<InternalCommittable<StateT>> internalCommittables,
            List<StateT> innerCommittables) {
        // Assume that (Global)Committer#commit does not create a new instance for returned
        // committables. This assumption is documented in the respective JavaDoc.
        Set<StateT> lookup =
                Collections.newSetFromMap(new IdentityHashMap<>(internalCommittables.size()));
        lookup.addAll(innerCommittables);

        Map<Boolean, List<InternalCommittable<StateT>>> matched =
                internalCommittables.stream()
                        .collect(Collectors.groupingBy(c -> lookup.contains(c.getCommittable())));
        return new MatchResult<>(
                matched.getOrDefault(true, Collections.emptyList()),
                matched.getOrDefault(false, Collections.emptyList()));
    }

    static <CommT> List<CommT> unwrap(List<InternalCommittable<CommT>> committables) {
        return committables.stream()
                .map(InternalCommittable::getCommittable)
                .collect(Collectors.toList());
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
