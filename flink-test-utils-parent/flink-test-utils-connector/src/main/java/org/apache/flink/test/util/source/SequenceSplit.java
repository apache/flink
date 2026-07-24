/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.Objects;

/**
 * A split carrying the emission position of one source subtask, so that a test source can resume
 * from the checkpointed position after recovery. The subtask index assumes a fixed source
 * parallelism.
 */
@Experimental
public class SequenceSplit implements SourceSplit {

    /** Serializer for {@link SequenceSplit} instances. */
    public static final SimpleVersionedSerializer<SequenceSplit> SERIALIZER =
            new SequenceSplitSerializer();

    private final int subtaskIndex;
    private final long position;

    public SequenceSplit(int subtaskIndex, long position) {
        this.subtaskIndex = subtaskIndex;
        this.position = position;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public long getPosition() {
        return position;
    }

    @Override
    public String splitId() {
        return "sequence-split-" + subtaskIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SequenceSplit)) {
            return false;
        }
        final SequenceSplit that = (SequenceSplit) o;
        return subtaskIndex == that.subtaskIndex && position == that.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtaskIndex, position);
    }

    @Override
    public String toString() {
        return "SequenceSplit{subtaskIndex=" + subtaskIndex + ", position=" + position + "}";
    }

    private static class SequenceSplitSerializer
            implements SimpleVersionedSerializer<SequenceSplit> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(SequenceSplit split) throws IOException {
            final DataOutputSerializer out = new DataOutputSerializer(12);
            out.writeInt(split.subtaskIndex);
            out.writeLong(split.position);
            return out.getCopyOfBuffer();
        }

        @Override
        public SequenceSplit deserialize(int version, byte[] serialized) throws IOException {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return new SequenceSplit(in.readInt(), in.readLong());
        }
    }
}
