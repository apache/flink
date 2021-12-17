/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Objects;

/** {@link CheckpointAndXid} serializer. */
@Internal
public final class CheckpointAndXidSerializer extends TypeSerializer<CheckpointAndXid> {

    private static final long serialVersionUID = 1L;

    public static final TypeSerializerSnapshot<CheckpointAndXid> SNAPSHOT =
            new CheckpointAndXidSimpleTypeSerializerSnapshot();

    private final TypeSerializer<Xid> xidSerializer = new XidSerializer();

    @Override
    public boolean isImmutableType() {
        return xidSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<CheckpointAndXid> duplicate() {
        return this;
    }

    @Override
    public CheckpointAndXid createInstance() {
        return CheckpointAndXid.createRestored(0L, 0, xidSerializer.createInstance());
    }

    @Override
    public CheckpointAndXid copy(CheckpointAndXid from) {
        return CheckpointAndXid.createRestored(
                from.checkpointId, from.attempts, xidSerializer.copy(from.xid));
    }

    @Override
    public CheckpointAndXid copy(CheckpointAndXid from, CheckpointAndXid reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(CheckpointAndXid record, DataOutputView target) throws IOException {
        target.writeLong(record.checkpointId);
        target.writeInt(record.attempts);
        xidSerializer.serialize(record.xid, target);
    }

    @Override
    public CheckpointAndXid deserialize(DataInputView source) throws IOException {
        return CheckpointAndXid.createRestored(
                source.readLong(), source.readInt(), xidSerializer.deserialize(source));
    }

    @Override
    public CheckpointAndXid deserialize(CheckpointAndXid reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CheckpointAndXidSerializer;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xidSerializer);
    }

    @Override
    public TypeSerializerSnapshot<CheckpointAndXid> snapshotConfiguration() {
        return SNAPSHOT;
    }

    /** SImple {@link TypeSerializerSnapshot} for {@link CheckpointAndXidSerializer}. */
    public static class CheckpointAndXidSimpleTypeSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<CheckpointAndXid> {
        private static final int VERSION = 1;

        public CheckpointAndXidSimpleTypeSerializerSnapshot() {
            super(CheckpointAndXidSerializer::new);
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            super.writeSnapshot(out);
            out.writeInt(VERSION);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader)
                throws IOException {
            super.readSnapshot(readVersion, in, classLoader);
            in.readInt();
        }
    }
}
