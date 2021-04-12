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
import java.util.ArrayList;
import java.util.List;

/** XaSinkStateSerializer. */
@Internal
public final class XaSinkStateSerializer extends TypeSerializer<JdbcXaSinkFunctionState> {

    private static final TypeSerializerSnapshot<JdbcXaSinkFunctionState> SNAPSHOT =
            new XaSinkStateSimpleXaTypeSerializerSnapshot();
    private final TypeSerializer<Xid> xidSerializer;
    private final TypeSerializer<CheckpointAndXid> checkpointAndXidSerializer;

    public XaSinkStateSerializer() {
        this(new XidSerializer(), new CheckpointAndXidSerializer());
    }

    private XaSinkStateSerializer(
            TypeSerializer<Xid> xidSerializer,
            TypeSerializer<CheckpointAndXid> checkpointAndXidSerializer) {
        this.xidSerializer = xidSerializer;
        this.checkpointAndXidSerializer = checkpointAndXidSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<JdbcXaSinkFunctionState> duplicate() {
        return this;
    }

    @Override
    public JdbcXaSinkFunctionState createInstance() {
        return JdbcXaSinkFunctionState.empty();
    }

    @Override
    public JdbcXaSinkFunctionState copy(JdbcXaSinkFunctionState from) {
        return from;
    }

    @Override
    public JdbcXaSinkFunctionState copy(
            JdbcXaSinkFunctionState from, JdbcXaSinkFunctionState reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(JdbcXaSinkFunctionState state, DataOutputView target) throws IOException {
        target.writeInt(state.getHanging().size());
        for (Xid h : state.getHanging()) {
            xidSerializer.serialize(h, target);
        }
        target.writeInt(state.getPrepared().size());
        for (CheckpointAndXid checkpointAndXid : state.getPrepared()) {
            checkpointAndXidSerializer.serialize(checkpointAndXid, target);
        }
    }

    @Override
    public JdbcXaSinkFunctionState deserialize(DataInputView source) throws IOException {
        int hangingSize = source.readInt();
        List<Xid> hanging = new ArrayList<>(hangingSize);
        for (int i = 0; i < hangingSize; i++) {
            hanging.add(xidSerializer.deserialize(source));
        }
        int preparedSize = source.readInt();
        List<CheckpointAndXid> prepared = new ArrayList<>(preparedSize);
        for (int i = 0; i < preparedSize; i++) {
            prepared.add(checkpointAndXidSerializer.deserialize(source));
        }
        return JdbcXaSinkFunctionState.of(prepared, hanging);
    }

    @Override
    public JdbcXaSinkFunctionState deserialize(JdbcXaSinkFunctionState reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof XaSinkStateSerializer;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<JdbcXaSinkFunctionState> snapshotConfiguration() {
        return SNAPSHOT;
    }

    /** Simple {@link TypeSerializerSnapshot} for {@link XaSinkStateSerializer}. */
    public static class XaSinkStateSimpleXaTypeSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<JdbcXaSinkFunctionState> {
        private static final int VERSION = 1;

        public XaSinkStateSimpleXaTypeSerializerSnapshot() {
            super(XaSinkStateSerializer::new);
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
