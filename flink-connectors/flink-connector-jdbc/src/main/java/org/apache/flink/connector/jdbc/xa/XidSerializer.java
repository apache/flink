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

/** {@link Xid} serializer. */
@Internal
public final class XidSerializer extends TypeSerializer<Xid> {

    private static final long serialVersionUID = 1L;

    private static final TypeSerializerSnapshot<Xid> SNAPSHOT =
            new XidSimpleTypeSerializerSnapshot();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<Xid> duplicate() {
        return this;
    }

    @Override
    public Xid createInstance() {
        return new XidImpl(0, new byte[0], new byte[0]);
    }

    @Override
    public Xid copy(Xid from) {
        return from;
    }

    @Override
    public Xid copy(Xid from, Xid reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Xid xid, DataOutputView target) throws IOException {
        target.writeInt(xid.getFormatId());
        writeBytesWithSize(target, xid.getGlobalTransactionId());
        writeBytesWithSize(target, xid.getBranchQualifier());
    }

    @Override
    public Xid deserialize(DataInputView source) throws IOException {
        return new XidImpl(source.readInt(), readBytesWithSize(source), readBytesWithSize(source));
    }

    private void writeBytesWithSize(DataOutputView target, byte[] bytes) throws IOException {
        target.writeByte(bytes.length);
        target.write(bytes, 0, bytes.length);
    }

    private byte[] readBytesWithSize(DataInputView source) throws IOException {
        byte len = source.readByte();
        byte[] bytes = new byte[len];
        source.read(bytes, 0, len);
        return bytes;
    }

    @Override
    public Xid deserialize(Xid reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof XidSerializer;
    }

    @Override
    public int hashCode() {
        return XidSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Xid> snapshotConfiguration() {
        return SNAPSHOT;
    }

    /** Simple {@link TypeSerializerSnapshot} for {@link XidSerializer}. */
    public static class XidSimpleTypeSerializerSnapshot extends SimpleTypeSerializerSnapshot<Xid> {
        private static final int VERSION = 1;

        public XidSimpleTypeSerializerSnapshot() {
            super(XidSerializer::new);
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
