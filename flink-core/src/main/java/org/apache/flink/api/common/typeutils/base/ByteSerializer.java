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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Type serializer for {@code Byte}. */
@Internal
public final class ByteSerializer extends TypeSerializerSingleton<Byte> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the ByteSerializer. */
    public static final ByteSerializer INSTANCE = new ByteSerializer();

    private static final Byte ZERO = (byte) 0;

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public Byte createInstance() {
        return ZERO;
    }

    @Override
    public Byte copy(Byte from) {
        return from;
    }

    @Override
    public Byte copy(Byte from, Byte reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public void serialize(Byte record, DataOutputView target) throws IOException {
        target.writeByte(record);
    }

    @Override
    public Byte deserialize(DataInputView source) throws IOException {
        return source.readByte();
    }

    @Override
    public Byte deserialize(Byte reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte());
    }

    @Override
    public TypeSerializerSnapshot<Byte> snapshotConfiguration() {
        return new ByteSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ByteSerializerSnapshot extends SimpleTypeSerializerSnapshot<Byte> {

        public ByteSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
