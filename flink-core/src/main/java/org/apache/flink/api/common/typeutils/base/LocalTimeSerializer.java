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
import java.time.LocalTime;

@Internal
public final class LocalTimeSerializer extends TypeSerializerSingleton<LocalTime> {

    private static final long serialVersionUID = 1L;

    public static final LocalTimeSerializer INSTANCE = new LocalTimeSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public LocalTime createInstance() {
        return LocalTime.MIDNIGHT;
    }

    @Override
    public LocalTime copy(LocalTime from) {
        return from;
    }

    @Override
    public LocalTime copy(LocalTime from, LocalTime reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 7;
    }

    @Override
    public void serialize(LocalTime record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeByte(Byte.MIN_VALUE);
            target.writeShort(Short.MIN_VALUE);
            target.writeInt(Integer.MIN_VALUE);
        } else {
            target.writeByte(record.getHour());
            target.writeByte(record.getMinute());
            target.writeByte(record.getSecond());
            target.writeInt(record.getNano());
        }
    }

    @Override
    public LocalTime deserialize(DataInputView source) throws IOException {
        final byte hour = source.readByte();
        if (hour == Byte.MIN_VALUE) {
            source.readShort();
            source.readInt();
            return null;
        } else {
            return LocalTime.of(hour, source.readByte(), source.readByte(), source.readInt());
        }
    }

    @Override
    public LocalTime deserialize(LocalTime reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte());
        target.writeShort(source.readShort());
        target.writeInt(source.readInt());
    }

    @Override
    public TypeSerializerSnapshot<LocalTime> snapshotConfiguration() {
        return new LocalTimeSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class LocalTimeSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<LocalTime> {

        public LocalTimeSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
