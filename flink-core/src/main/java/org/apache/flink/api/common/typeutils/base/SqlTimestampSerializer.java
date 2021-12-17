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
import java.sql.Timestamp;

@Internal
public final class SqlTimestampSerializer extends TypeSerializerSingleton<Timestamp> {

    private static final long serialVersionUID = 1L;

    public static final SqlTimestampSerializer INSTANCE = new SqlTimestampSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Timestamp createInstance() {
        return new Timestamp(0L);
    }

    @Override
    public Timestamp copy(Timestamp from) {
        if (from == null) {
            return null;
        }
        final Timestamp t = new Timestamp(from.getTime());
        t.setNanos(from.getNanos());
        return t;
    }

    @Override
    public Timestamp copy(Timestamp from, Timestamp reuse) {
        if (from == null) {
            return null;
        }
        reuse.setTime(from.getTime());
        reuse.setNanos(from.getNanos());
        return reuse;
    }

    @Override
    public int getLength() {
        return 12;
    }

    @Override
    public void serialize(Timestamp record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeLong(Long.MIN_VALUE);
            target.writeInt(0);
        } else {
            target.writeLong(record.getTime());
            target.writeInt(record.getNanos());
        }
    }

    @Override
    public Timestamp deserialize(DataInputView source) throws IOException {
        final long v = source.readLong();
        if (v == Long.MIN_VALUE) {
            return null;
        } else {
            final Timestamp t = new Timestamp(v);
            t.setNanos(source.readInt());
            return t;
        }
    }

    @Override
    public Timestamp deserialize(Timestamp reuse, DataInputView source) throws IOException {
        final long v = source.readLong();
        if (v == Long.MIN_VALUE) {
            return null;
        }
        reuse.setTime(v);
        reuse.setNanos(source.readInt());
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
        target.writeInt(source.readInt());
    }

    @Override
    public TypeSerializerSnapshot<Timestamp> snapshotConfiguration() {
        return new SqlTimestampSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class SqlTimestampSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Timestamp> {

        public SqlTimestampSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
