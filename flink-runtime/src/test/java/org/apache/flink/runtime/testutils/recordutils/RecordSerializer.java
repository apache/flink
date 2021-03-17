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

package org.apache.flink.runtime.testutils.recordutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Record;

import java.io.IOException;

/** Implementation of the (de)serialization and copying logic for the {@link Record}. */
public final class RecordSerializer extends TypeSerializer<Record> {

    private static final long serialVersionUID = 1L;

    private static final RecordSerializer INSTANCE = new RecordSerializer(); // singleton instance

    private static final int MAX_BIT = 0x80; // byte where only the most significant bit is set

    // --------------------------------------------------------------------------------------------

    public static RecordSerializer get() {
        return INSTANCE;
    }

    /** Creates a new instance of the RecordSerializers. Private to prevent instantiation. */
    private RecordSerializer() {}

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public RecordSerializer duplicate() {
        // does not hold state, so just return ourselves
        return this;
    }

    @Override
    public Record createInstance() {
        return new Record();
    }

    @Override
    public Record copy(Record from) {
        return from.createCopy();
    }

    @Override
    public Record copy(Record from, Record reuse) {
        from.copyTo(reuse);
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void serialize(Record record, DataOutputView target) throws IOException {
        record.serialize(target);
    }

    @Override
    public Record deserialize(DataInputView source) throws IOException {
        return deserialize(new Record(), source);
    }

    @Override
    public Record deserialize(Record target, DataInputView source) throws IOException {
        target.deserialize(source);
        return target;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int val = source.readUnsignedByte();
        target.writeByte(val);

        if (val >= MAX_BIT) {
            int shift = 7;
            int curr;
            val = val & 0x7f;
            while ((curr = source.readUnsignedByte()) >= MAX_BIT) {
                target.writeByte(curr);
                val |= (curr & 0x7f) << shift;
                shift += 7;
            }
            target.writeByte(curr);
            val |= curr << shift;
        }

        target.write(source, val);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RecordSerializer;
    }

    @Override
    public int hashCode() {
        return RecordSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Record> snapshotConfiguration() {
        throw new UnsupportedOperationException();
    }
}
