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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.io.IOException;

/** Serializer for {@link StringData}. */
@Internal
public final class StringDataSerializer extends TypeSerializerSingleton<StringData> {

    private static final long serialVersionUID = 1L;

    public static final StringDataSerializer INSTANCE = new StringDataSerializer();

    private StringDataSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public StringData createInstance() {
        return StringData.fromString("");
    }

    @Override
    public StringData copy(StringData from) {
        // BinaryStringData is the only implementation of StringData
        return ((BinaryStringData) from).copy();
    }

    @Override
    public StringData copy(StringData from, StringData reuse) {
        // BinaryStringData is the only implementation of StringData
        return ((BinaryStringData) from).copy();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(StringData record, DataOutputView target) throws IOException {
        // BinaryStringData is the only implementation of StringData
        BinaryStringData string = (BinaryStringData) record;
        string.ensureMaterialized();
        target.writeInt(string.getSizeInBytes());
        BinarySegmentUtils.copyToView(
                string.getSegments(), string.getOffset(), string.getSizeInBytes(), target);
    }

    @Override
    public StringData deserialize(DataInputView source) throws IOException {
        return deserializeInternal(source);
    }

    public static StringData deserializeInternal(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return StringData.fromBytes(bytes);
    }

    @Override
    public StringData deserialize(StringData record, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public TypeSerializerSnapshot<StringData> snapshotConfiguration() {
        return new StringDataSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class StringDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<StringData> {

        public StringDataSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
