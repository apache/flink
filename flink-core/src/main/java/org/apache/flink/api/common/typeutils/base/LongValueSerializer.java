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
import org.apache.flink.types.LongValue;

import java.io.IOException;

@Internal
public final class LongValueSerializer extends TypeSerializerSingleton<LongValue> {

    private static final long serialVersionUID = 1L;

    public static final LongValueSerializer INSTANCE = new LongValueSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public LongValue createInstance() {
        return new LongValue();
    }

    @Override
    public LongValue copy(LongValue from) {
        return copy(from, new LongValue());
    }

    @Override
    public LongValue copy(LongValue from, LongValue reuse) {
        reuse.setValue(from.getValue());
        return reuse;
    }

    @Override
    public int getLength() {
        return 8;
    }

    @Override
    public void serialize(LongValue record, DataOutputView target) throws IOException {
        record.write(target);
    }

    @Override
    public LongValue deserialize(DataInputView source) throws IOException {
        return deserialize(new LongValue(), source);
    }

    @Override
    public LongValue deserialize(LongValue reuse, DataInputView source) throws IOException {
        reuse.read(source);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
    }

    @Override
    public TypeSerializerSnapshot<LongValue> snapshotConfiguration() {
        return new LongValueSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class LongValueSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<LongValue> {

        public LongValueSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
