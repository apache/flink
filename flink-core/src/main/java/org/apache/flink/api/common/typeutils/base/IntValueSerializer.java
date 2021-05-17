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
import org.apache.flink.types.IntValue;

import java.io.IOException;

@Internal
public final class IntValueSerializer extends TypeSerializerSingleton<IntValue> {

    private static final long serialVersionUID = 1L;

    public static final IntValueSerializer INSTANCE = new IntValueSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public IntValue createInstance() {
        return new IntValue();
    }

    @Override
    public IntValue copy(IntValue from) {
        return copy(from, new IntValue());
    }

    @Override
    public IntValue copy(IntValue from, IntValue reuse) {
        reuse.setValue(from.getValue());
        return reuse;
    }

    @Override
    public int getLength() {
        return 4;
    }

    @Override
    public void serialize(IntValue record, DataOutputView target) throws IOException {
        record.write(target);
    }

    @Override
    public IntValue deserialize(DataInputView source) throws IOException {
        return deserialize(new IntValue(), source);
    }

    @Override
    public IntValue deserialize(IntValue reuse, DataInputView source) throws IOException {
        reuse.read(source);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeInt(source.readInt());
    }

    @Override
    public TypeSerializerSnapshot<IntValue> snapshotConfiguration() {
        return new IntValueSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class IntValueSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<IntValue> {

        public IntValueSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
