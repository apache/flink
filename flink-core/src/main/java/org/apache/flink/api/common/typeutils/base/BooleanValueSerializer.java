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
import org.apache.flink.types.BooleanValue;

import java.io.IOException;

@Internal
public final class BooleanValueSerializer extends TypeSerializerSingleton<BooleanValue> {

    private static final long serialVersionUID = 1L;

    public static final BooleanValueSerializer INSTANCE = new BooleanValueSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public BooleanValue createInstance() {
        return new BooleanValue();
    }

    @Override
    public BooleanValue copy(BooleanValue from) {
        BooleanValue result = new BooleanValue();
        result.setValue(from.getValue());
        return result;
    }

    @Override
    public BooleanValue copy(BooleanValue from, BooleanValue reuse) {
        reuse.setValue(from.getValue());
        return reuse;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public void serialize(BooleanValue record, DataOutputView target) throws IOException {
        record.write(target);
    }

    @Override
    public BooleanValue deserialize(DataInputView source) throws IOException {
        return deserialize(new BooleanValue(), source);
    }

    @Override
    public BooleanValue deserialize(BooleanValue reuse, DataInputView source) throws IOException {
        reuse.read(source);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeBoolean(source.readBoolean());
    }

    @Override
    public TypeSerializerSnapshot<BooleanValue> snapshotConfiguration() {
        return new BooleanValueSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class BooleanValueSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<BooleanValue> {

        public BooleanValueSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
