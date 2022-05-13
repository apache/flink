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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Specialized serializer for {@code NullValueArray}. */
public final class NullValueArraySerializer extends TypeSerializerSingleton<NullValueArray> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public NullValueArray createInstance() {
        return new NullValueArray();
    }

    @Override
    public NullValueArray copy(NullValueArray from) {
        return copy(from, new NullValueArray());
    }

    @Override
    public NullValueArray copy(NullValueArray from, NullValueArray reuse) {
        reuse.setValue(from);
        return reuse;
    }

    @Override
    public int getLength() {
        return 4;
    }

    @Override
    public void serialize(NullValueArray record, DataOutputView target) throws IOException {
        record.write(target);
    }

    @Override
    public NullValueArray deserialize(DataInputView source) throws IOException {
        return deserialize(new NullValueArray(), source);
    }

    @Override
    public NullValueArray deserialize(NullValueArray reuse, DataInputView source)
            throws IOException {
        reuse.read(source);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, getLength());
    }

    // -----------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<NullValueArray> snapshotConfiguration() {
        return new NullValueArraySerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class NullValueArraySerializerSnapshot
            extends SimpleTypeSerializerSnapshot<NullValueArray> {

        public NullValueArraySerializerSnapshot() {
            super(NullValueArraySerializer::new);
        }
    }
}
