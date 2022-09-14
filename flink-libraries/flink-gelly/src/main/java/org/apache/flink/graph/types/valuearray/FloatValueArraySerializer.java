/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Specialized serializer for {@code FloatValueArray}. */
public final class FloatValueArraySerializer extends TypeSerializerSingleton<FloatValueArray> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public FloatValueArray createInstance() {
        return new FloatValueArray();
    }

    @Override
    public FloatValueArray copy(FloatValueArray from) {
        return copy(from, new FloatValueArray());
    }

    @Override
    public FloatValueArray copy(FloatValueArray from, FloatValueArray reuse) {
        reuse.setValue(from);
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(FloatValueArray record, DataOutputView target) throws IOException {
        record.write(target);
    }

    @Override
    public FloatValueArray deserialize(DataInputView source) throws IOException {
        return deserialize(new FloatValueArray(), source);
    }

    @Override
    public FloatValueArray deserialize(FloatValueArray reuse, DataInputView source)
            throws IOException {
        reuse.read(source);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        FloatValueArray.copyInternal(source, target);
    }

    // -----------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<FloatValueArray> snapshotConfiguration() {
        return new FloatValueArraySerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class FloatValueArraySerializerSnapshot
            extends SimpleTypeSerializerSnapshot<FloatValueArray> {

        public FloatValueArraySerializerSnapshot() {
            super(FloatValueArraySerializer::new);
        }
    }
}
