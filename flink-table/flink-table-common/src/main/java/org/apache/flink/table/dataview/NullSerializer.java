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

package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A serializer for null. */
@Internal
public class NullSerializer extends TypeSerializerSingleton<Object> {

    // TODO move this class to org.apache.flink.table.runtime.typeutils
    //  after we dropped the legacy ListView/MapView logic

    private static final long serialVersionUID = -5381596724707742625L;

    public static final NullSerializer INSTANCE = new NullSerializer();

    private NullSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public Object createInstance() {
        return null;
    }

    @Override
    public Object copy(Object from) {
        return null;
    }

    @Override
    public Object copy(Object from, Object reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public void serialize(Object record, DataOutputView target) throws IOException {
        target.writeByte(0);
    }

    @Override
    public Object deserialize(DataInputView source) throws IOException {
        source.readByte();
        return null;
    }

    @Override
    public Object deserialize(Object reuse, DataInputView source) throws IOException {
        source.readByte();
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte());
    }

    @Override
    public TypeSerializerSnapshot<Object> snapshotConfiguration() {
        return new NullSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    @Internal
    public static final class NullSerializerSnapshot extends SimpleTypeSerializerSnapshot<Object> {
        public NullSerializerSnapshot() {
            super(() -> NullSerializer.INSTANCE);
        }
    }
}
