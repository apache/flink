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
import org.apache.flink.table.data.GeographyData;

import java.io.IOException;

/** Serializer for {@link GeographyData}. */
@Internal
public final class GeographyDataSerializer extends TypeSerializerSingleton<GeographyData> {

    private static final long serialVersionUID = 1L;

    private static final byte[] EMPTY_GEOMETRY_COLLECTION =
            new byte[] {1, GeographyData.GEOMETRY_COLLECTION, 0, 0, 0, 0, 0, 0, 0};

    public static final GeographyDataSerializer INSTANCE = new GeographyDataSerializer();

    private GeographyDataSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public GeographyData createInstance() {
        return GeographyData.fromBytes(EMPTY_GEOMETRY_COLLECTION);
    }

    @Override
    public GeographyData copy(GeographyData from) {
        return GeographyData.fromBytes(from.toBytes());
    }

    @Override
    public GeographyData copy(GeographyData from, GeographyData reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(GeographyData record, DataOutputView target) throws IOException {
        final byte[] bytes = record.toBytes();
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public GeographyData deserialize(DataInputView source) throws IOException {
        final int length = source.readInt();
        final byte[] bytes = new byte[length];
        source.readFully(bytes);
        return GeographyData.fromBytes(bytes);
    }

    @Override
    public GeographyData deserialize(GeographyData reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public TypeSerializerSnapshot<GeographyData> snapshotConfiguration() {
        return new GeographyDataSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class GeographyDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<GeographyData> {

        public GeographyDataSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
