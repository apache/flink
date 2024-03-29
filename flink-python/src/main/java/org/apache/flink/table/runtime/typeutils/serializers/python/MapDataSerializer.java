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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A {@link TypeSerializer} for {@link MapData}. It should be noted that the header will not be
 * encoded. Currently Python doesn't support BinaryMapData natively, so we can't use
 * BaseArraySerializer directly.
 */
@Internal
public class MapDataSerializer extends org.apache.flink.table.runtime.typeutils.MapDataSerializer {

    private static final long serialVersionUID = 1L;

    private final LogicalType keyType;

    private final LogicalType valueType;

    private final TypeSerializer keyTypeSerializer;

    private final TypeSerializer valueTypeSerializer;

    private final BinaryWriter.ValueSetter keySetter;

    private final BinaryWriter.ValueSetter valueSetter;

    private final ArrayData.ElementGetter keyGetter;

    private final ArrayData.ElementGetter valueGetter;

    private final BinaryArrayWriter.NullSetter nullValueSetter;

    private final int keySize;

    private final int valueSize;

    public MapDataSerializer(
            LogicalType keyType,
            LogicalType valueType,
            TypeSerializer keyTypeSerializer,
            TypeSerializer valueTypeSerializer) {
        super(keyType, valueType);
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyTypeSerializer = keyTypeSerializer;
        this.valueTypeSerializer = valueTypeSerializer;
        this.keySize = BinaryArrayData.calculateFixLengthPartSize(this.keyType);
        this.valueSize = BinaryArrayData.calculateFixLengthPartSize(this.valueType);
        this.keyGetter = ArrayData.createElementGetter(keyType);
        this.valueGetter = ArrayData.createElementGetter(valueType);
        this.nullValueSetter = BinaryArrayWriter.createNullSetter(valueType);
        this.keySetter = BinaryWriter.createValueSetter(keyType);
        this.valueSetter = BinaryWriter.createValueSetter(valueType);
    }

    @Override
    public void serialize(MapData map, DataOutputView target) throws IOException {
        BinaryMapData binaryMap = toBinaryMap(map);
        final int size = binaryMap.size();
        target.writeInt(size);
        BinaryArrayData keyArray = binaryMap.keyArray();
        BinaryArrayData valueArray = binaryMap.valueArray();
        for (int i = 0; i < size; i++) {
            if (keyArray.isNullAt(i)) {
                throw new IllegalArgumentException("The key of BinaryMapData must not be null.");
            }
            Object key = keyGetter.getElementOrNull(keyArray, i);
            keyTypeSerializer.serialize(key, target);
            if (valueArray.isNullAt(i)) {
                target.writeBoolean(true);
            } else {
                target.writeBoolean(false);
                Object value = valueGetter.getElementOrNull(valueArray, i);
                valueTypeSerializer.serialize(value, target);
            }
        }
    }

    @Override
    public MapData deserialize(DataInputView source) throws IOException {
        BinaryArrayData keyArray = new BinaryArrayData();
        BinaryArrayData valueArray = new BinaryArrayData();
        return deserializeInternal(source, keyArray, valueArray);
    }

    @Override
    public MapData deserialize(MapData reuse, DataInputView source) throws IOException {
        if (reuse instanceof GenericMapData) {
            return deserialize(source);
        }
        BinaryMapData binaryMap = (BinaryMapData) reuse;
        return deserializeInternal(source, binaryMap.keyArray(), binaryMap.valueArray());
    }

    private MapData deserializeInternal(
            DataInputView source, BinaryArrayData keyArray, BinaryArrayData valueArray)
            throws IOException {
        final int size = source.readInt();
        BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyArray, size, keySize);
        BinaryArrayWriter valueWriter = new BinaryArrayWriter(valueArray, size, valueSize);
        for (int i = 0; i < size; i++) {
            Object key = keyTypeSerializer.deserialize(source);
            keySetter.setValue(keyWriter, i, key);
            boolean isNull = source.readBoolean();
            if (isNull) {
                nullValueSetter.setNull(valueWriter, i);
            } else {
                Object value = valueTypeSerializer.deserialize(source);
                valueSetter.setValue(valueWriter, i, value);
            }
        }
        keyWriter.complete();
        valueWriter.complete();
        return BinaryMapData.valueOf(keyArray, valueArray);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializer<MapData> duplicate() {
        return new MapDataSerializer(keyType, valueType, keyTypeSerializer, valueTypeSerializer);
    }

    @Override
    public TypeSerializerSnapshot<MapData> snapshotConfiguration() {
        return new BaseMapSerializerSnapshot(
                keyType, valueType, keyTypeSerializer, valueTypeSerializer);
    }

    /** {@link TypeSerializerSnapshot} for {@link MapDataSerializer}. */
    public static final class BaseMapSerializerSnapshot implements TypeSerializerSnapshot<MapData> {
        private static final int CURRENT_VERSION = 1;

        private LogicalType keyType;
        private LogicalType valueType;

        private TypeSerializer keySerializer;
        private TypeSerializer valueSerializer;

        @SuppressWarnings("unused")
        public BaseMapSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        BaseMapSerializerSnapshot(
                LogicalType keyT,
                LogicalType valueT,
                TypeSerializer keySer,
                TypeSerializer valueSer) {
            this.keyType = keyT;
            this.valueType = valueT;

            this.keySerializer = keySer;
            this.valueSerializer = valueSer;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            DataOutputViewStream outStream = new DataOutputViewStream(out);
            InstantiationUtil.serializeObject(outStream, keyType);
            InstantiationUtil.serializeObject(outStream, valueType);
            InstantiationUtil.serializeObject(outStream, keySerializer);
            InstantiationUtil.serializeObject(outStream, valueSerializer);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            try {
                DataInputViewStream inStream = new DataInputViewStream(in);
                this.keyType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
                this.valueType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
                this.keySerializer =
                        InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
                this.valueSerializer =
                        InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<MapData> restoreSerializer() {
            return new MapDataSerializer(keyType, valueType, keySerializer, valueSerializer);
        }

        @Override
        public TypeSerializerSchemaCompatibility<MapData> resolveSchemaCompatibility(
                TypeSerializerSnapshot<MapData> oldSerializerSnapshot) {
            if (!(oldSerializerSnapshot instanceof BaseMapSerializerSnapshot)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            BaseMapSerializerSnapshot oldBaseMapDataSerializerSnapshot =
                    (BaseMapSerializerSnapshot) oldSerializerSnapshot;
            if (!keyType.equals(oldBaseMapDataSerializerSnapshot.keyType)
                    || !valueType.equals(oldBaseMapDataSerializerSnapshot.valueType)
                    || !keySerializer.equals(oldBaseMapDataSerializerSnapshot.keySerializer)
                    || !valueSerializer.equals(oldBaseMapDataSerializerSnapshot.valueSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
