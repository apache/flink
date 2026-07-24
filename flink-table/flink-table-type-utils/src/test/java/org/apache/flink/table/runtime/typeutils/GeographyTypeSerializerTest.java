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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.GeographyType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GeographyTypeSerializer}. */
class GeographyTypeSerializerTest extends SerializerTestBase<GeographyData> {

    private static final int FORMAT_VERSION = 1;

    private static final byte[] POINT_WKB =
            new byte[] {
                1, GeographyData.POINT, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            };

    private static final byte[] BIG_ENDIAN_POINT_WKB =
            new byte[] {
                0, 0, 0, 0, GeographyData.POINT, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            };

    @Override
    protected TypeSerializer<GeographyData> createSerializer() {
        return GeographyTypeSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<GeographyData> getTypeClass() {
        return GeographyData.class;
    }

    @Override
    protected GeographyData[] getTestData() {
        return new GeographyData[] {
            GeographyData.fromBytes(POINT_WKB),
            GeographyData.fromBytes(BIG_ENDIAN_POINT_WKB),
            GeographyTypeSerializer.INSTANCE.createInstance()
        };
    }

    @Override
    protected void deepEquals(String message, GeographyData should, GeographyData is) {
        assertThat(is.toBytes()).as(message).isEqualTo(should.toBytes());
    }

    @Test
    void testInternalSerializerRoundTripsRawWkb() throws Exception {
        final TypeSerializer<GeographyData> serializer =
                InternalSerializers.create(new GeographyType());
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        serializer.serialize(geography, new DataOutputViewStreamWrapper(bytes));
        final GeographyData deserialized =
                serializer.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(bytes.toByteArray())));

        assertThat(deserialized.toBytes()).isEqualTo(POINT_WKB);
        assertThat(deserialized.subtypeId()).isEqualTo(GeographyData.POINT);
    }

    @Test
    void testSerializedFormUsesVersionedLengthEnvelope() throws Exception {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        GeographyTypeSerializer.INSTANCE.serialize(
                GeographyData.fromBytes(POINT_WKB), new DataOutputViewStreamWrapper(bytes));
        final DataInputViewStreamWrapper input =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes.toByteArray()));
        final byte[] payload = new byte[POINT_WKB.length];

        assertThat(input.readUnsignedByte()).isEqualTo(FORMAT_VERSION);
        assertThat(input.readInt()).isEqualTo(POINT_WKB.length);
        input.readFully(payload);
        assertThat(payload).isEqualTo(POINT_WKB);
    }

    @Test
    void testRejectsUnsupportedPayloadVersion() throws Exception {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(bytes);
        output.writeByte(2);
        output.writeInt(POINT_WKB.length);
        output.write(POINT_WKB);

        assertThatThrownBy(
                        () ->
                                GeographyTypeSerializer.INSTANCE.deserialize(
                                        new DataInputViewStreamWrapper(
                                                new ByteArrayInputStream(bytes.toByteArray()))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported GEOGRAPHY serializer format version 2");
    }

    @Test
    void testSnapshotSelfCompatibility() throws Exception {
        final TypeSerializerSnapshot<GeographyData> snapshot =
                GeographyTypeSerializer.INSTANCE.snapshotConfiguration();
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                new DataOutputViewStreamWrapper(bytes), snapshot);
        final TypeSerializerSnapshot<GeographyData> restoredSnapshot =
                TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(bytes.toByteArray())),
                        getClass().getClassLoader());

        assertThat(
                        GeographyTypeSerializer.INSTANCE
                                .snapshotConfiguration()
                                .resolveSchemaCompatibility(restoredSnapshot)
                                .isCompatibleAsIs())
                .isTrue();
        assertThat(restoredSnapshot.restoreSerializer()).isSameAs(GeographyTypeSerializer.INSTANCE);
    }

    @Test
    void testCopyPreservesRawWkbBytes() {
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);
        final GeographyData copied = GeographyTypeSerializer.INSTANCE.copy(geography);

        assertThat(copied).isNotSameAs(geography);
        assertThat(copied.toBytes()).isEqualTo(POINT_WKB);
        assertThat(copied.subtypeId()).isEqualTo(GeographyData.POINT);
    }

    @Test
    void testCreateInstanceReturnsValidGeographyData() {
        final GeographyData instance = GeographyTypeSerializer.INSTANCE.createInstance();

        assertThat(instance.subtypeId()).isEqualTo(GeographyData.GEOMETRY_COLLECTION);
        assertThat(instance.sizeInBytes()).isEqualTo(9);
    }

    @Test
    void testRowDataSerializerConvertsGenericRowToBinaryRow() {
        final RowDataSerializer serializer =
                InternalTypeInfo.<RowData>ofFields(new GeographyType()).toRowSerializer();
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);
        final GenericRowData row = GenericRowData.of(geography);

        final BinaryRowData binaryRow = serializer.toBinaryRow(row, true);

        assertThat(binaryRow.getGeography(0).toBytes()).isEqualTo(POINT_WKB);
        assertThat(binaryRow.getGeography(0).subtypeId()).isEqualTo(GeographyData.POINT);
    }

    @Test
    void testRowDataSerializerPreservesNullGeography() {
        final RowDataSerializer serializer =
                InternalTypeInfo.<RowData>ofFields(new GeographyType()).toRowSerializer();
        final GenericRowData row = GenericRowData.of((Object) null);

        final BinaryRowData binaryRow = serializer.toBinaryRow(row, true);

        assertThat(binaryRow.isNullAt(0)).isTrue();
    }

    @Test
    void testArrayDataSerializerConvertsGenericArrayToBinaryArray() {
        final ArrayDataSerializer serializer = new ArrayDataSerializer(new GeographyType());
        final GenericArrayData array =
                new GenericArrayData(new Object[] {GeographyData.fromBytes(POINT_WKB), null});

        final BinaryArrayData binaryArray = serializer.toBinaryArray(array);

        assertThat(binaryArray.getGeography(0).toBytes()).isEqualTo(POINT_WKB);
        assertThat(binaryArray.getGeography(0).subtypeId()).isEqualTo(GeographyData.POINT);
        assertThat(binaryArray.isNullAt(1)).isTrue();
    }
}
