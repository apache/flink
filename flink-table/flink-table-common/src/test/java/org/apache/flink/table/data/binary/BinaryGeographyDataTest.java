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

package org.apache.flink.table.data.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.GeographyType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BinaryGeographyData}. */
class BinaryGeographyDataTest {

    private static final int BIG_ENDIAN = 0;
    private static final int LITTLE_ENDIAN = 1;
    private static final byte[] POINT_WKB = pointWkb(LITTLE_ENDIAN);

    @Test
    void testValidWkbRoundTrip() {
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);

        assertThat(geography).isInstanceOf(BinaryGeographyData.class);
        assertThat(geography.toBytes()).isEqualTo(POINT_WKB);
        assertThat(geography.subtypeId()).isEqualTo(GeographyData.POINT);
        assertThat(geography.sizeInBytes()).isEqualTo(POINT_WKB.length);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("supportedSubtypePayloads")
    void testSupported2dSubtypePayloads(String name, int subtypeId, byte[] wkb) {
        final BinaryGeographyData geography = BinaryGeographyData.fromBytes(wkb);

        assertThat(geography.subtypeId()).isEqualTo(subtypeId);
        assertThat(geography.toBytes()).isEqualTo(wkb);
        assertThat(geography.sizeInBytes()).isEqualTo(wkb.length);
    }

    static Stream<Arguments> supportedSubtypePayloads() {
        return Stream.of(
                Arguments.of("Point", GeographyData.POINT, pointWkb(LITTLE_ENDIAN)),
                Arguments.of("LineString", GeographyData.LINE_STRING, lineStringWkb(LITTLE_ENDIAN)),
                Arguments.of("Polygon", GeographyData.POLYGON, polygonWkb(LITTLE_ENDIAN)),
                Arguments.of("MultiPoint", GeographyData.MULTI_POINT, multiPointWkb(LITTLE_ENDIAN)),
                Arguments.of(
                        "MultiLineString",
                        GeographyData.MULTI_LINE_STRING,
                        multiLineStringWkb(LITTLE_ENDIAN)),
                Arguments.of(
                        "MultiPolygon",
                        GeographyData.MULTI_POLYGON,
                        multiPolygonWkb(LITTLE_ENDIAN)),
                Arguments.of(
                        "GeometryCollection",
                        GeographyData.GEOMETRY_COLLECTION,
                        geometryCollectionWkb(LITTLE_ENDIAN)));
    }

    @Test
    void testBigEndianSubtypeExtraction() {
        final BinaryGeographyData point = BinaryGeographyData.fromBytes(pointWkb(BIG_ENDIAN));
        final BinaryGeographyData lineString =
                BinaryGeographyData.fromBytes(lineStringWkb(BIG_ENDIAN));

        assertThat(point.subtypeId()).isEqualTo(GeographyData.POINT);
        assertThat(lineString.subtypeId()).isEqualTo(GeographyData.LINE_STRING);
    }

    @Test
    void testNullHandling() {
        assertThat(GeographyData.fromBytes((byte[]) null)).isNull();
        assertThat(GeographyData.fromBytes(null, 0, 0)).isNull();
        assertThat(BinaryGeographyData.fromBytes((byte[]) null)).isNull();
        assertThat(BinaryGeographyData.fromBytes(null, 0, 0)).isNull();
    }

    @Test
    void testGenericRowDataNullPath() {
        final RowData.FieldGetter getter = RowData.createFieldGetter(new GeographyType(), 0);
        final GenericRowData nullRow = GenericRowData.of((Object) null);
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);
        final GenericRowData valueRow = GenericRowData.of(geography);

        assertThat(getter.getFieldOrNull(nullRow)).isNull();
        assertThat(getter.getFieldOrNull(valueRow)).isSameAs(geography);
    }

    @Test
    void testBinaryRowDataNullPath() {
        final RowData.FieldGetter getter = RowData.createFieldGetter(new GeographyType(), 0);
        final int fixedLength = BinaryRowData.calculateFixPartSizeInBytes(1);
        final BinaryRowData nullRow = new BinaryRowData(1);
        nullRow.pointTo(MemorySegmentFactory.wrap(new byte[fixedLength]), 0, fixedLength);
        nullRow.setNullAt(0);

        assertThat(getter.getFieldOrNull(nullRow)).isNull();
    }

    @Test
    void testBinaryRowDataValuePath() {
        final BinaryRowData row = binaryRowWithGeography(POINT_WKB);

        assertThat(row.getGeography(0).toBytes()).isEqualTo(POINT_WKB);
        assertThat(row.getGeography(0).subtypeId()).isEqualTo(GeographyData.POINT);
    }

    @Test
    void testByteRangeCopiesOnlySelectedPayload() {
        final byte[] bytes = concat(bytes(42), POINT_WKB, bytes(99));
        final BinaryGeographyData geography =
                BinaryGeographyData.fromBytes(bytes, 1, POINT_WKB.length);

        bytes[1] = 0;

        assertThat(geography.toBytes()).isEqualTo(POINT_WKB);
        assertThat(geography.subtypeId()).isEqualTo(GeographyData.POINT);
    }

    @Test
    void testFromAddressSupportsSegmentBoundary() {
        final byte[] first = concat(new byte[14], Arrays.copyOfRange(POINT_WKB, 0, 2));
        final byte[] second = Arrays.copyOfRange(POINT_WKB, 2, 18);
        final byte[] third =
                concat(Arrays.copyOfRange(POINT_WKB, 18, POINT_WKB.length), new byte[13]);
        final MemorySegment[] segments =
                new MemorySegment[] {
                    MemorySegmentFactory.wrap(first),
                    MemorySegmentFactory.wrap(second),
                    MemorySegmentFactory.wrap(third)
                };

        final BinaryGeographyData geography =
                BinaryGeographyData.fromAddress(segments, 14, POINT_WKB.length);

        assertThat(geography.toBytes()).isEqualTo(POINT_WKB);
        assertThat(geography.subtypeId()).isEqualTo(GeographyData.POINT);
    }

    @Test
    void testMalformedPayloadBoundaries() {
        assertThatThrownBy(() -> BinaryGeographyData.fromBytes(bytes()))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Incomplete WKB header");

        assertThatThrownBy(() -> BinaryGeographyData.fromBytes(bytes(1, 1, 0, 0)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Incomplete WKB header");

        assertThatThrownBy(() -> BinaryGeographyData.fromBytes(bytes(2, 1, 0, 0, 0)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Unsupported byte order 2");

        assertThatThrownBy(() -> BinaryGeographyData.fromBytes(header(LITTLE_ENDIAN, 0)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Unsupported geography subtype ID 0");

        assertThatThrownBy(() -> BinaryGeographyData.fromBytes(header(LITTLE_ENDIAN, 8)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Unsupported geography subtype ID 8");
    }

    @Test
    void testStructurallyIncompletePayloads() {
        assertThatThrownBy(
                        () ->
                                BinaryGeographyData.fromBytes(
                                        header(LITTLE_ENDIAN, GeographyData.POINT)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Incomplete POINT coordinates");

        assertThatThrownBy(
                        () ->
                                BinaryGeographyData.fromBytes(
                                        concat(
                                                header(LITTLE_ENDIAN, GeographyData.LINE_STRING),
                                                unsignedInt(LITTLE_ENDIAN, 1),
                                                new byte[15])))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Incomplete coordinate sequence");

        assertThatThrownBy(
                        () ->
                                BinaryGeographyData.fromBytes(
                                        concat(
                                                header(LITTLE_ENDIAN, GeographyData.MULTI_POINT),
                                                unsignedInt(LITTLE_ENDIAN, 1),
                                                lineStringWkb(LITTLE_ENDIAN))))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Expected nested subtype ID 1 but found 2");

        assertThatThrownBy(
                        () ->
                                BinaryGeographyData.fromBytes(
                                        concat(pointWkb(LITTLE_ENDIAN), bytes(42))))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("trailing byte");
    }

    @Test
    void testInvalidByteRanges() {
        assertThatThrownBy(() -> BinaryGeographyData.fromBytes(POINT_WKB, -1, POINT_WKB.length))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Invalid ISO WKB byte range");

        assertThatThrownBy(() -> BinaryGeographyData.fromBytes(POINT_WKB, 0, POINT_WKB.length + 1))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Invalid ISO WKB byte range");
    }

    private static BinaryRowData binaryRowWithGeography(byte[] wkb) {
        final int fixedLength = BinaryRowData.calculateFixPartSizeInBytes(1);
        final byte[] rowBytes = new byte[fixedLength + wkb.length];
        final MemorySegment segment = MemorySegmentFactory.wrap(rowBytes);
        segment.putLong(8, ((long) fixedLength << 32) | wkb.length);
        segment.put(fixedLength, wkb, 0, wkb.length);

        final BinaryRowData row = new BinaryRowData(1);
        row.pointTo(segment, 0, rowBytes.length);
        return row;
    }

    private static byte[] pointWkb(int byteOrder) {
        return concat(header(byteOrder, GeographyData.POINT), new byte[16]);
    }

    private static byte[] lineStringWkb(int byteOrder) {
        return concat(
                header(byteOrder, GeographyData.LINE_STRING),
                unsignedInt(byteOrder, 2),
                new byte[32]);
    }

    private static byte[] polygonWkb(int byteOrder) {
        return concat(
                header(byteOrder, GeographyData.POLYGON),
                unsignedInt(byteOrder, 1),
                unsignedInt(byteOrder, 4),
                new byte[64]);
    }

    private static byte[] multiPointWkb(int byteOrder) {
        return concat(
                header(byteOrder, GeographyData.MULTI_POINT),
                unsignedInt(byteOrder, 1),
                pointWkb(byteOrder));
    }

    private static byte[] multiLineStringWkb(int byteOrder) {
        return concat(
                header(byteOrder, GeographyData.MULTI_LINE_STRING),
                unsignedInt(byteOrder, 1),
                lineStringWkb(byteOrder));
    }

    private static byte[] multiPolygonWkb(int byteOrder) {
        return concat(
                header(byteOrder, GeographyData.MULTI_POLYGON),
                unsignedInt(byteOrder, 1),
                polygonWkb(byteOrder));
    }

    private static byte[] geometryCollectionWkb(int byteOrder) {
        return concat(
                header(byteOrder, GeographyData.GEOMETRY_COLLECTION),
                unsignedInt(byteOrder, 2),
                pointWkb(byteOrder),
                lineStringWkb(byteOrder));
    }

    private static byte[] header(int byteOrder, int subtypeId) {
        return concat(bytes(byteOrder), unsignedInt(byteOrder, subtypeId));
    }

    private static byte[] unsignedInt(int byteOrder, int value) {
        if (byteOrder == LITTLE_ENDIAN) {
            return bytes(value, value >>> 8, value >>> 16, value >>> 24);
        }
        return bytes(value >>> 24, value >>> 16, value >>> 8, value);
    }

    private static byte[] concat(byte[]... values) {
        int length = 0;
        for (byte[] value : values) {
            length += value.length;
        }

        final byte[] result = new byte[length];
        int offset = 0;
        for (byte[] value : values) {
            System.arraycopy(value, 0, result, offset, value.length);
            offset += value.length;
        }
        return result;
    }

    private static byte[] bytes(int... values) {
        final byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }
}
