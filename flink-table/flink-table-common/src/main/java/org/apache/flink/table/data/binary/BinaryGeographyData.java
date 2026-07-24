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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GeographyData;

import java.util.Arrays;

/**
 * A binary implementation of {@link GeographyData} backed by raw ISO WKB bytes.
 *
 * <p>GEOGRAPHY uses OGC:CRS84 by contract, but ISO WKB does not encode CRS or SRID metadata. This
 * container stores the raw ISO WKB payload only; CRS validation, CRS transformation, and EWKB/SRID
 * handling belong to constructors, functions, and connector schema mapping.
 */
@Internal
public final class BinaryGeographyData extends BinarySection implements GeographyData {

    private static final int MIN_WKB_HEADER_SIZE = 5;
    private static final int WKB_COUNT_SIZE = 4;
    private static final int WKB_POINT_COORDINATE_SIZE = 16;
    private static final int BIG_ENDIAN = 0;
    private static final int LITTLE_ENDIAN = 1;

    private final int subtypeId;

    private BinaryGeographyData(MemorySegment[] segments, int offset, int sizeInBytes) {
        super(segments, offset, sizeInBytes);
        this.subtypeId = readSubtypeId(segments, offset, sizeInBytes);
    }

    /** Creates a {@link BinaryGeographyData} instance from the given address and length. */
    public static BinaryGeographyData fromAddress(
            MemorySegment[] segments, int offset, int numBytes) {
        return new BinaryGeographyData(segments, offset, numBytes);
    }

    /** Creates a {@link BinaryGeographyData} instance from the given ISO WKB bytes. */
    public static BinaryGeographyData fromBytes(byte[] bytes) {
        return bytes == null ? null : fromBytes(bytes, 0, bytes.length);
    }

    /** Creates a {@link BinaryGeographyData} instance from the given ISO WKB byte range. */
    public static BinaryGeographyData fromBytes(byte[] bytes, int offset, int numBytes) {
        if (bytes == null) {
            return null;
        }
        checkRange(bytes, offset, numBytes);
        byte[] copy = Arrays.copyOfRange(bytes, offset, offset + numBytes);
        return fromAddress(new MemorySegment[] {MemorySegmentFactory.wrap(copy)}, 0, copy.length);
    }

    @Override
    public byte[] toBytes() {
        return BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
    }

    @Override
    public int subtypeId() {
        return subtypeId;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    private static void checkRange(byte[] bytes, int offset, int numBytes) {
        if (offset < 0 || numBytes < 0 || offset > bytes.length - numBytes) {
            throw new TableRuntimeException(
                    String.format(
                            "Invalid ISO WKB byte range: offset %d, length %d, array length %d.",
                            offset, numBytes, bytes.length));
        }
    }

    private static int readSubtypeId(MemorySegment[] segments, int offset, int sizeInBytes) {
        final long endOffset = (long) offset + sizeInBytes;
        final GeometryHeader header = readHeader(segments, offset, endOffset);
        final long consumedOffset = validateGeometry(segments, offset, endOffset);
        if (consumedOffset != endOffset) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Found %d trailing byte(s).",
                            endOffset - consumedOffset));
        }
        return header.subtypeId;
    }

    private static long validateGeometry(
            MemorySegment[] segments, long geometryOffset, long endOffset) {
        final GeometryHeader header = readHeader(segments, geometryOffset, endOffset);
        long cursor = geometryOffset + MIN_WKB_HEADER_SIZE;

        switch (header.subtypeId) {
            case GeographyData.POINT:
                return requireBytes(
                                cursor, WKB_POINT_COORDINATE_SIZE, endOffset, "POINT coordinates")
                        + WKB_POINT_COORDINATE_SIZE;
            case GeographyData.LINE_STRING:
                return skipCoordinateSequence(segments, cursor, endOffset, header.byteOrder);
            case GeographyData.POLYGON:
                return skipPolygon(segments, cursor, endOffset, header.byteOrder);
            case GeographyData.MULTI_POINT:
                return skipTypedGeometryCollection(
                        segments, cursor, endOffset, header.byteOrder, GeographyData.POINT);
            case GeographyData.MULTI_LINE_STRING:
                return skipTypedGeometryCollection(
                        segments, cursor, endOffset, header.byteOrder, GeographyData.LINE_STRING);
            case GeographyData.MULTI_POLYGON:
                return skipTypedGeometryCollection(
                        segments, cursor, endOffset, header.byteOrder, GeographyData.POLYGON);
            case GeographyData.GEOMETRY_COLLECTION:
                return skipGeometryCollection(segments, cursor, endOffset, header.byteOrder);
            default:
                throw new TableRuntimeException(
                        String.format(
                                "Malformed ISO WKB payload. Unsupported geography subtype ID %d.",
                                header.subtypeId));
        }
    }

    private static long skipCoordinateSequence(
            MemorySegment[] segments, long offset, long endOffset, int byteOrder) {
        final long numPoints =
                readUnsignedInt(segments, offset, endOffset, byteOrder, "point count");
        long cursor = offset + WKB_COUNT_SIZE;
        final long coordinateSequenceSize =
                multiplyExact(numPoints, WKB_POINT_COORDINATE_SIZE, "coordinate sequence");
        return requireBytes(cursor, coordinateSequenceSize, endOffset, "coordinate sequence")
                + coordinateSequenceSize;
    }

    private static long skipPolygon(
            MemorySegment[] segments, long offset, long endOffset, int byteOrder) {
        final long numRings = readUnsignedInt(segments, offset, endOffset, byteOrder, "ring count");
        long cursor = offset + WKB_COUNT_SIZE;
        for (long i = 0; i < numRings; i++) {
            cursor = skipCoordinateSequence(segments, cursor, endOffset, byteOrder);
        }
        return cursor;
    }

    private static long skipTypedGeometryCollection(
            MemorySegment[] segments,
            long offset,
            long endOffset,
            int byteOrder,
            int expectedSubtypeId) {
        final long numGeometries =
                readUnsignedInt(segments, offset, endOffset, byteOrder, "geometry count");
        long cursor = offset + WKB_COUNT_SIZE;
        for (long i = 0; i < numGeometries; i++) {
            final GeometryHeader nestedHeader = readHeader(segments, cursor, endOffset);
            if (nestedHeader.subtypeId != expectedSubtypeId) {
                throw new TableRuntimeException(
                        String.format(
                                "Malformed ISO WKB payload. Expected nested subtype ID %d but found %d.",
                                expectedSubtypeId, nestedHeader.subtypeId));
            }
            cursor = validateGeometry(segments, cursor, endOffset);
        }
        return cursor;
    }

    private static long skipGeometryCollection(
            MemorySegment[] segments, long offset, long endOffset, int byteOrder) {
        final long numGeometries =
                readUnsignedInt(segments, offset, endOffset, byteOrder, "geometry count");
        long cursor = offset + WKB_COUNT_SIZE;
        for (long i = 0; i < numGeometries; i++) {
            cursor = validateGeometry(segments, cursor, endOffset);
        }
        return cursor;
    }

    private static GeometryHeader readHeader(
            MemorySegment[] segments, long offset, long endOffset) {
        requireBytes(offset, MIN_WKB_HEADER_SIZE, endOffset, "WKB header");

        final int byteOrder = BinarySegmentUtils.getByte(segments, (int) offset) & 0xFF;
        if (byteOrder != BIG_ENDIAN && byteOrder != LITTLE_ENDIAN) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Unsupported byte order %d.", byteOrder));
        }

        final long subtypeId =
                readUnsignedInt(segments, offset + 1, endOffset, byteOrder, "subtype ID");

        if (subtypeId < GeographyData.POINT || subtypeId > GeographyData.GEOMETRY_COLLECTION) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Unsupported geography subtype ID %d.",
                            subtypeId));
        }
        return new GeometryHeader(byteOrder, (int) subtypeId);
    }

    private static long readUnsignedInt(
            MemorySegment[] segments,
            long offset,
            long endOffset,
            int byteOrder,
            String fieldName) {
        requireBytes(offset, WKB_COUNT_SIZE, endOffset, fieldName);

        if (byteOrder == LITTLE_ENDIAN) {
            return (BinarySegmentUtils.getByte(segments, (int) offset) & 0xFFL)
                    | ((BinarySegmentUtils.getByte(segments, (int) offset + 1) & 0xFFL) << 8)
                    | ((BinarySegmentUtils.getByte(segments, (int) offset + 2) & 0xFFL) << 16)
                    | ((BinarySegmentUtils.getByte(segments, (int) offset + 3) & 0xFFL) << 24);
        }
        return ((BinarySegmentUtils.getByte(segments, (int) offset) & 0xFFL) << 24)
                | ((BinarySegmentUtils.getByte(segments, (int) offset + 1) & 0xFFL) << 16)
                | ((BinarySegmentUtils.getByte(segments, (int) offset + 2) & 0xFFL) << 8)
                | (BinarySegmentUtils.getByte(segments, (int) offset + 3) & 0xFFL);
    }

    private static long requireBytes(
            long offset, long numBytes, long endOffset, String componentName) {
        if (offset > endOffset || endOffset - offset < numBytes) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Incomplete %s: expected %d byte(s) but found %d.",
                            componentName, numBytes, Math.max(0, endOffset - offset)));
        }
        return offset;
    }

    private static long multiplyExact(long value, int factor, String componentName) {
        final long result = value * factor;
        if (value != 0 && result / value != factor) {
            throw new TableRuntimeException(
                    String.format("Malformed ISO WKB payload. %s size overflows.", componentName));
        }
        return result;
    }

    private static final class GeometryHeader {
        private final int byteOrder;
        private final int subtypeId;

        private GeometryHeader(int byteOrder, int subtypeId) {
            this.byteOrder = byteOrder;
            this.subtypeId = subtypeId;
        }
    }
}
