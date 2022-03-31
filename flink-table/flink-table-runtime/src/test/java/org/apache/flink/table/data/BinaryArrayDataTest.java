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

package org.apache.flink.table.data;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.utils.RawValueDataAsserter.equivalent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Test of {@link BinaryArrayData} and {@link BinaryArrayWriter}. */
public class BinaryArrayDataTest {

    @Test
    public void testArray() {
        // 1.array test
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 6);
        writer.setNullInt(1);
        writer.writeInt(2, 666);
        writer.complete();

        assertThat(6).isEqualTo(array.getInt(0));
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(666).isEqualTo(array.getInt(2));

        // 2.test write to binary row.
        {
            BinaryRowData row2 = new BinaryRowData(1);
            BinaryRowWriter writer2 = new BinaryRowWriter(row2);
            writer2.writeArray(0, array, new ArrayDataSerializer(DataTypes.INT().getLogicalType()));
            writer2.complete();

            BinaryArrayData array2 = (BinaryArrayData) row2.getArray(0);
            assertThat(array).isEqualTo(array2);
            assertThat(6).isEqualTo(array2.getInt(0));
            assertThat(array2.isNullAt(1)).isTrue();
            assertThat(666).isEqualTo(array2.getInt(2));
        }

        // 3.test write var seg array to binary row.
        {
            BinaryArrayData array3 = splitArray(array);

            BinaryRowData row2 = new BinaryRowData(1);
            BinaryRowWriter writer2 = new BinaryRowWriter(row2);
            writer2.writeArray(
                    0, array3, new ArrayDataSerializer(DataTypes.INT().getLogicalType()));
            writer2.complete();

            BinaryArrayData array2 = (BinaryArrayData) row2.getArray(0);
            assertThat(array).isEqualTo(array2);
            assertThat(6).isEqualTo(array2.getInt(0));
            assertThat(array2.isNullAt(1)).isTrue();
            assertThat(666).isEqualTo(array2.getInt(2));
        }
    }

    @Test
    public void testArrayTypes() {
        {
            // test bool
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);
            writer.setNullBoolean(0);
            writer.writeBoolean(1, true);
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getBoolean(1)).isTrue();
            array.setBoolean(0, true);
            assertThat(array.getBoolean(0)).isTrue();
            array.setNullBoolean(0);
            assertThat(array.isNullAt(0)).isTrue();

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getBoolean(1)).isTrue();
            newArray.setBoolean(0, true);
            assertThat(newArray.getBoolean(0)).isTrue();
            newArray.setNullBoolean(0);
            assertThat(newArray.isNullAt(0)).isTrue();

            newArray.setBoolean(0, true);
            assertThat(BinaryArrayData.fromPrimitiveArray(newArray.toBooleanArray()))
                    .isEqualTo(newArray);
        }

        {
            // test byte
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);
            writer.setNullByte(0);
            writer.writeByte(1, (byte) 25);
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getByte(1)).isEqualTo((byte) 25);
            array.setByte(0, (byte) 5);
            assertThat(array.getByte(0)).isEqualTo((byte) 5);
            array.setNullByte(0);
            assertThat(array.isNullAt(0)).isTrue();

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getByte(1)).isEqualTo((byte) 25);
            newArray.setByte(0, (byte) 5);
            assertThat(newArray.getByte(0)).isEqualTo((byte) 5);
            newArray.setNullByte(0);
            assertThat(newArray.isNullAt(0)).isTrue();

            newArray.setByte(0, (byte) 3);
            assertThat(BinaryArrayData.fromPrimitiveArray(newArray.toByteArray()))
                    .isEqualTo(newArray);
        }

        {
            // test short
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 2);
            writer.setNullShort(0);
            writer.writeShort(1, (short) 25);
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getShort(1)).isEqualTo((short) 25);
            array.setShort(0, (short) 5);
            assertThat(array.getShort(0)).isEqualTo((short) 5);
            array.setNullShort(0);
            assertThat(array.isNullAt(0)).isTrue();

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getShort(1)).isEqualTo((short) 25);
            newArray.setShort(0, (short) 5);
            assertThat(newArray.getShort(0)).isEqualTo((short) 5);
            newArray.setNullShort(0);
            assertThat(newArray.isNullAt(0)).isTrue();

            newArray.setShort(0, (short) 3);
            assertThat(BinaryArrayData.fromPrimitiveArray(newArray.toShortArray()))
                    .isEqualTo(newArray);
        }

        {
            // test int
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);
            writer.setNullInt(0);
            writer.writeInt(1, 25);
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getInt(1)).isEqualTo(25);
            array.setInt(0, 5);
            assertThat(array.getInt(0)).isEqualTo(5);
            array.setNullInt(0);
            assertThat(array.isNullAt(0)).isTrue();

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getInt(1)).isEqualTo(25);
            newArray.setInt(0, 5);
            assertThat(newArray.getInt(0)).isEqualTo(5);
            newArray.setNullInt(0);
            assertThat(newArray.isNullAt(0)).isTrue();

            newArray.setInt(0, 3);
            assertThat(BinaryArrayData.fromPrimitiveArray(newArray.toIntArray()))
                    .isEqualTo(newArray);
        }

        {
            // test long
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
            writer.setNullLong(0);
            writer.writeLong(1, 25);
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getLong(1)).isEqualTo(25);
            array.setLong(0, 5);
            assertThat(array.getLong(0)).isEqualTo(5);
            array.setNullLong(0);
            assertThat(array.isNullAt(0)).isTrue();

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getLong(1)).isEqualTo(25);
            newArray.setLong(0, 5);
            assertThat(newArray.getLong(0)).isEqualTo(5);
            newArray.setNullLong(0);
            assertThat(newArray.isNullAt(0)).isTrue();

            newArray.setLong(0, 3);
            assertThat(BinaryArrayData.fromPrimitiveArray(newArray.toLongArray()))
                    .isEqualTo(newArray);
        }

        {
            // test float
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);
            writer.setNullFloat(0);
            writer.writeFloat(1, 25);
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getFloat(1)).isEqualTo(25f);
            array.setFloat(0, 5);
            assertThat(array.getFloat(0)).isEqualTo(5f);
            array.setNullFloat(0);
            assertThat(array.isNullAt(0)).isTrue();

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getFloat(1)).isEqualTo(25f);
            newArray.setFloat(0, 5);
            assertThat(newArray.getFloat(0)).isEqualTo(5f);
            newArray.setNullFloat(0);
            assertThat(newArray.isNullAt(0)).isTrue();

            newArray.setFloat(0, 3);
            assertThat(BinaryArrayData.fromPrimitiveArray(newArray.toFloatArray()))
                    .isEqualTo(newArray);
        }

        {
            // test double
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
            writer.setNullDouble(0);
            writer.writeDouble(1, 25);
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getDouble(1)).isEqualTo(25d);
            array.setDouble(0, 5);
            assertThat(array.getDouble(0)).isEqualTo(5d);
            array.setNullDouble(0);
            assertThat(array.isNullAt(0)).isTrue();

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getDouble(1)).isEqualTo(25d);
            newArray.setDouble(0, 5);
            assertThat(newArray.getDouble(0)).isEqualTo(5d);
            newArray.setNullDouble(0);
            assertThat(newArray.isNullAt(0)).isTrue();

            newArray.setDouble(0, 3);
            assertThat(BinaryArrayData.fromPrimitiveArray(newArray.toDoubleArray()))
                    .isEqualTo(newArray);
        }

        {
            // test string
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
            writer.setNullAt(0);
            writer.writeString(1, fromString("jaja"));
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getString(1)).isEqualTo(fromString("jaja"));

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getString(1)).isEqualTo(fromString("jaja"));
        }

        BinaryArrayData subArray = new BinaryArrayData();
        BinaryArrayWriter subWriter = new BinaryArrayWriter(subArray, 2, 8);
        subWriter.setNullAt(0);
        subWriter.writeString(1, fromString("hehehe"));
        subWriter.complete();

        {
            // test array
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
            writer.setNullAt(0);
            writer.writeArray(
                    1, subArray, new ArrayDataSerializer(DataTypes.INT().getLogicalType()));
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getArray(1)).isEqualTo(subArray);

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getArray(1)).isEqualTo(subArray);
        }

        {
            // test map
            BinaryArrayData array = new BinaryArrayData();
            BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
            writer.setNullAt(0);
            writer.writeMap(
                    1,
                    BinaryMapData.valueOf(subArray, subArray),
                    new MapDataSerializer(
                            DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType()));
            writer.complete();

            assertThat(array.isNullAt(0)).isTrue();
            assertThat(array.getMap(1)).isEqualTo(BinaryMapData.valueOf(subArray, subArray));

            BinaryArrayData newArray = splitArray(array);
            assertThat(newArray.isNullAt(0)).isTrue();
            assertThat(newArray.getMap(1)).isEqualTo(BinaryMapData.valueOf(subArray, subArray));
        }
    }

    @Test
    public void testMap() {
        BinaryArrayData array1 = new BinaryArrayData();
        BinaryArrayWriter writer1 = new BinaryArrayWriter(array1, 3, 4);
        writer1.writeInt(0, 6);
        writer1.writeInt(1, 5);
        writer1.writeInt(2, 666);
        writer1.complete();

        BinaryArrayData array2 = new BinaryArrayData();
        BinaryArrayWriter writer2 = new BinaryArrayWriter(array2, 3, 8);
        writer2.writeString(0, fromString("6"));
        writer2.writeString(1, fromString("5"));
        writer2.writeString(2, fromString("666"));
        writer2.complete();

        BinaryMapData binaryMap = BinaryMapData.valueOf(array1, array2);

        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter rowWriter = new BinaryRowWriter(row);
        rowWriter.writeMap(
                0,
                binaryMap,
                new MapDataSerializer(
                        DataTypes.INT().getLogicalType(), DataTypes.INT().getLogicalType()));
        rowWriter.complete();

        BinaryMapData map = (BinaryMapData) row.getMap(0);
        BinaryArrayData key = map.keyArray();
        BinaryArrayData value = map.valueArray();

        assertThat(map).isEqualTo(binaryMap);
        assertThat(key).isEqualTo(array1);
        assertThat(value).isEqualTo(array2);

        assertThat(5).isEqualTo(key.getInt(1));
        assertThat(fromString("5")).isEqualTo(value.getString(1));
    }

    private static BinaryArrayData splitArray(BinaryArrayData array) {
        BinaryArrayData ret = new BinaryArrayData();
        MemorySegment[] segments =
                splitBytes(
                        BinarySegmentUtils.copyToBytes(
                                array.getSegments(), 0, array.getSizeInBytes()),
                        0);
        ret.pointTo(segments, 0, array.getSizeInBytes());
        return ret;
    }

    private static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
        int newSize = (bytes.length + 1) / 2 + baseOffset;
        MemorySegment[] ret = new MemorySegment[2];
        ret[0] = MemorySegmentFactory.wrap(new byte[newSize]);
        ret[1] = MemorySegmentFactory.wrap(new byte[newSize]);

        ret[0].put(baseOffset, bytes, 0, newSize - baseOffset);
        ret[1].put(0, bytes, newSize - baseOffset, bytes.length - (newSize - baseOffset));
        return ret;
    }

    @Test
    public void testToArray() {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 2);
        writer.writeShort(0, (short) 5);
        writer.writeShort(1, (short) 10);
        writer.writeShort(2, (short) 15);
        writer.complete();

        short[] shorts = array.toShortArray();
        assertThat(shorts[0]).isEqualTo((short) 5);
        assertThat(shorts[1]).isEqualTo((short) 10);
        assertThat(shorts[2]).isEqualTo((short) 15);

        MemorySegment[] segments = splitBytes(writer.getSegments().getArray(), 3);
        array.pointTo(segments, 3, array.getSizeInBytes());
        assertThat(array.getShort(0)).isEqualTo((short) 5);
        assertThat(array.getShort(1)).isEqualTo((short) 10);
        assertThat(array.getShort(2)).isEqualTo((short) 15);
        short[] shorts2 = array.toShortArray();
        assertThat(shorts2[0]).isEqualTo((short) 5);
        assertThat(shorts2[1]).isEqualTo((short) 10);
        assertThat(shorts2[2]).isEqualTo((short) 15);
    }

    @Test
    public void testDecimal() {

        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        // 1.compact
        {
            int precision = 4;
            int scale = 2;
            writer.reset();
            writer.writeDecimal(0, DecimalData.fromUnscaledLong(5, precision, scale), precision);
            writer.setNullAt(1);
            writer.complete();

            assertThat(array.getDecimal(0, precision, scale).toString()).isEqualTo("0.05");
            assertThat(array.isNullAt(1)).isTrue();
            array.setDecimal(0, DecimalData.fromUnscaledLong(6, precision, scale), precision);
            assertThat(array.getDecimal(0, precision, scale).toString()).isEqualTo("0.06");
        }

        // 2.not compact
        {
            int precision = 25;
            int scale = 5;
            DecimalData decimal1 =
                    DecimalData.fromBigDecimal(BigDecimal.valueOf(5.55), precision, scale);
            DecimalData decimal2 =
                    DecimalData.fromBigDecimal(BigDecimal.valueOf(6.55), precision, scale);

            writer.reset();
            writer.writeDecimal(0, decimal1, precision);
            writer.writeDecimal(1, null, precision);
            writer.complete();

            assertThat(array.getDecimal(0, precision, scale).toString()).isEqualTo("5.55000");
            assertThat(array.isNullAt(1)).isTrue();
            array.setDecimal(0, decimal2, precision);
            assertThat(array.getDecimal(0, precision, scale).toString()).isEqualTo("6.55000");
        }
    }

    @Test
    public void testGeneric() {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
        RawValueData<String> generic = RawValueData.fromObject("hahah");
        RawValueDataSerializer<String> serializer =
                new RawValueDataSerializer<>(StringSerializer.INSTANCE);
        writer.writeRawValue(0, generic, serializer);
        writer.setNullAt(1);
        writer.complete();

        RawValueData<String> newGeneric = array.getRawValue(0);
        assertThat(newGeneric).satisfies(matching(equivalent(generic, serializer)));
        assertThat(array.isNullAt(1)).isTrue();
    }

    @Test
    public void testNested() {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
        writer.writeRow(
                0,
                GenericRowData.of(fromString("1"), 1),
                new RowDataSerializer(RowType.of(VarCharType.STRING_TYPE, new IntType())));
        writer.setNullAt(1);
        writer.complete();

        RowData nestedRow = array.getRow(0, 2);
        assertThat(nestedRow.getString(0).toString()).isEqualTo("1");
        assertThat(nestedRow.getInt(1)).isEqualTo(1);
        assertThat(array.isNullAt(1)).isTrue();
    }

    @Test
    public void testBinary() {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
        byte[] bytes1 = new byte[] {1, -1, 5};
        byte[] bytes2 = new byte[] {1, -1, 5, 5, 1, 5, 1, 5};
        writer.writeBinary(0, bytes1);
        writer.writeBinary(1, bytes2);
        writer.complete();

        assertThat(array.getBinary(0)).isEqualTo(bytes1);
        assertThat(array.getBinary(1)).isEqualTo(bytes2);
    }

    @Test
    public void testTimestampData() {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        // 1. compact
        {
            final int precision = 3;
            writer.reset();
            writer.writeTimestamp(0, TimestampData.fromEpochMillis(123L), precision);
            writer.setNullAt(1);
            writer.complete();

            assertThat(array.getTimestamp(0, 3).toString()).isEqualTo("1970-01-01T00:00:00.123");
            assertThat(array.isNullAt(1)).isTrue();
            array.setTimestamp(0, TimestampData.fromEpochMillis(-123L), precision);
            assertThat(array.getTimestamp(0, 3).toString()).isEqualTo("1969-12-31T23:59:59.877");
        }

        // 2. not compact
        {
            final int precision = 9;
            TimestampData timestamp1 =
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123456789));
            TimestampData timestamp2 =
                    TimestampData.fromTimestamp(Timestamp.valueOf("1969-01-01 00:00:00.123456789"));

            writer.reset();
            writer.writeTimestamp(0, timestamp1, precision);
            writer.writeTimestamp(1, null, precision);
            writer.complete();

            assertThat(array.getTimestamp(0, precision).toString())
                    .isEqualTo("1970-01-01T00:00:00.123456789");
            assertThat(array.isNullAt(1)).isTrue();
            array.setTimestamp(0, timestamp2, precision);
            assertThat(array.getTimestamp(0, precision).toString())
                    .isEqualTo("1969-01-01T00:00:00.123456789");
        }
    }
}
