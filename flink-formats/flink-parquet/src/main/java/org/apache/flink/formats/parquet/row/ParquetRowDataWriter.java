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

package org.apache.flink.formats.parquet.row;

import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.computeMinBytesForDecimalPrecision;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.JULIAN_EPOCH_OFFSET_DAYS;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.MILLIS_IN_DAY;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.NANOS_PER_MILLISECOND;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.NANOS_PER_SECOND;

/** Writes a record to the Parquet API with the expected schema in order to be written to a file. */
public class ParquetRowDataWriter {

    private final RowWriter rowWriter;
    private final RecordConsumer recordConsumer;
    private final boolean utcTimestamp;

    public ParquetRowDataWriter(
            RecordConsumer recordConsumer,
            RowType rowType,
            GroupType schema,
            boolean utcTimestamp) {
        this.recordConsumer = recordConsumer;
        this.utcTimestamp = utcTimestamp;

        rowWriter = new RowWriter(rowType, schema);
    }

    /**
     * It writes a record to Parquet.
     *
     * @param record Contains the record that is going to be written.
     */
    public void write(final RowData record) {
        recordConsumer.startMessage();
        rowWriter.write(record);
        recordConsumer.endMessage();
    }

    private FieldWriter createWriter(LogicalType t, Type type) {
        if (type.isPrimitive()) {
            switch (t.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return new StringWriter();
                case BOOLEAN:
                    return new BooleanWriter();
                case BINARY:
                case VARBINARY:
                    return new BinaryWriter();
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) t;
                    return createDecimalWriter(decimalType.getPrecision(), decimalType.getScale());
                case TINYINT:
                    return new ByteWriter();
                case SMALLINT:
                    return new ShortWriter();
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTEGER:
                    return new IntWriter();
                case BIGINT:
                    return new LongWriter();
                case FLOAT:
                    return new FloatWriter();
                case DOUBLE:
                    return new DoubleWriter();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    TimestampType timestampType = (TimestampType) t;
                    return new TimestampWriter(timestampType.getPrecision());
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) t;
                    return new TimestampWriter(localZonedTimestampType.getPrecision());
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        } else {
            GroupType groupType = type.asGroupType();
            LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();

            if (t instanceof ArrayType
                    && logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                return new ArrayWriter(((ArrayType) t).getElementType(), groupType);
            } else if (t instanceof MapType
                    && logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                return new MapWriter(
                        ((MapType) t).getKeyType(), ((MapType) t).getValueType(), groupType);
            } else if (t instanceof MultisetType
                    && logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                return new MapWriter(
                        ((MultisetType) t).getElementType(), new IntType(false), groupType);
            } else if (t instanceof RowType && type instanceof GroupType) {
                return new RowWriter((RowType) t, groupType);
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        }
    }

    private interface FieldWriter {

        void write(RowData row, int ordinal);

        void write(ArrayData arrayData, int ordinal);
    }

    private class BooleanWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeBoolean(row.getBoolean(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeBoolean(arrayData.getBoolean(ordinal));
        }

        private void writeBoolean(boolean value) {
            recordConsumer.addBoolean(value);
        }
    }

    private class ByteWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeByte(row.getByte(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeByte(arrayData.getByte(ordinal));
        }

        private void writeByte(byte value) {
            recordConsumer.addInteger(value);
        }
    }

    private class ShortWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeShort(row.getShort(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeShort(arrayData.getShort(ordinal));
        }

        private void writeShort(short value) {
            recordConsumer.addInteger(value);
        }
    }

    private class LongWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeLong(row.getLong(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeLong(arrayData.getLong(ordinal));
        }

        private void writeLong(long value) {
            recordConsumer.addLong(value);
        }
    }

    private class FloatWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeFloat(row.getFloat(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeFloat(arrayData.getFloat(ordinal));
        }

        private void writeFloat(float value) {
            recordConsumer.addFloat(value);
        }
    }

    private class DoubleWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeDouble(row.getDouble(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeDouble(arrayData.getDouble(ordinal));
        }

        private void writeDouble(double value) {
            recordConsumer.addDouble(value);
        }
    }

    private class StringWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeString(row.getString(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeString(arrayData.getString(ordinal));
        }

        private void writeString(StringData value) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(value.toBytes()));
        }
    }

    private class BinaryWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeBinary(row.getBinary(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeBinary(arrayData.getBinary(ordinal));
        }

        private void writeBinary(byte[] value) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(value));
        }
    }

    private class IntWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            writeInt(row.getInt(ordinal));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeInt(arrayData.getInt(ordinal));
        }

        private void writeInt(int value) {
            recordConsumer.addInteger(value);
        }
    }

    /**
     * We only support INT96 bytes now, julianDay(4) + nanosOfDay(8). See
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
     * TIMESTAMP_MILLIS and TIMESTAMP_MICROS are the deprecated ConvertedType.
     */
    private class TimestampWriter implements FieldWriter {

        private final int precision;

        private TimestampWriter(int precision) {
            this.precision = precision;
        }

        @Override
        public void write(RowData row, int ordinal) {
            writeTimestamp(row.getTimestamp(ordinal, precision));
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {
            writeTimestamp(arrayData.getTimestamp(ordinal, precision));
        }

        private void writeTimestamp(TimestampData value) {
            recordConsumer.addBinary(timestampToInt96(value));
        }
    }

    /** It writes a map field to parquet, both key and value are nullable. */
    private class MapWriter implements FieldWriter {

        private String repeatedGroupName;
        private String keyName, valueName;
        private FieldWriter keyWriter, valueWriter;

        private MapWriter(LogicalType keyType, LogicalType valueType, GroupType groupType) {
            // Get the internal map structure (MAP_KEY_VALUE)
            GroupType repeatedType = groupType.getType(0).asGroupType();
            this.repeatedGroupName = repeatedType.getName();

            // Get key element information
            Type type = repeatedType.getType(0);
            this.keyName = type.getName();
            this.keyWriter = createWriter(keyType, type);

            // Get value element information
            Type valuetype = repeatedType.getType(1);
            this.valueName = valuetype.getName();
            this.valueWriter = createWriter(valueType, valuetype);
        }

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.startGroup();

            MapData mapData = row.getMap(ordinal);

            if (mapData != null && mapData.size() > 0) {
                recordConsumer.startField(repeatedGroupName, 0);

                ArrayData keyArray = mapData.keyArray();
                ArrayData valueArray = mapData.valueArray();
                for (int i = 0; i < keyArray.size(); i++) {
                    recordConsumer.startGroup();
                    if (!keyArray.isNullAt(i)) {
                        // write key element
                        recordConsumer.startField(keyName, 0);
                        keyWriter.write(keyArray, i);
                        recordConsumer.endField(keyName, 0);
                    }

                    if (!valueArray.isNullAt(i)) {
                        // write value element
                        recordConsumer.startField(valueName, 1);
                        valueWriter.write(valueArray, i);
                        recordConsumer.endField(valueName, 1);
                    }
                    recordConsumer.endGroup();
                }

                recordConsumer.endField(repeatedGroupName, 0);
            }
            recordConsumer.endGroup();
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {}
    }

    /** It writes an array type field to parquet. */
    private class ArrayWriter implements FieldWriter {

        private String elementName;
        private FieldWriter elementWriter;
        private String repeatedGroupName;

        private ArrayWriter(LogicalType t, GroupType groupType) {

            // Get the internal array structure
            GroupType repeatedType = groupType.getType(0).asGroupType();
            this.repeatedGroupName = repeatedType.getName();

            Type elementType = repeatedType.getType(0);
            this.elementName = elementType.getName();

            this.elementWriter = createWriter(t, elementType);
        }

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.startGroup();
            ArrayData arrayData = row.getArray(ordinal);
            int listLength = arrayData.size();

            if (listLength > 0) {
                recordConsumer.startField(repeatedGroupName, 0);
                for (int i = 0; i < listLength; i++) {
                    recordConsumer.startGroup();
                    if (!arrayData.isNullAt(i)) {
                        recordConsumer.startField(elementName, 0);
                        elementWriter.write(arrayData, i);
                        recordConsumer.endField(elementName, 0);
                    }
                    recordConsumer.endGroup();
                }

                recordConsumer.endField(repeatedGroupName, 0);
            }
            recordConsumer.endGroup();
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {}
    }

    /** It writes a row type field to parquet. */
    private class RowWriter implements FieldWriter {
        private List<LogicalType> logicalTypes;
        private FieldWriter[] fieldWriters;
        private final String[] fieldNames;

        public RowWriter(RowType rowType, GroupType groupType) {
            this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
            this.logicalTypes = rowType.getChildren();
            this.fieldWriters = new FieldWriter[rowType.getFieldCount()];
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i] = createWriter(logicalTypes.get(i), groupType.getType(i));
            }
        }

        public void write(RowData row) {
            for (int i = 0; i < fieldWriters.length; i++) {
                if (!row.isNullAt(i)) {
                    String fieldName = fieldNames[i];
                    FieldWriter writer = fieldWriters[i];

                    recordConsumer.startField(fieldName, i);
                    writer.write(row, i);
                    recordConsumer.endField(fieldName, i);
                }
            }
        }

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.startGroup();
            RowData rowData = row.getRow(ordinal, fieldWriters.length);
            write(rowData);
            recordConsumer.endGroup();
        }

        @Override
        public void write(ArrayData arrayData, int ordinal) {}
    }

    private Binary timestampToInt96(TimestampData timestampData) {
        int julianDay;
        long nanosOfDay;
        if (utcTimestamp) {
            long mills = timestampData.getMillisecond();
            julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
            nanosOfDay =
                    (mills % MILLIS_IN_DAY) * NANOS_PER_MILLISECOND
                            + timestampData.getNanoOfMillisecond();
        } else {
            Timestamp timestamp = timestampData.toTimestamp();
            long mills = timestamp.getTime();
            julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
            nanosOfDay = ((mills % MILLIS_IN_DAY) / 1000) * NANOS_PER_SECOND + timestamp.getNanos();
        }

        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(nanosOfDay);
        buf.putInt(julianDay);
        buf.flip();
        return Binary.fromConstantByteBuffer(buf);
    }

    private FieldWriter createDecimalWriter(int precision, int scale) {
        Preconditions.checkArgument(
                precision <= DecimalType.MAX_PRECISION,
                "Decimal precision %s exceeds max precision %s",
                precision,
                DecimalType.MAX_PRECISION);

        /*
         * This is optimizer for UnscaledBytesWriter.
         */
        class LongUnscaledBytesWriter implements FieldWriter {
            private final int numBytes;
            private final int initShift;
            private final byte[] decimalBuffer;

            private LongUnscaledBytesWriter() {
                this.numBytes = computeMinBytesForDecimalPrecision(precision);
                this.initShift = 8 * (numBytes - 1);
                this.decimalBuffer = new byte[numBytes];
            }

            @Override
            public void write(ArrayData arrayData, int ordinal) {
                long unscaledLong =
                        (arrayData.getDecimal(ordinal, precision, scale)).toUnscaledLong();
                addRecord(unscaledLong);
            }

            @Override
            public void write(RowData row, int ordinal) {
                long unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
                addRecord(unscaledLong);
            }

            private void addRecord(long unscaledLong) {
                int i = 0;
                int shift = initShift;
                while (i < numBytes) {
                    decimalBuffer[i] = (byte) (unscaledLong >> shift);
                    i += 1;
                    shift -= 8;
                }

                recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBuffer, 0, numBytes));
            }
        }

        class UnscaledBytesWriter implements FieldWriter {
            private final int numBytes;
            private final byte[] decimalBuffer;

            private UnscaledBytesWriter() {
                this.numBytes = computeMinBytesForDecimalPrecision(precision);
                this.decimalBuffer = new byte[numBytes];
            }

            @Override
            public void write(ArrayData arrayData, int ordinal) {
                byte[] bytes = (arrayData.getDecimal(ordinal, precision, scale)).toUnscaledBytes();
                addRecord(bytes);
            }

            @Override
            public void write(RowData row, int ordinal) {
                byte[] bytes = row.getDecimal(ordinal, precision, scale).toUnscaledBytes();
                addRecord(bytes);
            }

            private void addRecord(byte[] bytes) {
                byte[] writtenBytes;
                if (bytes.length == numBytes) {
                    // Avoid copy.
                    writtenBytes = bytes;
                } else {
                    byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
                    Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
                    System.arraycopy(
                            bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
                    writtenBytes = decimalBuffer;
                }
                recordConsumer.addBinary(Binary.fromReusedByteArray(writtenBytes, 0, numBytes));
            }
        }

        // 1 <= precision <= 18, writes as FIXED_LEN_BYTE_ARRAY
        // optimizer for UnscaledBytesWriter
        if (ParquetSchemaConverter.is32BitDecimal(precision)
                || ParquetSchemaConverter.is64BitDecimal(precision)) {
            return new LongUnscaledBytesWriter();
        }

        // 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
        return new UnscaledBytesWriter();
    }
}
