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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
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

    private final RecordConsumer recordConsumer;
    private final boolean utcTimestamp;

    private final FieldWriter[] filedWriters;
    private final String[] fieldNames;

    public ParquetRowDataWriter(
            RecordConsumer recordConsumer,
            RowType rowType,
            GroupType schema,
            boolean utcTimestamp) {
        this.recordConsumer = recordConsumer;
        this.utcTimestamp = utcTimestamp;

        this.filedWriters = new FieldWriter[rowType.getFieldCount()];
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.filedWriters[i] = createWriter(rowType.getTypeAt(i), schema.getType(i));
        }
    }

    /**
     * It writes a record to Parquet.
     *
     * @param record Contains the record that is going to be written.
     */
    public void write(final RowData record) {
        recordConsumer.startMessage();
        for (int i = 0; i < filedWriters.length; i++) {
            if (!record.isNullAt(i)) {
                String fieldName = fieldNames[i];
                FieldWriter writer = filedWriters[i];

                recordConsumer.startField(fieldName, i);
                writer.write(record, i);
                recordConsumer.endField(fieldName, i);
            }
        }
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
            } else if (t instanceof RowType && type instanceof GroupType) {
                return new RowWriter(t, groupType);
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        }
    }

    private interface FieldWriter {

        void write(RowData row, int ordinal);

        void write(Object value);
    }

    private class BooleanWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addBoolean(row.getBoolean(ordinal));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addBoolean((boolean) value);
        }
    }

    private class ByteWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addInteger(row.getByte(ordinal));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addInteger((byte) value);
        }
    }

    private class ShortWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addInteger(row.getShort(ordinal));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addInteger((short) value);
        }
    }

    private class LongWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addLong(row.getLong(ordinal));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addLong((long) value);
        }
    }

    private class FloatWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addFloat(row.getFloat(ordinal));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addFloat((float) value);
        }
    }

    private class DoubleWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addDouble(row.getDouble(ordinal));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addDouble((double) value);
        }
    }

    private class StringWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(row.getString(ordinal).toBytes()));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(((StringData) value).toBytes()));
        }
    }

    private class BinaryWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(row.getBinary(ordinal)));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) value));
        }
    }

    private class IntWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addInteger(row.getInt(ordinal));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addInteger((int) value);
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
            recordConsumer.addBinary(timestampToInt96(row.getTimestamp(ordinal, precision)));
        }

        @Override
        public void write(Object value) {
            recordConsumer.addBinary(timestampToInt96((TimestampData) value));
        }
    }

    private class MapWriter implements FieldWriter {

        private String repeatedGroupName;
        private String keyName, valueName;
        private FieldWriter keyWriter, valueWriter;
        private ArrayData.ElementGetter keyElementGetter, valueElementGetter;

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

            this.keyElementGetter = ArrayData.createElementGetter(keyType);
            this.valueElementGetter = ArrayData.createElementGetter(valueType);
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
                    Object key = keyElementGetter.getElementOrNull(keyArray, i);
                    Object value = valueElementGetter.getElementOrNull(valueArray, i);

                    recordConsumer.startGroup();
                    if (key != null) {
                        // write key element
                        recordConsumer.startField(keyName, 0);
                        keyWriter.write(key);
                        recordConsumer.endField(keyName, 0);

                        // write value element
                        if (value != null) {
                            recordConsumer.startField(valueName, 1);
                            valueWriter.write(value);
                            recordConsumer.endField(valueName, 1);
                        }
                    }
                    recordConsumer.endGroup();
                }

                recordConsumer.endField(repeatedGroupName, 0);
            }
            recordConsumer.endGroup();
        }

        @Override
        public void write(Object value) {}
    }

    private class ArrayWriter implements FieldWriter {

        private String elementName;
        private FieldWriter elementWriter;
        private String repeatedGroupName;
        private ArrayData.ElementGetter elementGetter;

        private ArrayWriter(LogicalType t, GroupType groupType) {

            // Get the internal array structure
            GroupType repeatedType = groupType.getType(0).asGroupType();
            this.repeatedGroupName = repeatedType.getName();

            Type elementType = repeatedType.getType(0);
            this.elementName = elementType.getName();

            this.elementWriter = createWriter(t, elementType);
            this.elementGetter = ArrayData.createElementGetter(t);
        }

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.startGroup();
            ArrayData arrayData = row.getArray(ordinal);
            int listLength = arrayData.size();

            if (listLength > 0) {
                recordConsumer.startField(repeatedGroupName, 0);

                for (int i = 0; i < listLength; i++) {
                    Object object = elementGetter.getElementOrNull(arrayData, i);
                    recordConsumer.startGroup();
                    if (object != null) {
                        recordConsumer.startField(elementName, 0);
                        elementWriter.write(object);
                        recordConsumer.endField(elementName, 0);
                    }
                    recordConsumer.endGroup();
                }

                recordConsumer.endField(repeatedGroupName, 0);
            }
            recordConsumer.endGroup();
        }

        @Override
        public void write(Object value) {}
    }

    private class GroupTypeWriter implements FieldWriter {
        private List<Type> types;
        private List<LogicalType> logicalTypes;
        private FieldWriter[] fieldWriters;

        public GroupTypeWriter(LogicalType t, GroupType groupType) {
            this.types = groupType.getFields();
            this.logicalTypes = t.getChildren();

            this.fieldWriters = new FieldWriter[types.size()];
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i] = createWriter(logicalTypes.get(i), types.get(i));
            }
        }

        @Override
        public void write(RowData row, int ordinal) {
            RowData rowData = row.getRow(ordinal, types.size());
            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);
                FieldWriter writer = fieldWriters[i];

                if (!rowData.isNullAt(i)) {
                    String fieldName = type.getName();
                    recordConsumer.startField(fieldName, i);
                    writer.write(rowData, i);
                    recordConsumer.endField(fieldName, i);
                }
            }
        }

        @Override
        public void write(Object value) {}
    }

    private class RowWriter extends GroupTypeWriter implements FieldWriter {
        public RowWriter(LogicalType t, GroupType groupType) {
            super(t, groupType);
        }

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.startGroup();
            super.write(row, ordinal);
            recordConsumer.endGroup();
        }
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
            public void write(Object value) {
                long unscaledLong = ((DecimalData) value).toUnscaledLong();
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
            public void write(Object value) {
                byte[] bytes = ((DecimalData) value).toUnscaledBytes();
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
        if (DecimalDataUtils.is32BitDecimal(precision)
                || DecimalDataUtils.is64BitDecimal(precision)) {
            return new LongUnscaledBytesWriter();
        }

        // 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
        return new UnscaledBytesWriter();
    }
}
