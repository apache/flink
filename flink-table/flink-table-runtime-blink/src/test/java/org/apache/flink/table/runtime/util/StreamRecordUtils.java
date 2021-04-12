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

package org.apache.flink.table.runtime.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.data.StringData.fromString;

/** Utilities to generate StreamRecord which encapsulates RowData. */
public class StreamRecordUtils {

    /**
     * Creates n new {@link StreamRecord} of {@link RowData} based on the given fields array and the
     * give RowKind.
     */
    public static StreamRecord<RowData> record(RowKind rowKind, Object... fields) {
        RowData row = row(fields);
        row.setRowKind(rowKind);
        return new StreamRecord<>(row);
    }

    /**
     * Creates n new {@link StreamRecord} of {@link RowData} based on the given fields array and a
     * default INSERT RowKind.
     *
     * @param fields input object array
     * @return generated StreamRecord
     */
    public static StreamRecord<RowData> insertRecord(Object... fields) {
        return new StreamRecord<>(row(fields));
    }

    /**
     * Creates n new {@link StreamRecord} of {@link BinaryRowData} based on the given fields array
     * and the given RowKind.
     */
    public static StreamRecord<RowData> binaryRecord(RowKind rowKind, Object... fields) {
        BinaryRowData row = binaryrow(fields);
        row.setRowKind(rowKind);
        return new StreamRecord<>(row);
    }

    /**
     * Creates n new {@link StreamRecord} of {@link RowData} based on the given fields array and a
     * default UPDATE_BEFORE RowKind.
     *
     * @param fields input object array
     * @return generated StreamRecord
     */
    public static StreamRecord<RowData> updateBeforeRecord(Object... fields) {
        RowData row = row(fields);
        row.setRowKind(RowKind.UPDATE_BEFORE);
        return new StreamRecord<>(row);
    }

    /**
     * Creates n new {@link StreamRecord} of {@link RowData} based on the given fields array and a
     * default UPDATE_AFTER RowKind.
     */
    public static StreamRecord<RowData> updateAfterRecord(Object... fields) {
        RowData row = row(fields);
        row.setRowKind(RowKind.UPDATE_AFTER);
        return new StreamRecord<>(row);
    }

    /**
     * Creates n new {@link StreamRecord} of {@link RowData} based on the given fields array and a
     * default DELETE RowKind.
     *
     * @param fields input object array
     * @return generated StreamRecord
     */
    public static StreamRecord<RowData> deleteRecord(Object... fields) {
        RowData row = row(fields);
        row.setRowKind(RowKind.DELETE);
        return new StreamRecord<>(row);
    }

    /**
     * Receives a object array, generates a RowData based on the array.
     *
     * @param fields input object array
     * @return generated RowData.
     */
    public static RowData row(Object... fields) {
        Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            Object field = fields[i];
            if (field instanceof String) {
                objects[i] = fromString((String) field);
            } else {
                objects[i] = field;
            }
        }
        return GenericRowData.of(objects);
    }

    /**
     * Receives a object array, generates a BinaryRowData based on the array.
     *
     * @param fields input object array
     * @return generated BinaryRowData.
     */
    public static BinaryRowData binaryrow(Object... fields) {
        BinaryRowData row = new BinaryRowData(fields.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int j = 0; j < fields.length; j++) {
            Object value = fields[j];
            if (value == null) {
                writer.setNullAt(j);
            } else if (value instanceof Byte) {
                writer.writeByte(j, (Byte) value);
            } else if (value instanceof Short) {
                writer.writeShort(j, (Short) value);
            } else if (value instanceof Integer) {
                writer.writeInt(j, (Integer) value);
            } else if (value instanceof String) {
                writer.writeString(j, StringData.fromString((String) value));
            } else if (value instanceof Double) {
                writer.writeDouble(j, (Double) value);
            } else if (value instanceof Float) {
                writer.writeFloat(j, (Float) value);
            } else if (value instanceof Long) {
                writer.writeLong(j, (Long) value);
            } else if (value instanceof Boolean) {
                writer.writeBoolean(j, (Boolean) value);
            } else if (value instanceof byte[]) {
                writer.writeBinary(j, (byte[]) value);
            } else if (value instanceof DecimalData) {
                DecimalData decimal = (DecimalData) value;
                writer.writeDecimal(j, decimal, decimal.precision());
            } else if (value instanceof TimestampData) {
                TimestampData timestamp = (TimestampData) value;
                writer.writeTimestamp(j, timestamp, 3);
            } else if (value instanceof Tuple2 && ((Tuple2) value).f0 instanceof TimestampData) {
                TimestampData timestamp = (TimestampData) ((Tuple2) value).f0;
                writer.writeTimestamp(j, timestamp, (int) ((Tuple2) value).f1);
            } else if (value instanceof Tuple2 && ((Tuple2) value).f0 instanceof ArrayData) {
                ArrayData array = (ArrayData) ((Tuple2) value).f0;
                ArrayDataSerializer serializer = (ArrayDataSerializer) ((Tuple2) value).f1;
                writer.writeArray(j, array, serializer);
            } else if (value instanceof Tuple2 && ((Tuple2) value).f0 instanceof RowData) {
                RowData rowData = ((RowData) ((Tuple2) value).f0);
                RowDataSerializer serializer = (RowDataSerializer) ((Tuple2) value).f1;
                writer.writeRow(j, rowData, serializer);
            } else {
                throw new RuntimeException("Not support yet!");
            }
        }

        writer.complete();
        return row;
    }

    private StreamRecordUtils() {
        // deprecate default constructor
    }
}
