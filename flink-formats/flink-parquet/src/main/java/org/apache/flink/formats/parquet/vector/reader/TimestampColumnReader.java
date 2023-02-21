/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.writable.WritableIntVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableTimestampVector;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * Timestamp {@link ColumnReader}. We only support INT96 bytes now, julianDay(4) + nanosOfDay(8).
 * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
 * TIMESTAMP_MILLIS and TIMESTAMP_MICROS are the deprecated ConvertedType.
 */
public class TimestampColumnReader extends AbstractColumnReader<WritableTimestampVector> {

    public static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
    public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    public static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    public static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    private final boolean utcTimestamp;

    public TimestampColumnReader(
            boolean utcTimestamp, ColumnDescriptor descriptor, PageReader pageReader)
            throws IOException {
        super(descriptor, pageReader);
        this.utcTimestamp = utcTimestamp;
        checkTypeName(PrimitiveType.PrimitiveTypeName.INT96);
    }

    @Override
    protected boolean supportLazyDecode() {
        return utcTimestamp;
    }

    @Override
    protected void readBatch(int rowId, int num, WritableTimestampVector column) {
        for (int i = 0; i < num; i++) {
            if (runLenDecoder.readInteger() == maxDefLevel) {
                ByteBuffer buffer = readDataBuffer(12);
                column.setTimestamp(
                        rowId + i,
                        int96ToTimestamp(utcTimestamp, buffer.getLong(), buffer.getInt()));
            } else {
                column.setNullAt(rowId + i);
            }
        }
    }

    @Override
    protected void readBatchFromDictionaryIds(
            int rowId, int num, WritableTimestampVector column, WritableIntVector dictionaryIds) {
        for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
                column.setTimestamp(
                        i,
                        decodeInt96ToTimestamp(utcTimestamp, dictionary, dictionaryIds.getInt(i)));
            }
        }
    }

    public static TimestampData decodeInt96ToTimestamp(
            boolean utcTimestamp, org.apache.parquet.column.Dictionary dictionary, int id) {
        Binary binary = dictionary.decodeToBinary(id);
        Preconditions.checkArgument(
                binary.length() == 12, "Timestamp with int96 should be 12 bytes.");
        ByteBuffer buffer = binary.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        return int96ToTimestamp(utcTimestamp, buffer.getLong(), buffer.getInt());
    }

    public static TimestampData int96ToTimestamp(
            boolean utcTimestamp, long nanosOfDay, int julianDay) {
        long millisecond = julianDayToMillis(julianDay) + (nanosOfDay / NANOS_PER_MILLISECOND);

        if (utcTimestamp) {
            int nanoOfMillisecond = (int) (nanosOfDay % NANOS_PER_MILLISECOND);
            return TimestampData.fromEpochMillis(millisecond, nanoOfMillisecond);
        } else {
            Timestamp timestamp = new Timestamp(millisecond);
            timestamp.setNanos((int) (nanosOfDay % NANOS_PER_SECOND));
            return TimestampData.fromTimestamp(timestamp);
        }
    }

    private static long julianDayToMillis(int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }
}
