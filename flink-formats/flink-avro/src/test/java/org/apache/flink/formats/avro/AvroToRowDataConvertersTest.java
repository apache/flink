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

package org.apache.flink.formats.avro;

import org.apache.flink.formats.avro.AvroToRowDataConverters.AvroToRowDataConverter;
import org.apache.flink.formats.avro.RowDataToAvroConverters.RowDataToAvroConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroToRowDataConverters}. */
class AvroToRowDataConvertersTest {

    private static final String TIMESTAMP_MICROS_RECORD_SCHEMA =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"ts\",\"type\":{\"type\":\"long\","
                    + "\"logicalType\":\"timestamp-micros\"}}]}";
    private static final String TIMESTAMP_MILLIS_RECORD_SCHEMA =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"ts\",\"type\":{\"type\":\"long\","
                    + "\"logicalType\":\"timestamp-millis\"}}]}";
    private static final String LOCAL_TIMESTAMP_MICROS_RECORD_SCHEMA =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"ts\",\"type\":{\"type\":\"long\","
                    + "\"logicalType\":\"local-timestamp-micros\"}}]}";

    @Test
    void testTimestampMicrosLogicalTypeReadAsMicros() {
        // FLINK-39036: when the Flink type is TIMESTAMP_WITH_LOCAL_TIME_ZONE(6) (mapped
        // from timestamp-micros), the reader must interpret the incoming long as
        // microseconds, not milliseconds.
        final RowType rowType = RowType.of(new LocalZonedTimestampType(6));
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType, false);

        final GenericRecord record =
                new GenericData.Record(new Schema.Parser().parse(TIMESTAMP_MICROS_RECORD_SCHEMA));
        record.put(0, 1_704_164_645_123_456L);

        final RowData row = (RowData) converter.convert(record);
        assertThat(row.getTimestamp(0, 6))
                .isEqualTo(
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2024-01-02T03:04:05.123456")));
    }

    @Test
    void testLocalTimestampMicrosLogicalTypeReadAsMicros() {
        // local-timestamp-micros maps to TIMESTAMP(6) under the new (non-legacy) mapping.
        final RowType rowType = RowType.of(new TimestampType(6));
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType, false);

        final GenericRecord record =
                new GenericData.Record(
                        new Schema.Parser().parse(LOCAL_TIMESTAMP_MICROS_RECORD_SCHEMA));
        record.put(0, 1_704_164_645_123_456L);

        final RowData row = (RowData) converter.convert(record);
        assertThat(row.getTimestamp(0, 6))
                .isEqualTo(
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2024-01-02T03:04:05.123456")));
    }

    @Test
    void testTimestampMillisPrecisionPreservesExistingBehavior() {
        // Regression: when precision <= 3, the incoming long must continue to be
        // interpreted as milliseconds. This guards the most common existing usage.
        final RowType rowType = RowType.of(new TimestampType(3));
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType, false);

        final GenericRecord record =
                new GenericData.Record(new Schema.Parser().parse(TIMESTAMP_MILLIS_RECORD_SCHEMA));
        record.put(0, 1_704_164_645_123L);

        final RowData row = (RowData) converter.convert(record);
        assertThat(row.getTimestamp(0, 3))
                .isEqualTo(TimestampData.fromEpochMillis(1_704_164_645_123L));
    }

    @Test
    void testNegativeMicrosTimestampHandlesFloorSemantics() {
        // 1ms before epoch = -1000 micros. Naive division would yield millis=-1,
        // nanoOfMillis=0, but TimestampData.fromEpochMillis(long, int) requires
        // nanoOfMillis in [0, 999_999], so floor-div / floor-mod must be used.
        final RowType rowType = RowType.of(new LocalZonedTimestampType(6));
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType, false);

        final GenericRecord record =
                new GenericData.Record(new Schema.Parser().parse(TIMESTAMP_MICROS_RECORD_SCHEMA));
        record.put(0, -1_500L); // 1.5ms before epoch (-1500 micros)

        final RowData row = (RowData) converter.convert(record);
        // -1500 micros = floor(-1500 / 1000) ms + floorMod(-1500, 1000) micros
        //              = -2 ms + 500 micros = -2 ms + 500_000 nanos
        assertThat(row.getTimestamp(0, 6)).isEqualTo(TimestampData.fromEpochMillis(-2L, 500_000));
    }

    @Test
    void testRoundTripPreservesMicrosPrecision() {
        // End-to-end guard against the original FLINK-39036 bug pattern: the
        // writer-only fix used to silently break Flink-internal round-trips with a
        // timestamp-micros schema because the reader still interpreted the value as
        // millis. Both halves must honor the logical type for a round-trip to be a
        // no-op.
        final TimestampData original =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2024-01-02T03:04:05.123456"));

        final Schema fieldSchema =
                new Schema.Parser()
                        .parse("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}");
        final RowDataToAvroConverter writer =
                RowDataToAvroConverters.createConverter(new LocalZonedTimestampType(6), false);
        final Long encoded = (Long) writer.convert(fieldSchema, original);
        assertThat(encoded).isEqualTo(1_704_164_645_123_456L);

        final RowType rowType = RowType.of(new LocalZonedTimestampType(6));
        final AvroToRowDataConverter reader =
                AvroToRowDataConverters.createRowConverter(rowType, false);
        final GenericRecord record =
                new GenericData.Record(new Schema.Parser().parse(TIMESTAMP_MICROS_RECORD_SCHEMA));
        record.put(0, encoded);
        final RowData row = (RowData) reader.convert(record);

        assertThat(row.getTimestamp(0, 6)).isEqualTo(original);
    }

    @Test
    void testRoundTripPreservesMillisPrecision() {
        // Regression guard for the common timestamp-millis case.
        final TimestampData original =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2024-01-02T03:04:05.123"));

        final Schema fieldSchema =
                new Schema.Parser()
                        .parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
        final RowDataToAvroConverter writer =
                RowDataToAvroConverters.createConverter(new LocalZonedTimestampType(3), false);
        final Long encoded = (Long) writer.convert(fieldSchema, original);
        assertThat(encoded).isEqualTo(1_704_164_645_123L);

        final RowType rowType = RowType.of(new LocalZonedTimestampType(3));
        final AvroToRowDataConverter reader =
                AvroToRowDataConverters.createRowConverter(rowType, false);
        final GenericRecord record =
                new GenericData.Record(new Schema.Parser().parse(TIMESTAMP_MILLIS_RECORD_SCHEMA));
        record.put(0, encoded);
        final RowData row = (RowData) reader.convert(record);

        assertThat(row.getTimestamp(0, 3)).isEqualTo(original);
    }
}
