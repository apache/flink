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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the timestamp and time conversions of {@link AvroToRowDataConverters}. */
class AvroToRowDataConvertersTest {

    private static final Schema LONG_RECORD =
            new Schema.Parser()
                    .parse(
                            "{\"type\":\"record\",\"name\":\"R\",\"fields\":"
                                    + "[{\"name\":\"v\",\"type\":\"long\"}]}");

    private static final Schema INT_RECORD =
            new Schema.Parser()
                    .parse(
                            "{\"type\":\"record\",\"name\":\"R\",\"fields\":"
                                    + "[{\"name\":\"v\",\"type\":\"int\"}]}");

    // ---------------------------------------------------------------------------------------
    // timestamp-micros: a TIMESTAMP(6) column read from a long
    // ---------------------------------------------------------------------------------------

    /**
     * Zero, a single microsecond either side of it, negative values that are and are not a whole
     * number of milliseconds, 0001-01-01, the last microsecond of 9999 and a present-day value with
     * microsecond digits.
     */
    @ParameterizedTest
    @ValueSource(
            longs = {
                0L,
                1L,
                -1L,
                -999L,
                -1000L,
                -1001L,
                -86_400_000_000L,
                -86_400_000_001L,
                -62_135_596_800_000_000L,
                253_402_300_799_999_999L,
                1_792_140_825_306_001L
            })
    void testMicrosLongIsConvertedExactly(long micros) {
        final TimestampData expected = TimestampData.fromInstant(microsToInstant(micros));

        assertThat(convertTimestamp(DataTypes.TIMESTAMP(6), false, micros)).isEqualTo(expected);
        assertThat(convertTimestamp(DataTypes.TIMESTAMP_LTZ(6), false, micros)).isEqualTo(expected);
    }

    @Test
    void testNegativeMicrosWithARemainderHaveANonNegativeNanoOfMillisecond() {
        // A truncating division gives -1 millisecond and -1000 nanoseconds here, which
        // TimestampData rejects.
        final TimestampData oneMicroBeforeTheEpoch =
                convertTimestamp(DataTypes.TIMESTAMP(6), false, -1L);
        assertThat(oneMicroBeforeTheEpoch.getMillisecond()).isEqualTo(-1L);
        assertThat(oneMicroBeforeTheEpoch.getNanoOfMillisecond()).isEqualTo(999_000);

        final TimestampData beforeADay =
                convertTimestamp(DataTypes.TIMESTAMP(6), false, -86_400_000_001L);
        assertThat(beforeADay.getMillisecond()).isEqualTo(-86_400_001L);
        assertThat(beforeADay.getNanoOfMillisecond()).isEqualTo(999_000);
    }

    @Test
    void testLegacyTimestampMappingStillReadsEveryLongAsMilliseconds() {
        // The legacy mapping declares timestamp-millis only, whatever the precision of the column.
        assertThat(convertTimestamp(DataTypes.TIMESTAMP(6), true, 1_792_140_825_306L))
                .isEqualTo(TimestampData.fromEpochMillis(1_792_140_825_306L));
    }

    // ---------------------------------------------------------------------------------------
    // timestamp-millis: a TIMESTAMP(3) column read from a long, unchanged
    // ---------------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            longs = {0L, 1L, -1L, -1000L, -86_400_000L, 1_792_140_825_306L, 253_402_300_799_999L})
    void testMillisLongIsUnchanged(long millis) {
        assertThat(convertTimestamp(DataTypes.TIMESTAMP(3), true, millis))
                .isEqualTo(TimestampData.fromEpochMillis(millis));
        assertThat(convertTimestamp(DataTypes.TIMESTAMP_LTZ(3), false, millis))
                .isEqualTo(TimestampData.fromEpochMillis(millis));
    }

    // ---------------------------------------------------------------------------------------
    // Instant and LocalDateTime, as an Avro data model with logical type conversions gives them
    // ---------------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            longs = {
                1L,
                -1L,
                -999L,
                -86_400_000_001L,
                253_402_300_799_999_999L,
                1_792_140_825_306_001L
            })
    void testInstantKeepsSubMillisecondDigitsInAMicrosecondColumn(long micros) {
        final Instant instant = microsToInstant(micros);

        assertThat(convertTimestamp(DataTypes.TIMESTAMP(6), false, instant))
                .isEqualTo(TimestampData.fromInstant(instant));
        assertThat(convertTimestamp(DataTypes.TIMESTAMP_LTZ(6), false, instant))
                .isEqualTo(TimestampData.fromInstant(instant));
    }

    @Test
    void testInstantIsStillTruncatedToMillisecondsInAMillisecondColumn() {
        final Instant instant = microsToInstant(1_792_140_825_306_001L);

        assertThat(convertTimestamp(DataTypes.TIMESTAMP(3), true, instant))
                .isEqualTo(TimestampData.fromEpochMillis(instant.toEpochMilli()));
    }

    @Test
    void testLocalDateTimeKeepsSubMillisecondDigits() {
        final LocalDateTime dateTime =
                LocalDateTime.ofEpochSecond(-86_401L, 999_999_000, ZoneOffset.UTC);

        assertThat(convertTimestamp(DataTypes.TIMESTAMP(6), false, dateTime))
                .isEqualTo(TimestampData.fromLocalDateTime(dateTime));
    }

    // ---------------------------------------------------------------------------------------
    // time: milliseconds of day
    // ---------------------------------------------------------------------------------------

    @Test
    void testTimeFromAnIntIsMilliseconds() {
        assertThat(convertTime(45_296_789)).isEqualTo(45_296_789);
    }

    @Test
    void testTimeFromARawLongIsMicrosTruncatedToMilliseconds() {
        // time-micros read without a logical type conversion is a long. RowData stores a TIME as
        // an int of milliseconds, so the microseconds are dropped.
        assertThat(convertTime(45_296_789_123L)).isEqualTo(45_296_789);
        assertThat(convertTime(86_399_999_999L)).isEqualTo(86_399_999);
        assertThat(convertTime(1L)).isEqualTo(0);
        assertThat(convertTime(0L)).isEqualTo(0);
    }

    @Test
    void testTimeFromALocalTimeIsMilliseconds() {
        assertThat(convertTime(LocalTime.ofNanoOfDay(45_296_789_123_000L))).isEqualTo(45_296_789);
    }

    // ---------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------

    private static Instant microsToInstant(long micros) {
        return Instant.EPOCH.plus(micros, ChronoUnit.MICROS);
    }

    private static TimestampData convertTimestamp(
            DataType type, boolean legacyTimestampMapping, Object avroValue) {
        final RowType rowType = (RowType) ROW(FIELD("v", type)).getLogicalType();
        final GenericRecord record = new GenericData.Record(LONG_RECORD);
        record.put("v", avroValue);

        final RowData row =
                (RowData)
                        AvroToRowDataConverters.createRowConverter(rowType, legacyTimestampMapping)
                                .convert(record);
        return row.getTimestamp(0, 6);
    }

    private static int convertTime(Object avroValue) {
        final RowType rowType = (RowType) ROW(FIELD("v", DataTypes.TIME(3))).getLogicalType();
        final GenericRecord record =
                new GenericData.Record(avroValue instanceof Long ? LONG_RECORD : INT_RECORD);
        record.put("v", avroValue);

        final RowData row =
                (RowData) AvroToRowDataConverters.createRowConverter(rowType).convert(record);
        return row.getInt(0);
    }
}
