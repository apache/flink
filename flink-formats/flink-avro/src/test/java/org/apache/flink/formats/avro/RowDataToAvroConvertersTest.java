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

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the timestamp conversions of {@link RowDataToAvroConverters}. */
class RowDataToAvroConvertersTest {

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
    void testTimestampOfMicrosecondPrecisionIsWrittenAsMicroseconds(long micros) {
        final TimestampData timestamp = TimestampData.fromInstant(microsToInstant(micros));

        assertThat(write(DataTypes.TIMESTAMP(6), false, timestamp)).isEqualTo(micros);
        assertThat(write(DataTypes.TIMESTAMP_LTZ(6), false, timestamp)).isEqualTo(micros);
    }

    @Test
    void testTimestampOfMillisecondPrecisionIsStillWrittenAsMilliseconds() {
        final TimestampData timestamp = TimestampData.fromEpochMillis(1_792_140_825_306L);

        assertThat(write(DataTypes.TIMESTAMP(3), false, timestamp)).isEqualTo(1_792_140_825_306L);
        assertThat(write(DataTypes.TIMESTAMP(3), true, timestamp)).isEqualTo(1_792_140_825_306L);
        assertThat(write(DataTypes.TIMESTAMP_LTZ(3), false, timestamp))
                .isEqualTo(1_792_140_825_306L);
    }

    @Test
    void testWrittenValueMatchesTheLogicalTypeTheSchemaDeclares() {
        // The schema derived for a TIMESTAMP(6) says microseconds, so that is what must be written.
        final RowType rowType =
                (RowType)
                        ROW(FIELD("v", DataTypes.TIMESTAMP(6).notNull()))
                                .notNull()
                                .getLogicalType();
        final Schema schema = AvroSchemaConverter.convertToSchema(rowType, false);
        assertThat(schema.getField("v").schema().getLogicalType().getName())
                .isEqualTo("local-timestamp-micros");

        final TimestampData timestamp = TimestampData.fromInstant(microsToInstant(123_456L));
        assertThat(write(DataTypes.TIMESTAMP(6), false, timestamp)).isEqualTo(123_456L);
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                1L,
                -1L,
                -999L,
                -1001L,
                -86_400_000_001L,
                -62_135_596_800_000_000L,
                253_402_300_799_999_999L,
                1_792_140_825_306_001L
            })
    void testWhatIsWrittenIsReadBackUnchanged(long micros) {
        final TimestampData timestamp = TimestampData.fromInstant(microsToInstant(micros));

        for (DataType type : new DataType[] {DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP_LTZ(6)}) {
            final RowType rowType = (RowType) ROW(FIELD("v", type)).getLogicalType();
            final Schema schema = AvroSchemaConverter.convertToSchema(rowType, false);
            final GenericRecord record =
                    (GenericRecord)
                            RowDataToAvroConverters.createConverter(rowType, false)
                                    .convert(schema, GenericRowData.of(timestamp));

            final RowData back =
                    (RowData)
                            AvroToRowDataConverters.createRowConverter(rowType, false)
                                    .convert(record);

            assertThat(back.getTimestamp(0, 6)).isEqualTo(timestamp);
        }
    }

    private static Instant microsToInstant(long micros) {
        return Instant.EPOCH.plus(micros, ChronoUnit.MICROS);
    }

    private static Object write(
            DataType type, boolean legacyTimestampMapping, TimestampData timestamp) {
        final RowType rowType = (RowType) ROW(FIELD("v", type)).getLogicalType();
        final Schema schema = AvroSchemaConverter.convertToSchema(rowType, legacyTimestampMapping);
        final GenericRecord record =
                (GenericRecord)
                        RowDataToAvroConverters.createConverter(rowType, legacyTimestampMapping)
                                .convert(schema, GenericRowData.of(timestamp));
        return record.get(0);
    }
}
