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

import org.apache.flink.formats.avro.RowDataToAvroConverters.RowDataToAvroConverter;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataToAvroConverters}. */
class RowDataToAvroConvertersTest {

    private static final String TIMESTAMP_MILLIS_SCHEMA =
            "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}";
    private static final String TIMESTAMP_MICROS_SCHEMA =
            "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}";
    private static final String LOCAL_TIMESTAMP_MILLIS_SCHEMA =
            "{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}";
    private static final String LOCAL_TIMESTAMP_MICROS_SCHEMA =
            "{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}";

    @Test
    void testTimestampWithLocalTimeZoneRespectsMicrosLogicalType() {
        // FLINK-39036: writer must produce micros when the Avro schema declares
        // logicalType=timestamp-micros, otherwise downstream readers that respect
        // the logical type interpret the millis value as micros and shift the
        // timestamp by a factor of 1000.
        final RowDataToAvroConverter converter =
                RowDataToAvroConverters.createConverter(new LocalZonedTimestampType(6), false);

        final TimestampData timestamp =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2024-01-02T03:04:05.123456"));

        final long micros =
                (long)
                        converter.convert(
                                new Schema.Parser().parse(TIMESTAMP_MICROS_SCHEMA), timestamp);
        assertThat(micros).isEqualTo(1_704_164_645_123_456L);

        final long millis =
                (long)
                        converter.convert(
                                new Schema.Parser().parse(TIMESTAMP_MILLIS_SCHEMA), timestamp);
        assertThat(millis).isEqualTo(1_704_164_645_123L);
    }

    @Test
    void testTimestampWithoutTimeZoneRespectsLocalMicrosLogicalType() {
        // TIMESTAMP_WITHOUT_TIME_ZONE maps to Avro local-timestamp-* logical types
        // under the new mapping. The converter must honor local-timestamp-micros
        // and emit microseconds instead of milliseconds.
        final RowDataToAvroConverter converter =
                RowDataToAvroConverters.createConverter(new TimestampType(6), false);

        final TimestampData timestamp =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2024-01-02T03:04:05.123456"));

        final long micros =
                (long)
                        converter.convert(
                                new Schema.Parser().parse(LOCAL_TIMESTAMP_MICROS_SCHEMA),
                                timestamp);
        assertThat(micros).isEqualTo(1_704_164_645_123_456L);

        final long millis =
                (long)
                        converter.convert(
                                new Schema.Parser().parse(LOCAL_TIMESTAMP_MILLIS_SCHEMA),
                                timestamp);
        assertThat(millis).isEqualTo(1_704_164_645_123L);
    }

    @Test
    void testLegacyTimestampMappingRespectsMicrosLogicalType() {
        // The legacy mapping path also has to honor timestamp-micros when present
        // in the Avro schema, since users may serialize against an externally
        // provided schema with the micros logical type.
        final RowDataToAvroConverter converter =
                RowDataToAvroConverters.createConverter(new TimestampType(6), true);

        final TimestampData timestamp =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2024-01-02T03:04:05.123456"));

        final long micros =
                (long)
                        converter.convert(
                                new Schema.Parser().parse(TIMESTAMP_MICROS_SCHEMA), timestamp);
        assertThat(micros).isEqualTo(1_704_164_645_123_456L);
    }
}
