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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader;
import org.apache.flink.table.data.TimestampData;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static org.junit.Assert.assertEquals;

/** Test for {@link TimestampColumnReader}. */
public class ParquetInt64TimestampReaderTest {
    @Test
    public void testReadInt64TimestampMicros() {
        LocalDateTime localDateTime = LocalDateTime.of(2021, 11, 22, 17, 50, 20, 112233);
        long time =
                localDateTime.toEpochSecond(OffsetDateTime.now().getOffset()) * 1_000_000
                        + localDateTime.getNano() / 1_000;
        TimestampData timestampData =
                TimestampColumnReader.int64ToTimestamp(
                        false, time, LogicalTypeAnnotation.TimeUnit.MICROS);
        assertEquals("2021-11-22T17:50:20.000112", timestampData.toString());
    }

    @Test
    public void testReadInt64TimestampMillis() {
        LocalDateTime localDateTime = LocalDateTime.of(2021, 11, 22, 17, 50, 20, 112233);
        long time =
                localDateTime.toEpochSecond(OffsetDateTime.now().getOffset()) * 1_000
                        + localDateTime.getNano() / 1_000_000;
        TimestampData timestampData =
                TimestampColumnReader.int64ToTimestamp(
                        false, time, LogicalTypeAnnotation.TimeUnit.MILLIS);
        assertEquals("2021-11-22T17:50:20", timestampData.toString());
    }

    @Test
    public void testReadInt64TimestampNanos() {
        LocalDateTime localDateTime = LocalDateTime.of(2021, 11, 22, 17, 50, 20, 112233);
        long time =
                localDateTime.toEpochSecond(OffsetDateTime.now().getOffset()) * 1_000_000_000
                        + localDateTime.getNano();
        TimestampData timestampData =
                TimestampColumnReader.int64ToTimestamp(
                        false, time, LogicalTypeAnnotation.TimeUnit.NANOS);
        assertEquals("2021-11-22T17:50:20.000112233", timestampData.toString());
    }
}
