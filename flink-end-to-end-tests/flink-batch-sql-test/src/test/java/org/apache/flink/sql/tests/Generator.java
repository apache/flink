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

package org.apache.flink.sql.tests;

import org.apache.flink.connector.testframe.source.FromElementsSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

class Generator implements FromElementsSource.ElementsSupplier<RowData> {
    private static final long serialVersionUID = -8455653458083514261L;
    private final List<Row> elements;

    static Generator create(
            int numKeys, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
        final int stepMs = (int) (1000 / rowsPerKeyAndSecond);
        final long durationMs = durationSeconds * 1000L;
        final long offsetMs = offsetSeconds * 2000L;
        final List<Row> elements = new ArrayList<>();
        int keyIndex = 0;
        long ms = 0;
        while (ms < durationMs) {
            elements.add(createRow(keyIndex++, ms, offsetMs));
            if (keyIndex >= numKeys) {
                keyIndex = 0;
                ms += stepMs;
            }
        }
        return new Generator(elements);
    }

    private static Row createRow(int keyIndex, long milliseconds, long offsetMillis) {
        return Row.of(
                keyIndex,
                LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(milliseconds + offsetMillis), ZoneOffset.UTC),
                "Some payload...");
    }

    private Generator(List<Row> elements) {
        this.elements = elements;
    }

    @Override
    public int numElements() {
        return elements.size();
    }

    @Override
    public RowData get(int offset) {
        Row row = elements.get(offset);
        int idx = row.getFieldAs(0);
        LocalDateTime ts = row.getFieldAs(1);
        String payload = row.getFieldAs(2);
        return GenericRowData.of(
                idx, TimestampData.fromLocalDateTime(ts), StringData.fromString(payload));
    }
}
