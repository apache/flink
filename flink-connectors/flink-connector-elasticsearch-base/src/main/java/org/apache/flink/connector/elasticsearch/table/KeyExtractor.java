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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.SerializableFunction;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.List;

/** An extractor for a Elasticsearch key from a {@link RowData}. */
@Internal
class KeyExtractor implements SerializableFunction<RowData, String> {
    private final FieldFormatter[] fieldFormatters;
    private final String keyDelimiter;

    private interface FieldFormatter extends Serializable {
        String format(RowData rowData);
    }

    private KeyExtractor(FieldFormatter[] fieldFormatters, String keyDelimiter) {
        this.fieldFormatters = fieldFormatters;
        this.keyDelimiter = keyDelimiter;
    }

    @Override
    public String apply(RowData rowData) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fieldFormatters.length; i++) {
            if (i > 0) {
                builder.append(keyDelimiter);
            }
            final String value = fieldFormatters[i].format(rowData);
            builder.append(value);
        }
        return builder.toString();
    }

    public static SerializableFunction<RowData, String> createKeyExtractor(
            List<LogicalTypeWithIndex> primaryKeyTypesWithIndex, String keyDelimiter) {
        if (!primaryKeyTypesWithIndex.isEmpty()) {
            FieldFormatter[] formatters =
                    primaryKeyTypesWithIndex.stream()
                            .map(
                                    logicalTypeWithIndex ->
                                            toFormatter(
                                                    logicalTypeWithIndex.index,
                                                    logicalTypeWithIndex.logicalType))
                            .toArray(FieldFormatter[]::new);
            return new KeyExtractor(formatters, keyDelimiter);
        } else {
            return (row) -> null;
        }
    }

    private static FieldFormatter toFormatter(int index, LogicalType type) {
        switch (type.getTypeRoot()) {
            case DATE:
                return (row) -> LocalDate.ofEpochDay(row.getInt(index)).toString();
            case TIME_WITHOUT_TIME_ZONE:
                return (row) ->
                        LocalTime.ofNanoOfDay((long) row.getInt(index) * 1_000_000L).toString();
            case INTERVAL_YEAR_MONTH:
                return (row) -> Period.ofDays(row.getInt(index)).toString();
            case INTERVAL_DAY_TIME:
                return (row) -> Duration.ofMillis(row.getLong(index)).toString();
            case DISTINCT_TYPE:
                return toFormatter(index, ((DistinctType) type).getSourceType());
            default:
                RowData.FieldGetter fieldGetter = RowData.createFieldGetter(type, index);
                return (row) -> fieldGetter.getFieldOrNull(row).toString();
        }
    }
}
