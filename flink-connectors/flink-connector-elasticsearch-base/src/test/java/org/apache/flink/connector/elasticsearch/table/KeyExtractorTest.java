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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyExtractor}. */
public class KeyExtractorTest {
    @Test
    public void testSimpleKey() {
        List<LogicalTypeWithIndex> logicalTypesWithIndex =
                Stream.of(
                                new LogicalTypeWithIndex(
                                        0, DataTypes.BIGINT().notNull().getLogicalType()))
                        .collect(Collectors.toList());

        Function<RowData, String> keyExtractor =
                KeyExtractor.createKeyExtractor(logicalTypesWithIndex, "_");

        String key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
        assertThat(key).isEqualTo("12");
    }

    @Test
    public void testNoPrimaryKey() {
        List<LogicalTypeWithIndex> logicalTypesWithIndex = Collections.emptyList();

        Function<RowData, String> keyExtractor =
                KeyExtractor.createKeyExtractor(logicalTypesWithIndex, "_");

        String key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
        assertThat(key).isNull();
    }

    @Test
    public void testTwoFieldsKey() {
        List<LogicalTypeWithIndex> logicalTypesWithIndex =
                Stream.of(
                                new LogicalTypeWithIndex(
                                        0, DataTypes.BIGINT().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        2, DataTypes.TIMESTAMP().notNull().getLogicalType()))
                        .collect(Collectors.toList());

        Function<RowData, String> keyExtractor =
                KeyExtractor.createKeyExtractor(logicalTypesWithIndex, "_");

        String key =
                keyExtractor.apply(
                        GenericRowData.of(
                                12L,
                                StringData.fromString("ABCD"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2012-12-12T12:12:12"))));
        assertThat(key).isEqualTo("12_2012-12-12T12:12:12");
    }

    @Test
    public void testAllTypesKey() {
        List<LogicalTypeWithIndex> logicalTypesWithIndex =
                Stream.of(
                                new LogicalTypeWithIndex(
                                        0, DataTypes.TINYINT().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        1, DataTypes.SMALLINT().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        2, DataTypes.INT().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        3, DataTypes.BIGINT().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        4, DataTypes.BOOLEAN().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        5, DataTypes.FLOAT().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        6, DataTypes.DOUBLE().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        7, DataTypes.STRING().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        8, DataTypes.TIMESTAMP().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        9,
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
                                                .notNull()
                                                .getLogicalType()),
                                new LogicalTypeWithIndex(
                                        10, DataTypes.TIME().notNull().getLogicalType()),
                                new LogicalTypeWithIndex(
                                        11, DataTypes.DATE().notNull().getLogicalType()))
                        .collect(Collectors.toList());

        Function<RowData, String> keyExtractor =
                KeyExtractor.createKeyExtractor(logicalTypesWithIndex, "_");

        String key =
                keyExtractor.apply(
                        GenericRowData.of(
                                (byte) 1,
                                (short) 2,
                                3,
                                (long) 4,
                                true,
                                1.0f,
                                2.0d,
                                StringData.fromString("ABCD"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2012-12-12T12:12:12")),
                                TimestampData.fromInstant(Instant.parse("2013-01-13T13:13:13Z")),
                                (int) (LocalTime.parse("14:14:14").toNanoOfDay() / 1_000_000),
                                (int) LocalDate.parse("2015-05-15").toEpochDay()));
        assertThat(key)
                .isEqualTo(
                        "1_2_3_4_true_1.0_2.0_ABCD_2012-12-12T12:12:12_2013-01-13T13:13:13_14:14:14_2015-05-15");
    }
}
