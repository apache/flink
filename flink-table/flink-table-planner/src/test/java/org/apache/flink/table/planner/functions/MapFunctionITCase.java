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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.util.CollectionUtil;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.util.CollectionUtil.entry;

/** Test {@link BuiltInFunctionDefinitions#MAP} and its return type. */
public class MapFunctionITCase extends BuiltInFunctionTestBase {
    private static final LocalDate TEST_DATE_1 = LocalDate.of(1985, 11, 4);
    private static final LocalDate TEST_DATE_2 = LocalDate.of(2018, 7, 26);
    private static final LocalTime TEST_TIME_1 = LocalTime.of(17, 18, 19);
    private static final LocalTime TEST_TIME_2 = LocalTime.of(14, 15, 16);
    private static final LocalDateTime TEST_DATE_TIME_1 = LocalDateTime.of(1985, 11, 4, 17, 18, 19);
    private static final LocalDateTime TEST_DATE_TIME_2 = LocalDateTime.of(2018, 7, 26, 14, 15, 16);
    private static final String A = "a";
    private static final String B = "b";
    private static final int INTERVAL_1 = -123;
    private static final Integer INTERVAL_NULL = null;

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP)
                        .onFieldsWithData(
                                1,
                                2,
                                3,
                                4,
                                new BigDecimal("1.2345"),
                                new BigDecimal("1.2346"),
                                true)
                        .andDataTypes(
                                INT().notNull(),
                                INT().notNull(),
                                INT().notNull(),
                                INT().notNull(),
                                DECIMAL(10, 4).notNull(),
                                DECIMAL(10, 4).notNull(),
                                BOOLEAN().notNull())
                        .testResult(
                                resultSpec(
                                        map($("f0"), $("f0"), $("f0"), $("f1")),
                                        "MAP[f0, f1]",
                                        Collections.singletonMap(1, 2),
                                        DataTypes.MAP(INT().notNull(), INT().notNull()).notNull()),
                                resultSpec(
                                        map($("f4"), $("f5")),
                                        "MAP[f4, f5]",
                                        Collections.singletonMap(
                                                new BigDecimal("1.2345"), new BigDecimal("1.2346")),
                                        DataTypes.MAP(
                                                        DECIMAL(10, 4).notNull(),
                                                        DECIMAL(10, 4).notNull())
                                                .notNull()),
                                resultSpec(
                                        map(
                                                $("f0").plus($("f1")),
                                                $("f2").times($("f2")),
                                                $("f2").minus($("f1")),
                                                $("f3").minus($("f0"))),
                                        "MAP[f0 + f1, f2 * f2, f2 - f1, f3 - f0]",
                                        CollectionUtil.map(
                                                entry(1 + 2, 3 * 3), entry(3 - 2, 4 - 1)),
                                        DataTypes.MAP(INT().notNull(), INT().notNull()).notNull()),
                                resultSpec(
                                        map(
                                                $("f0"),
                                                $("f1").cast(BIGINT().notNull()),
                                                $("f2"),
                                                $("f3").cast(BIGINT().notNull())),
                                        "MAP[f0, CAST(f1 AS BIGINT), f2, CAST(f3 AS BIGINT)]",
                                        CollectionUtil.map(entry(1, 2L), entry(3, 4L)),
                                        DataTypes.MAP(INT().notNull(), BIGINT().notNull())
                                                .notNull()),
                                resultSpec(
                                        map($("f6"), $("f6")),
                                        "MAP[f6, f6]",
                                        Collections.singletonMap(true, true),
                                        DataTypes.MAP(BOOLEAN().notNull(), BOOLEAN().notNull())
                                                .notNull()),
                                resultSpec(
                                        map(
                                                $("f0"),
                                                $("f1").cast(DOUBLE().notNull()),
                                                $("f2"),
                                                $("f3").cast(FLOAT().notNull())),
                                        "MAP[f0, CAST(f1 AS DOUBLE), f2, CAST(f3 AS FLOAT)]",
                                        CollectionUtil.map(entry(1, 2d), entry(3, 4.0)),
                                        DataTypes.MAP(INT().notNull(), DOUBLE().notNull())
                                                .notNull()),
                                resultSpec(
                                        map($("f4"), $("f5")),
                                        "MAP[f4, f5]",
                                        Collections.singletonMap(
                                                new BigDecimal("1.2345"), new BigDecimal("1.2346")),
                                        DataTypes.MAP(
                                                        DECIMAL(10, 4).notNull(),
                                                        DECIMAL(10, 4).notNull())
                                                .notNull()),
                                resultSpec(
                                        map(map($("f0"), $("f1")), map($("f2"), $("f3"))),
                                        "MAP[MAP[f0, f1], MAP[f2, f3]]",
                                        Collections.singletonMap(
                                                Collections.singletonMap(1, 2),
                                                Collections.singletonMap(3, 4)),
                                        DataTypes.MAP(
                                                        MAP(
                                                                        DataTypes.INT().notNull(),
                                                                        DataTypes.INT().notNull())
                                                                .notNull(),
                                                        MAP(
                                                                        DataTypes.INT().notNull(),
                                                                        DataTypes.INT().notNull())
                                                                .notNull())
                                                .notNull())),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP)
                        .onFieldsWithData(
                                TEST_DATE_1,
                                TEST_DATE_2,
                                TEST_TIME_1,
                                TEST_TIME_2,
                                TEST_DATE_TIME_1,
                                TEST_DATE_TIME_2)
                        .andDataTypes(
                                DATE().notNull(),
                                DATE().notNull(),
                                TIME().notNull(),
                                TIME().notNull(),
                                TIMESTAMP().notNull(),
                                TIMESTAMP().notNull())
                        .testResult(
                                resultSpec(
                                        map($("f0"), $("f2"), $("f1"), $("f3")),
                                        "MAP[f0, f2, f1, f3]",
                                        CollectionUtil.map(
                                                entry(TEST_DATE_1, TEST_TIME_1),
                                                entry(TEST_DATE_2, TEST_TIME_2)),
                                        DataTypes.MAP(DATE().notNull(), TIME().notNull())
                                                .notNull()),
                                resultSpec(
                                        map($("f2"), $("f4"), $("f3"), $("f5")),
                                        "MAP[f2, f4, f3, f5]",
                                        CollectionUtil.map(
                                                entry(TEST_TIME_1, TEST_DATE_TIME_1),
                                                entry(TEST_TIME_2, TEST_DATE_TIME_2)),
                                        DataTypes.MAP(TIME().notNull(), TIMESTAMP().notNull())
                                                .notNull())),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP)
                        .onFieldsWithData(A, B, INTERVAL_1, INTERVAL_NULL)
                        .andDataTypes(
                                STRING().notNull(),
                                STRING().notNull(),
                                INTERVAL(MONTH()),
                                INTERVAL(MONTH()).nullable())
                        .testResult(
                                resultSpec(
                                        map($("f0"), $("f2"), $("f1"), $("f3")),
                                        "MAP[f0, f2, f1, f3]",
                                        CollectionUtil.map(
                                                entry(A, Period.ofMonths(INTERVAL_1)),
                                                entry(B, INTERVAL_NULL)),
                                        DataTypes.MAP(
                                                        STRING().notNull(),
                                                        INTERVAL(MONTH()).nullable())
                                                .notNull())));
    }
}
