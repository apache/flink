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
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Collections;
import java.util.Map;
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
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.mapFromArrays;
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
                        mapTestCases(),
                        mapKeysTestCases(),
                        mapValuesTestCases(),
                        mapEntriesTestCases(),
                        mapFromArraysTestCases())
                .flatMap(s -> s);
    }

    private Stream<TestSetSpec> mapTestCases() {
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

    private Stream<TestSetSpec> mapKeysTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_KEYS)
                        .onFieldsWithData(
                                null,
                                "item",
                                Collections.singletonMap(1, "value"),
                                Collections.singletonMap(new Integer[] {1, 2}, "value"))
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.STRING(),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                DataTypes.MAP(DataTypes.ARRAY(DataTypes.INT()), DataTypes.STRING()))
                        .testTableApiValidationError(
                                call("MAP_KEYS", $("f0"), $("f1")),
                                "Invalid function call:\nMAP_KEYS(BOOLEAN, STRING)")
                        .testResult(
                                map(
                                                $("f0").cast(DataTypes.BOOLEAN()),
                                                $("f1").cast(DataTypes.STRING()))
                                        .mapKeys(),
                                "MAP_KEYS(MAP[CAST(f0 AS BOOLEAN), CAST(f1 AS STRING)])",
                                new Boolean[] {null},
                                DataTypes.ARRAY(DataTypes.BOOLEAN()).notNull())
                        .testResult(
                                $("f2").mapKeys(),
                                "MAP_KEYS(f2)",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f3").mapKeys(),
                                "MAP_KEYS(f3)",
                                new Integer[][] {new Integer[] {1, 2}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))));
    }

    private Stream<TestSetSpec> mapValuesTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_VALUES)
                        .onFieldsWithData(
                                null,
                                "item",
                                Collections.singletonMap(1, "value1"),
                                Collections.singletonMap(
                                        3, Collections.singletonMap(true, "value2")))
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.STRING(),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                DataTypes.MAP(
                                        DataTypes.INT(),
                                        DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.STRING())))
                        .testTableApiValidationError(
                                call("MAP_VALUES", $("f0"), $("f1")),
                                "Invalid function call:\nMAP_VALUES(BOOLEAN, STRING)")
                        .testResult(
                                map(
                                                $("f1").cast(DataTypes.STRING()),
                                                $("f0").cast(DataTypes.BOOLEAN()))
                                        .mapValues(),
                                "MAP_VALUES(MAP[CAST(f1 AS STRING), CAST(f0 AS BOOLEAN)])",
                                new Boolean[] {null},
                                DataTypes.ARRAY(DataTypes.BOOLEAN()).notNull())
                        .testResult(
                                $("f2").mapValues(),
                                "MAP_VALUES(f2)",
                                new String[] {"value1"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f3").mapValues(),
                                "MAP_VALUES(f3)",
                                new Map[] {Collections.singletonMap(true, "value2")},
                                DataTypes.ARRAY(
                                        DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.STRING()))));
    }

    private Stream<TestSetSpec> mapEntriesTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ENTRIES)
                        .onFieldsWithData(
                                null,
                                "item",
                                Collections.singletonMap(1, "value1"),
                                Collections.singletonMap(
                                        3, Collections.singletonMap(true, "value2")))
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.STRING(),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                DataTypes.MAP(
                                        DataTypes.INT(),
                                        DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.STRING())))
                        .testTableApiValidationError(
                                call("MAP_ENTRIES", $("f0"), $("f1")),
                                "Invalid function call:\nMAP_ENTRIES(BOOLEAN, STRING)")
                        .testResult(
                                map(
                                                $("f0").cast(DataTypes.BOOLEAN()),
                                                $("f1").cast(DataTypes.STRING()))
                                        .mapEntries(),
                                "MAP_ENTRIES(MAP[CAST(f0 AS BOOLEAN), CAST(f1 AS STRING)])",
                                new Row[] {Row.of(null, "item")},
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("key", DataTypes.BOOLEAN()),
                                                        DataTypes.FIELD(
                                                                "value", DataTypes.STRING())))
                                        .notNull())
                        .testResult(
                                $("f2").mapEntries(),
                                "MAP_ENTRIES(f2)",
                                new Row[] {Row.of(1, "value1")},
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.INT()),
                                                DataTypes.FIELD("value", DataTypes.STRING()))))
                        .testResult(
                                $("f3").mapEntries(),
                                "MAP_ENTRIES(f3)",
                                new Row[] {Row.of(3, Collections.singletonMap(true, "value2"))},
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.INT()),
                                                DataTypes.FIELD(
                                                        "value",
                                                        DataTypes.MAP(
                                                                DataTypes.BOOLEAN(),
                                                                DataTypes.STRING()))))));
    }

    private Stream<TestSetSpec> mapFromArraysTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS, "Invalid input")
                        .onFieldsWithData(null, null, new Integer[] {1}, new Integer[] {1, 2})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.BOOLEAN()),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiRuntimeError(
                                mapFromArrays($("f2"), $("f3")),
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 1 is not equal to the length of the values array 2")
                        .testSqlRuntimeError(
                                "MAP_FROM_ARRAYS(array[1, 2, 3], array[1, 2])",
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 3 is not equal to the length of the values array 2")
                        .testResult(
                                mapFromArrays($("f0"), $("f1")),
                                "MAP_FROM_ARRAYS(f0, f1)",
                                null,
                                DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.STRING())),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS)
                        .onFieldsWithData(
                                new Integer[] {1, 2},
                                new String[] {"one", "two"},
                                new Integer[][] {new Integer[] {1, 2}, new Integer[] {3, 4}})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                        .testResult(
                                mapFromArrays($("f0"), $("f1")),
                                "MAP_FROM_ARRAYS(f0, f1)",
                                CollectionUtil.map(entry(1, "one"), entry(2, "two")),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .testTableApiResult(
                                mapFromArrays($("f1"), $("f2")),
                                CollectionUtil.map(
                                        entry("one", new Integer[] {1, 2}),
                                        entry("two", new Integer[] {3, 4})),
                                DataTypes.MAP(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));
    }
}
