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

import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.mapFromArrays;
import static org.apache.flink.table.api.Expressions.row;

/** Tests for {@link BuiltInFunctionDefinitions} around arrays. */
class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_CONTAINS)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3},
                                null,
                                new String[] {"Hello", "World"},
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[] {1, null, 3},
                                new Integer[] {1, 2, 3})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING()).notNull(),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        // ARRAY<INT>
                        .testResult(
                                $("f0").arrayContains(2),
                                "ARRAY_CONTAINS(f0, 2)",
                                true,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f0").arrayContains(42),
                                "ARRAY_CONTAINS(f0, 42)",
                                false,
                                DataTypes.BOOLEAN().nullable())
                        // ARRAY<INT> of null value
                        .testResult(
                                $("f1").arrayContains(12),
                                "ARRAY_CONTAINS(f1, 12)",
                                null,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f1").arrayContains(null),
                                "ARRAY_CONTAINS(f1, NULL)",
                                null,
                                DataTypes.BOOLEAN().nullable())
                        // ARRAY<STRING> NOT NULL
                        .testResult(
                                $("f2").arrayContains("Hello"),
                                "ARRAY_CONTAINS(f2, 'Hello')",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        // ARRAY<ROW<BOOLEAN, DATE>>
                        .testResult(
                                $("f3").arrayContains(row(true, LocalDate.of(1990, 10, 14))),
                                "ARRAY_CONTAINS(f3, (TRUE, DATE '1990-10-14'))",
                                true,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").arrayContains(row(false, LocalDate.of(1990, 10, 14))),
                                "ARRAY_CONTAINS(f3, (FALSE, DATE '1990-10-14'))",
                                false,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").arrayContains(null),
                                "ARRAY_CONTAINS(f3, null)",
                                true,
                                DataTypes.BOOLEAN())
                        // ARRAY<INT> with null elements
                        .testResult(
                                $("f4").arrayContains(null),
                                "ARRAY_CONTAINS(f4, NULL)",
                                true,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f5").arrayContains(lit(null, DataTypes.INT())),
                                "ARRAY_CONTAINS(f5, CAST(NULL AS INT))",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f5").arrayContains(lit(4, DataTypes.INT().notNull())),
                                "ARRAY_CONTAINS(f5, 4)",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f5").arrayContains(lit(3, DataTypes.INT().notNull())),
                                "ARRAY_CONTAINS(f5, 3)",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        // invalid signatures
                        .testSqlValidationError(
                                "ARRAY_CONTAINS(f0, TRUE)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_CONTAINS(haystack <ARRAY>, needle <ARRAY ELEMENT>)")
                        .testTableApiValidationError(
                                $("f0").arrayContains(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_CONTAINS(haystack <ARRAY>, needle <ARRAY ELEMENT>)"),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_DISTINCT)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3},
                                new Integer[] {null, 1, 2, 3, 4, 5, 4, 3, 2, 1, null},
                                null,
                                new String[] {"Hello", "Hello", "Hello"},
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                })
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING()).notNull(),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())))
                        .testResult(
                                $("f0").arrayDistinct(),
                                "ARRAY_DISTINCT(f0)",
                                new Integer[] {1, 2, 3},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f1").arrayDistinct(),
                                "ARRAY_DISTINCT(f1)",
                                new Integer[] {null, 1, 2, 3, 4, 5},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f2").arrayDistinct(),
                                "ARRAY_DISTINCT(f2)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f3").arrayDistinct(),
                                "ARRAY_DISTINCT(f3)",
                                new String[] {"Hello"},
                                DataTypes.ARRAY(DataTypes.STRING()).notNull())
                        .testResult(
                                $("f4").arrayDistinct(),
                                "ARRAY_DISTINCT(f4)",
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE()))),
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
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))),
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
                                        DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.STRING()))),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS, "Invalid input")
                        .onFieldsWithData(null, null, new Integer[] {1}, new Integer[] {1, 2})
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.INT().nullable(),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiRuntimeError(
                                mapFromArrays($("f2"), $("f3")),
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 1 is not equal to the length of the values array 2")
                        .testSqlRuntimeError(
                                "MAP_FROM_ARRAYS(array[1, 2, 3], array[1, 2])",
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 3 is not equal to the length of the values array 2"),
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
                                of(1, "one", 2, "two"),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .testTableApiResult(
                                mapFromArrays($("f1"), $("f2")),
                                of("one", new Integer[] {1, 2}, "two", new Integer[] {3, 4}),
                                DataTypes.MAP(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));
    }

    // --------------------------------------------------------------------------------------------

    private static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2) {
        Map<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }
}
