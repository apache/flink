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

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.mapFromArrays;

/** Tests for collections {@link BuiltInFunctionDefinitions}. */
public class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_KEYS)
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
                        .testTableApiError(
                                call("MAP_KEYS", $("f0"), $("f1")),
                                "Invalid function call:\nMAP_KEYS(BOOLEAN, STRING)")
                        .testResult(
                                map(
                                                $("f0").cast(DataTypes.BOOLEAN()),
                                                $("f1").cast(DataTypes.INT()))
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
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_VALUES)
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
                        .testTableApiError(
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
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS, "Invalid input")
                        .onFieldsWithData(null, null, new Integer[] {1}, new Integer[] {1, 2})
                        .andDataTypes(
                                DataTypes.BOOLEAN().nullable(),
                                DataTypes.INT().nullable(),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiError(
                                mapFromArrays($("f0"), $("f1")),
                                "Invalid function call:\nMAP_FROM_ARRAYS(BOOLEAN, INT)")
                        .testTableApiError(
                                mapFromArrays($("f2"), $("f3")),
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 1 is not equal to the length of the values array 2")
                        .testSqlError(
                                "MAP_FROM_ARRAYS(array[1], array[1, 2])",
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 1 is not equal to the length of the values array 2")
                        .testSqlError(
                                "MAP_FROM_ARRAYS(array[1, 2, 3], array[1, 2])",
                                "Invalid function MAP_FROM_ARRAYS call:\n"
                                        + "The length of the keys array 3 is not equal to the length of the values array 2"),
                TestSpec.forFunction(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS)
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
