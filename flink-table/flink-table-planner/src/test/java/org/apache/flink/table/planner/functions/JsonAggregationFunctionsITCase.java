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

import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.jsonArrayAgg;
import static org.apache.flink.table.api.Expressions.jsonObjectAgg;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;

/** Tests for built-in JSON aggregation functions. */
class JsonAggregationFunctionsITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    public Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(
                // JSON_OBJECTAGG
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECTAGG_NULL_ON_NULL)
                        .withDescription("Basic Aggregation")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(INSERT, "C", 3)))
                        .testResult(
                                source -> "SELECT JSON_OBJECTAGG(f0 VALUE f1) FROM " + source,
                                source ->
                                        source.select(
                                                jsonObjectAgg(JsonOnNull.NULL, $("f0"), $("f1"))),
                                ROW(VARCHAR(2000).notNull()),
                                ROW(STRING().notNull()),
                                Collections.singletonList(Row.of("{\"A\":1,\"B\":null,\"C\":3}"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECTAGG_ABSENT_ON_NULL)
                        .withDescription("Omits NULLs")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(INSERT, "C", 3)))
                        .testResult(
                                source ->
                                        "SELECT JSON_OBJECTAGG(f0 VALUE f1 ABSENT ON NULL) FROM "
                                                + source,
                                source ->
                                        source.select(
                                                jsonObjectAgg(JsonOnNull.ABSENT, $("f0"), $("f1"))),
                                ROW(VARCHAR(2000).notNull()),
                                ROW(STRING().notNull()),
                                Collections.singletonList(Row.of("{\"A\":1,\"C\":3}"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECTAGG_NULL_ON_NULL)
                        .withDescription("Retractions")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "B", 2),
                                        Row.ofKind(INSERT, "C", 3),
                                        Row.ofKind(DELETE, "B", 2)))
                        .testResult(
                                source -> "SELECT JSON_OBJECTAGG(f0 VALUE f1) FROM " + source,
                                source ->
                                        source.select(
                                                jsonObjectAgg(JsonOnNull.NULL, $("f0"), $("f1"))),
                                ROW(VARCHAR(2000).notNull()),
                                ROW(STRING().notNull()),
                                Collections.singletonList(Row.of("{\"A\":1,\"C\":3}"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECTAGG_NULL_ON_NULL)
                        .withDescription("Group Aggregation")
                        .withSource(
                                ROW(INT(), STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, 1, "A", 0),
                                        Row.ofKind(INSERT, 1, "B", 0),
                                        Row.ofKind(INSERT, 2, "A", 0),
                                        Row.ofKind(INSERT, 2, "C", 0)))
                        .testResult(
                                source ->
                                        "SELECT f0, JSON_OBJECTAGG(f1 VALUE f2) FROM "
                                                + source
                                                + " GROUP BY f0",
                                source ->
                                        source.groupBy($("f0"))
                                                .select(
                                                        $("f0"),
                                                        jsonObjectAgg(
                                                                JsonOnNull.NULL, $("f1"), $("f2"))),
                                ROW(INT(), VARCHAR(2000).notNull()),
                                ROW(INT(), STRING().notNull()),
                                Arrays.asList(
                                        Row.of(1, "{\"A\":0,\"B\":0}"),
                                        Row.of(2, "{\"A\":0,\"C\":0}"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECTAGG_NULL_ON_NULL)
                        .withDescription("Basic Json Aggregation With Other Aggs")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(INSERT, "C", 3)))
                        .testResult(
                                source ->
                                        "SELECT max(f1), JSON_OBJECTAGG(f0 VALUE f1) FROM "
                                                + source,
                                source ->
                                        source.select(
                                                $("f1").max(),
                                                jsonObjectAgg(JsonOnNull.NULL, $("f0"), $("f1"))),
                                ROW(INT(), VARCHAR(2000).notNull()),
                                ROW(INT(), STRING().notNull()),
                                Collections.singletonList(
                                        Row.of(3, "{\"A\":1,\"B\":null,\"C\":3}"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECTAGG_NULL_ON_NULL)
                        .withDescription("Group Json Aggregation With Other Aggs")
                        .withSource(
                                ROW(INT(), STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, 1, "A", 1),
                                        Row.ofKind(INSERT, 1, "B", 3),
                                        Row.ofKind(INSERT, 2, "A", 2),
                                        Row.ofKind(INSERT, 2, "C", 5)))
                        .testResult(
                                source ->
                                        "SELECT f0, JSON_OBJECTAGG(f1 VALUE f2), max(f2) FROM "
                                                + source
                                                + " GROUP BY f0",
                                source ->
                                        source.groupBy($("f0"))
                                                .select(
                                                        $("f0"),
                                                        jsonObjectAgg(
                                                                JsonOnNull.NULL, $("f1"), $("f2")),
                                                        $("f2").max()),
                                ROW(INT(), VARCHAR(2000).notNull(), INT()),
                                ROW(INT(), STRING().notNull(), INT()),
                                Arrays.asList(
                                        Row.of(1, "{\"A\":1,\"B\":3}", 3),
                                        Row.of(2, "{\"A\":2,\"C\":5}", 5))),

                // JSON_ARRAYAGG
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_ARRAYAGG_ABSENT_ON_NULL)
                        .withDescription("Basic Aggregation")
                        .withSource(
                                ROW(STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A"),
                                        Row.ofKind(INSERT, (String) null),
                                        Row.ofKind(INSERT, "C")))
                        .testResult(
                                source -> "SELECT JSON_ARRAYAGG(f0) FROM " + source,
                                source -> source.select(jsonArrayAgg(JsonOnNull.ABSENT, $("f0"))),
                                ROW(VARCHAR(2000).notNull()),
                                ROW(STRING().notNull()),
                                Collections.singletonList(Row.of("[\"A\",\"C\"]"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_ARRAYAGG_NULL_ON_NULL)
                        .withDescription("Keeps NULLs")
                        .withSource(
                                ROW(STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A"),
                                        Row.ofKind(INSERT, (String) null),
                                        Row.ofKind(INSERT, "C")))
                        .testResult(
                                source -> "SELECT JSON_ARRAYAGG(f0 NULL ON NULL) FROM " + source,
                                source -> source.select(jsonArrayAgg(JsonOnNull.NULL, $("f0"))),
                                ROW(VARCHAR(2000).notNull()),
                                ROW(STRING().notNull()),
                                Collections.singletonList(Row.of("[\"A\",null,\"C\"]"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_ARRAYAGG_ABSENT_ON_NULL)
                        .withDescription("Retractions")
                        .withSource(
                                ROW(INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, 1),
                                        Row.ofKind(INSERT, 2),
                                        Row.ofKind(INSERT, 3),
                                        Row.ofKind(DELETE, 2)))
                        .testResult(
                                source -> "SELECT JSON_ARRAYAGG(f0) FROM " + source,
                                source -> source.select(jsonArrayAgg(JsonOnNull.ABSENT, $("f0"))),
                                ROW(VARCHAR(2000).notNull()),
                                ROW(STRING().notNull()),
                                Collections.singletonList(Row.of("[1,3]"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_ARRAYAGG_ABSENT_ON_NULL)
                        .withDescription("Basic Array Aggregation With Other Aggs")
                        .withSource(
                                ROW(STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A"),
                                        Row.ofKind(INSERT, (String) null),
                                        Row.ofKind(INSERT, "C")))
                        .testResult(
                                source -> "SELECT max(f0), JSON_ARRAYAGG(f0) FROM " + source,
                                source ->
                                        source.select(
                                                $("f0").max(),
                                                jsonArrayAgg(JsonOnNull.ABSENT, $("f0"))),
                                ROW(STRING(), VARCHAR(2000).notNull()),
                                ROW(STRING(), STRING().notNull()),
                                Collections.singletonList(Row.of("C", "[\"A\",\"C\"]"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_ARRAYAGG_ABSENT_ON_NULL)
                        .withDescription("Group Array Aggregation With Other Aggs")
                        .withSource(
                                ROW(INT(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, 1, "A"),
                                        Row.ofKind(INSERT, 1, null),
                                        Row.ofKind(INSERT, 2, "C"),
                                        Row.ofKind(INSERT, 2, "D")))
                        .testResult(
                                source ->
                                        "SELECT f0, max(f1), JSON_ARRAYAGG(f1)FROM "
                                                + source
                                                + " GROUP BY f0",
                                source ->
                                        source.groupBy($("f0"))
                                                .select(
                                                        $("f0"),
                                                        $("f1").max(),
                                                        jsonArrayAgg(JsonOnNull.ABSENT, $("f1"))),
                                ROW(INT(), STRING(), VARCHAR(2000).notNull()),
                                ROW(INT(), STRING(), STRING().notNull()),
                                Arrays.asList(
                                        Row.of(1, "A", "[\"A\"]"),
                                        Row.of(2, "D", "[\"C\",\"D\"]"))));
    }
}
