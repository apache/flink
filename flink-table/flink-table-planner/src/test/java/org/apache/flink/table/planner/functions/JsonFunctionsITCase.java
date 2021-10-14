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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.JsonType;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import org.apache.commons.io.IOUtils;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.jsonArray;
import static org.apache.flink.table.api.Expressions.jsonObject;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.JsonQueryOnEmptyOrError.EMPTY_ARRAY;
import static org.apache.flink.table.api.JsonQueryOnEmptyOrError.EMPTY_OBJECT;
import static org.apache.flink.table.api.JsonQueryOnEmptyOrError.ERROR;
import static org.apache.flink.table.api.JsonQueryOnEmptyOrError.NULL;
import static org.apache.flink.table.api.JsonQueryWrapper.CONDITIONAL_ARRAY;
import static org.apache.flink.table.api.JsonQueryWrapper.UNCONDITIONAL_ARRAY;
import static org.apache.flink.table.api.JsonQueryWrapper.WITHOUT_ARRAY;

/** Tests for built-in JSON functions. */
public class JsonFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() throws Exception {
        final List<TestSpec> testCases = new ArrayList<>();
        testCases.add(jsonExistsSpec());
        testCases.add(jsonValueSpec());
        testCases.addAll(isJsonSpec());
        testCases.addAll(jsonQuerySpec());
        testCases.addAll(jsonObjectSpec());
        testCases.addAll(jsonArraySpec());

        return testCases;
    }

    private static TestSpec jsonExistsSpec() throws Exception {
        final String jsonValue = getJsonFromResource("/json/json-exists.json");
        return TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_EXISTS)
                .onFieldsWithData(jsonValue)
                .andDataTypes(STRING())
                .testResult(
                        // NULL
                        resultSpec(
                                nullOf(STRING()).jsonExists("lax $"),
                                "JSON_EXISTS(CAST(NULL AS STRING), 'lax $')",
                                null,
                                BOOLEAN()),

                        // Path variants
                        resultSpec(
                                $("f0").jsonExists("lax $"),
                                "JSON_EXISTS(f0, 'lax $')",
                                true,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonExists("lax $.type"),
                                "JSON_EXISTS(f0, 'lax $.type')",
                                true,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonExists("lax $.author.address.city"),
                                "JSON_EXISTS(f0, 'lax $.author.address.city')",
                                true,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonExists("lax $.metadata.tags[0]"),
                                "JSON_EXISTS(f0, 'lax $.metadata.tags[0]')",
                                true,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonExists("lax $.metadata.tags[3]"),
                                "JSON_EXISTS(f0, 'lax $.metadata.tags[3]')",
                                false,
                                BOOLEAN()),
                        // This should pass, but is broken due to
                        // https://issues.apache.org/jira/browse/CALCITE-4717.
                        // new TestSpecColumn(
                        //        $("f0").jsonExists("lax $.metadata.references.url"),
                        //        "JSON_EXISTS(f0, 'lax $.metadata.references.url')",
                        //        true,
                        //        DataTypes.BOOLEAN())
                        resultSpec(
                                $("f0").jsonExists("lax $.metadata.references[0].url"),
                                "JSON_EXISTS(f0, 'lax $.metadata.references[0].url')",
                                true,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonExists("lax $.metadata.references[0].invalid"),
                                "JSON_EXISTS(f0, 'lax $.metadata.references[0].invalid')",
                                false,
                                BOOLEAN()),

                        // ON ERROR
                        resultSpec(
                                $("f0").jsonExists("strict $.invalid", JsonExistsOnError.TRUE),
                                "JSON_EXISTS(f0, 'strict $.invalid' TRUE ON ERROR)",
                                true,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonExists("strict $.invalid", JsonExistsOnError.FALSE),
                                "JSON_EXISTS(f0, 'strict $.invalid' FALSE ON ERROR)",
                                false,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonExists("strict $.invalid", JsonExistsOnError.UNKNOWN),
                                "JSON_EXISTS(f0, 'strict $.invalid' UNKNOWN ON ERROR)",
                                null,
                                BOOLEAN()))
                .testSqlRuntimeError(
                        "JSON_EXISTS(f0, 'strict $.invalid' ERROR ON ERROR)",
                        "No results for path: $['invalid']")
                .testTableApiRuntimeError(
                        $("f0").jsonExists("strict $.invalid", JsonExistsOnError.ERROR),
                        "No results for path: $['invalid']");
    }

    private static TestSpec jsonValueSpec() throws Exception {
        final String jsonValue = getJsonFromResource("/json/json-value.json");
        return TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_VALUE)
                .onFieldsWithData(jsonValue)
                .andDataTypes(STRING())
                .testResult(
                        // NULL and invalid types
                        resultSpec(
                                lit(null, STRING()).jsonValue("lax $"),
                                "JSON_VALUE(CAST(NULL AS STRING), 'lax $')",
                                null,
                                STRING(),
                                VARCHAR(2000)),

                        // RETURNING + Supported Data Types
                        resultSpec(
                                $("f0").jsonValue("$.type"),
                                "JSON_VALUE(f0, '$.type')",
                                "account",
                                STRING(),
                                VARCHAR(2000)),
                        resultSpec(
                                $("f0").jsonValue("$.activated", BOOLEAN()),
                                "JSON_VALUE(f0, '$.activated' RETURNING BOOLEAN)",
                                true,
                                BOOLEAN()),
                        resultSpec(
                                $("f0").jsonValue("$.age", INT()),
                                "JSON_VALUE(f0, '$.age' RETURNING INT)",
                                42,
                                INT()),
                        resultSpec(
                                $("f0").jsonValue("$.balance", DOUBLE()),
                                "JSON_VALUE(f0, '$.balance' RETURNING DOUBLE)",
                                13.37,
                                DOUBLE()),

                        // ON EMPTY / ON ERROR
                        resultSpec(
                                $("f0").jsonValue(
                                                "lax $.invalid",
                                                STRING(),
                                                JsonValueOnEmptyOrError.NULL,
                                                null,
                                                JsonValueOnEmptyOrError.ERROR,
                                                null),
                                "JSON_VALUE(f0, 'lax $.invalid' NULL ON EMPTY ERROR ON ERROR)",
                                null,
                                STRING(),
                                VARCHAR(2000)),
                        resultSpec(
                                $("f0").jsonValue(
                                                "lax $.invalid",
                                                INT(),
                                                JsonValueOnEmptyOrError.DEFAULT,
                                                42,
                                                JsonValueOnEmptyOrError.ERROR,
                                                null),
                                "JSON_VALUE(f0, 'lax $.invalid' RETURNING INTEGER DEFAULT 42 ON EMPTY ERROR ON ERROR)",
                                42,
                                INT()),
                        resultSpec(
                                $("f0").jsonValue(
                                                "strict $.invalid",
                                                STRING(),
                                                JsonValueOnEmptyOrError.ERROR,
                                                null,
                                                JsonValueOnEmptyOrError.NULL,
                                                null),
                                "JSON_VALUE(f0, 'strict $.invalid' ERROR ON EMPTY NULL ON ERROR)",
                                null,
                                STRING(),
                                VARCHAR(2000)),
                        resultSpec(
                                $("f0").jsonValue(
                                                "strict $.invalid",
                                                INT(),
                                                JsonValueOnEmptyOrError.NULL,
                                                null,
                                                JsonValueOnEmptyOrError.DEFAULT,
                                                42),
                                "JSON_VALUE(f0, 'strict $.invalid' RETURNING INTEGER NULL ON EMPTY DEFAULT 42 ON ERROR)",
                                42,
                                INT()));
    }

    private static List<TestSpec> isJsonSpec() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData(1)
                        .andDataTypes(INT())
                        .testSqlValidationError(
                                "f0 IS JSON",
                                "Cannot apply 'IS JSON VALUE' to arguments of type '<INTEGER> IS JSON VALUE'. "
                                        + "Supported form(s): '<CHARACTER> IS JSON VALUE'")
                        .testTableApiValidationError(
                                $("f0").isJson(),
                                String.format("Invalid function call:%nIS_JSON(INT)")),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData((String) null)
                        .andDataTypes(STRING())
                        .testResult($("f0").isJson(), "f0 IS JSON", false, BOOLEAN().notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData("a")
                        .andDataTypes(STRING())
                        .testResult($("f0").isJson(), "f0 IS JSON", false, BOOLEAN().notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData("\"a\"")
                        .andDataTypes(STRING())
                        .testResult(
                                resultSpec(
                                        $("f0").isJson(), "f0 IS JSON", true, BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.VALUE),
                                        "f0 IS JSON VALUE",
                                        true,
                                        BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.SCALAR),
                                        "f0 IS JSON SCALAR",
                                        true,
                                        BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.ARRAY),
                                        "f0 IS JSON ARRAY",
                                        false,
                                        BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.OBJECT),
                                        "f0 IS JSON OBJECT",
                                        false,
                                        BOOLEAN().notNull())),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData("{}")
                        .andDataTypes(STRING())
                        .testResult(
                                resultSpec(
                                        $("f0").isJson(), "f0 IS JSON", true, BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.VALUE),
                                        "f0 IS JSON VALUE",
                                        true,
                                        BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.SCALAR),
                                        "f0 IS JSON SCALAR",
                                        false,
                                        BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.ARRAY),
                                        "f0 IS JSON ARRAY",
                                        false,
                                        BOOLEAN().notNull()),
                                resultSpec(
                                        $("f0").isJson(JsonType.OBJECT),
                                        "f0 IS JSON OBJECT",
                                        true,
                                        BOOLEAN().notNull())));
    }

    private static List<TestSpec> jsonQuerySpec() throws Exception {
        final String jsonValue = getJsonFromResource("/json/json-query.json");
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_QUERY)
                        .onFieldsWithData((String) null)
                        .andDataTypes(STRING())
                        .testResult(
                                $("f0").jsonQuery("$"),
                                "JSON_QUERY(f0, '$')",
                                null,
                                STRING(),
                                VARCHAR(2000)),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_QUERY)
                        .onFieldsWithData(jsonValue)
                        .andDataTypes(STRING())

                        // Wrapping Behavior

                        .testResult(
                                $("f0").jsonQuery("$.a1", WITHOUT_ARRAY),
                                "JSON_QUERY(f0, '$.a1' WITHOUT WRAPPER)",
                                "[]",
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.a1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.a1' WITH CONDITIONAL WRAPPER)",
                                "[]",
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.o1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.o1' WITH CONDITIONAL WRAPPER)",
                                "[{}]",
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.a1", UNCONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.a1' WITH UNCONDITIONAL WRAPPER)",
                                "[[]]",
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.n1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.n1' WITH CONDITIONAL WRAPPER)",
                                "[1]",
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.s1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.s1' WITH CONDITIONAL WRAPPER)",
                                "[\"Test\"]",
                                STRING(),
                                VARCHAR(2000))

                        // Empty Behavior

                        .testResult(
                                $("f0").jsonQuery("lax $.err1", WITHOUT_ARRAY, NULL, NULL),
                                "JSON_QUERY(f0, 'lax $.err1' NULL ON EMPTY)",
                                null,
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("lax $.err2", WITHOUT_ARRAY, EMPTY_ARRAY, NULL),
                                "JSON_QUERY(f0, 'lax $.err2' EMPTY ARRAY ON EMPTY)",
                                "[]",
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("lax $.err3", WITHOUT_ARRAY, EMPTY_OBJECT, NULL),
                                "JSON_QUERY(f0, 'lax $.err3' EMPTY OBJECT ON EMPTY)",
                                "{}",
                                STRING(),
                                VARCHAR(2000))
                        .testSqlRuntimeError(
                                "JSON_QUERY(f0, 'lax $.err4' ERROR ON EMPTY)",
                                "Empty result of JSON_QUERY function is not allowed")
                        .testTableApiRuntimeError(
                                $("f0").jsonQuery("lax $.err5", WITHOUT_ARRAY, ERROR, NULL),
                                "Empty result of JSON_QUERY function is not allowed")

                        // Error Behavior

                        .testResult(
                                $("f0").jsonQuery("strict $.err6", WITHOUT_ARRAY, NULL, NULL),
                                "JSON_QUERY(f0, 'strict $.err6' NULL ON ERROR)",
                                null,
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery(
                                                "strict $.err7", WITHOUT_ARRAY, NULL, EMPTY_ARRAY),
                                "JSON_QUERY(f0, 'strict $.err7' EMPTY ARRAY ON ERROR)",
                                "[]",
                                STRING(),
                                VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery(
                                                "strict $.err8", WITHOUT_ARRAY, NULL, EMPTY_OBJECT),
                                "JSON_QUERY(f0, 'strict $.err8' EMPTY OBJECT ON ERROR)",
                                "{}",
                                STRING(),
                                VARCHAR(2000))
                        .testSqlRuntimeError(
                                "JSON_QUERY(f0, 'strict $.err9' ERROR ON ERROR)",
                                "No results for path")
                        .testTableApiRuntimeError(
                                $("f0").jsonQuery("strict $.err10", WITHOUT_ARRAY, NULL, ERROR),
                                "No results for path"));
    }

    private static List<TestSpec> jsonObjectSpec() {
        final Map<String, String> mapData = new HashMap<>();
        mapData.put("M1", "V1");
        mapData.put("M2", "V2");

        final Map<String, Integer> multisetData = new HashMap<>();
        multisetData.put("M1", 1);
        multisetData.put("M2", 2);

        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECT)
                        .onFieldsWithData(0)
                        .testResult(
                                resultSpec(
                                        jsonObject(JsonOnNull.NULL),
                                        "JSON_OBJECT()",
                                        "{}",
                                        STRING().notNull(),
                                        VARCHAR(2000).notNull()),
                                resultSpec(
                                        jsonObject(JsonOnNull.NULL, "K", nullOf(STRING())),
                                        "JSON_OBJECT(KEY 'K' VALUE CAST(NULL AS STRING) NULL ON NULL)",
                                        "{\"K\":null}",
                                        STRING().notNull(),
                                        VARCHAR(2000).notNull()),
                                resultSpec(
                                        jsonObject(JsonOnNull.ABSENT, "K", nullOf(STRING())),
                                        "JSON_OBJECT(KEY 'K' VALUE CAST(NULL AS STRING) ABSENT ON NULL)",
                                        "{}",
                                        STRING().notNull(),
                                        VARCHAR(2000).notNull())),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_OBJECT)
                        .onFieldsWithData(
                                "V",
                                true,
                                1,
                                1.23d,
                                1.23,
                                LocalDateTime.parse("1990-06-02T13:37:42.001"),
                                Instant.parse("1990-06-02T13:37:42.001Z"),
                                Arrays.asList("A1", "A2", "A3"),
                                Row.of("R1", Instant.parse("1990-06-02T13:37:42.001Z")),
                                mapData,
                                multisetData,
                                "Test".getBytes(StandardCharsets.UTF_8),
                                "Test".getBytes(StandardCharsets.UTF_8),
                                Row.of(Collections.singletonList(Row.of(1, 2))))
                        .andDataTypes(
                                STRING(),
                                BOOLEAN(),
                                INT(),
                                DOUBLE(),
                                DECIMAL(3, 2),
                                TIMESTAMP(3),
                                TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                                ARRAY(STRING()),
                                ROW(STRING(), TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                                MAP(STRING(), STRING()),
                                MAP(STRING(), INT()),
                                BINARY(4),
                                VARBINARY(4),
                                ROW(ARRAY(ROW(INT(), INT()))))
                        .withFunction(CreateMultiset.class)
                        .testResult(
                                jsonObject(
                                        JsonOnNull.NULL,
                                        "K0",
                                        $("f0"),
                                        "K1",
                                        $("f1"),
                                        "K2",
                                        $("f2"),
                                        "K3",
                                        $("f3"),
                                        "K4",
                                        $("f4"),
                                        "K5",
                                        $("f5"),
                                        "K6",
                                        $("f6"),
                                        "K7",
                                        $("f7"),
                                        "K8",
                                        $("f8"),
                                        "K9",
                                        $("f9"),
                                        "K10",
                                        call("CreateMultiset", $("f10")),
                                        "K11",
                                        $("f11"),
                                        "K12",
                                        $("f12"),
                                        "K13",
                                        $("f13"),
                                        "K14",
                                        jsonObject(JsonOnNull.NULL, "A", "B"),
                                        "K15",
                                        jsonArray(
                                                JsonOnNull.NULL,
                                                "A",
                                                jsonObject(JsonOnNull.NULL, "K", "V"))),
                                "JSON_OBJECT("
                                        + "'K0' VALUE f0, "
                                        + "'K1' VALUE f1, "
                                        + "'K2' VALUE f2, "
                                        + "'K3' VALUE f3, "
                                        + "'K4' VALUE f4, "
                                        + "'K5' VALUE f5, "
                                        + "'K6' VALUE f6, "
                                        + "'K7' VALUE f7, "
                                        + "'K8' VALUE f8, "
                                        + "'K9' VALUE f9, "
                                        + "'K10' VALUE CreateMultiset(f10), "
                                        + "'K11' VALUE f11, "
                                        + "'K12' VALUE f12, "
                                        + "'K13' VALUE f13, "
                                        + "'K14' VALUE JSON_OBJECT(KEY 'A' VALUE 'B'), "
                                        + "'K15' VALUE JSON_ARRAY('A', JSON_OBJECT('K' VALUE 'V'))"
                                        + ")",
                                "{"
                                        + "\"K0\":\"V\","
                                        + "\"K1\":true,"
                                        + "\"K2\":1,"
                                        + "\"K3\":1.23,"
                                        + "\"K4\":1.23,"
                                        + "\"K5\":\"1990-06-02T13:37:42.001\","
                                        + "\"K6\":\"1990-06-02T13:37:42.001Z\","
                                        + "\"K7\":[\"A1\",\"A2\",\"A3\"],"
                                        + "\"K8\":{\"f0\":\"R1\",\"f1\":\"1990-06-02T13:37:42.001Z\"},"
                                        + "\"K9\":{\"M1\":\"V1\",\"M2\":\"V2\"},"
                                        + "\"K10\":{\"M1\":1,\"M2\":2},"
                                        + "\"K11\":\"VGVzdA==\","
                                        + "\"K12\":\"VGVzdA==\","
                                        + "\"K13\":{\"f0\":[{\"f0\":1,\"f1\":2}]},"
                                        + "\"K14\":{\"A\":\"B\"},"
                                        + "\"K15\":[\"A\",{\"K\":\"V\"}]"
                                        + "}",
                                STRING().notNull(),
                                VARCHAR(2000).notNull()));
    }

    private static List<TestSpec> jsonArraySpec() {
        final Map<String, String> mapData = new HashMap<>();
        mapData.put("M1", "V1");
        mapData.put("M2", "V2");

        final Map<String, Integer> multisetData = new HashMap<>();
        multisetData.put("M1", 1);
        multisetData.put("M2", 2);

        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_ARRAY)
                        .onFieldsWithData(0)
                        .testResult(
                                jsonArray(JsonOnNull.NULL),
                                "JSON_ARRAY()",
                                "[]",
                                STRING().notNull(),
                                VARCHAR(2000).notNull())
                        .testResult(
                                jsonArray(JsonOnNull.NULL, nullOf(STRING())),
                                "JSON_ARRAY(CAST(NULL AS STRING) NULL ON NULL)",
                                "[null]",
                                STRING().notNull(),
                                VARCHAR(2000).notNull())
                        .testResult(
                                jsonArray(JsonOnNull.ABSENT, nullOf(STRING())),
                                "JSON_ARRAY(CAST(NULL AS STRING) ABSENT ON NULL)",
                                "[]",
                                STRING().notNull(),
                                VARCHAR(2000).notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_ARRAY)
                        .onFieldsWithData(
                                "V",
                                true,
                                1,
                                1.23d,
                                1.23,
                                LocalDateTime.parse("1990-06-02T13:37:42.001"),
                                Instant.parse("1990-06-02T13:37:42.001Z"),
                                Arrays.asList("A1", "A2", "A3"),
                                Row.of("R1", Instant.parse("1990-06-02T13:37:42.001Z")),
                                mapData,
                                multisetData,
                                "Test".getBytes(StandardCharsets.UTF_8),
                                "Test".getBytes(StandardCharsets.UTF_8),
                                Row.of(Collections.singletonList(Row.of(1, 2))))
                        .andDataTypes(
                                STRING(),
                                BOOLEAN(),
                                INT(),
                                DOUBLE(),
                                DECIMAL(3, 2),
                                TIMESTAMP(3),
                                TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                                ARRAY(STRING()),
                                ROW(STRING(), TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                                MAP(STRING(), STRING()),
                                MAP(STRING(), INT()),
                                BINARY(4),
                                VARBINARY(4),
                                ROW(ARRAY(ROW(INT(), INT()))))
                        .withFunction(CreateMultiset.class)
                        .testResult(
                                jsonArray(
                                        JsonOnNull.NULL,
                                        $("f0"),
                                        $("f1"),
                                        $("f2"),
                                        $("f3"),
                                        $("f4"),
                                        $("f5"),
                                        $("f6"),
                                        $("f7"),
                                        $("f8"),
                                        $("f9"),
                                        call("CreateMultiset", $("f10")),
                                        $("f11"),
                                        $("f12"),
                                        $("f13"),
                                        jsonArray(JsonOnNull.NULL, "V"),
                                        jsonObject(
                                                JsonOnNull.NULL,
                                                "K",
                                                jsonArray(JsonOnNull.NULL, "V"))),
                                "JSON_ARRAY("
                                        + "f0, "
                                        + "f1, "
                                        + "f2, "
                                        + "f3, "
                                        + "f4, "
                                        + "f5, "
                                        + "f6, "
                                        + "f7, "
                                        + "f8, "
                                        + "f9, "
                                        + "CreateMultiset(f10), "
                                        + "f11, "
                                        + "f12, "
                                        + "f13, "
                                        + "JSON_ARRAY('V'), "
                                        + "JSON_OBJECT('K' VALUE JSON_ARRAY('V'))"
                                        + ")",
                                "["
                                        + "\"V\","
                                        + "true,"
                                        + "1,"
                                        + "1.23,"
                                        + "1.23,"
                                        + "\"1990-06-02T13:37:42.001\","
                                        + "\"1990-06-02T13:37:42.001Z\","
                                        + "[\"A1\",\"A2\",\"A3\"],"
                                        + "{\"f0\":\"R1\",\"f1\":\"1990-06-02T13:37:42.001Z\"},"
                                        + "{\"M1\":\"V1\",\"M2\":\"V2\"},"
                                        + "{\"M1\":1,\"M2\":2},"
                                        + "\"VGVzdA==\","
                                        + "\"VGVzdA==\","
                                        + "{\"f0\":[{\"f0\":1,\"f1\":2}]},"
                                        + "[\"V\"],"
                                        + "{\"K\":[\"V\"]}"
                                        + "]",
                                STRING().notNull(),
                                VARCHAR(2000).notNull()));
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * {@link BuiltInFunctionTestBase} uses a {@code VALUES} clause, but there currently is no way
     * to create a {@code MULTISET} for it yet. We work around this for now with a custom function.
     */
    public static class CreateMultiset extends ScalarFunction {
        public @DataTypeHint("MULTISET<STRING>") Map<String, Integer> eval(
                Map<String, Integer> map) {
            return map;
        }
    }

    private static String getJsonFromResource(String fileName) throws Exception {
        final InputStream jsonResource = JsonFunctionsITCase.class.getResourceAsStream(fileName);
        if (jsonResource == null) {
            throw new IllegalStateException(
                    String.format("%s: Missing test data.", JsonFunctionsITCase.class.getName()));
        }

        return IOUtils.toString(jsonResource, Charset.defaultCharset());
    }
}
