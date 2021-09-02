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
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonType;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.apache.commons.io.IOUtils;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
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
        testCases.add(jsonExists());
        testCases.add(jsonValue());
        testCases.addAll(isJson());
        testCases.addAll(jsonQuery());

        return testCases;
    }

    private static TestSpec jsonExists() throws Exception {
        final String jsonValue = getJsonFromResource("/json/json-exists.json");
        return TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_EXISTS)
                .onFieldsWithData(jsonValue)
                .andDataTypes(DataTypes.STRING())

                // NULL
                .testResult(
                        nullOf(DataTypes.STRING()).jsonExists("lax $"),
                        "JSON_EXISTS(CAST(NULL AS STRING), 'lax $')",
                        null,
                        DataTypes.BOOLEAN())

                // Path variants
                .testResult(
                        $("f0").jsonExists("lax $"),
                        "JSON_EXISTS(f0, 'lax $')",
                        true,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("lax $.type"),
                        "JSON_EXISTS(f0, 'lax $.type')",
                        true,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("lax $.author.address.city"),
                        "JSON_EXISTS(f0, 'lax $.author.address.city')",
                        true,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("lax $.metadata.tags[0]"),
                        "JSON_EXISTS(f0, 'lax $.metadata.tags[0]')",
                        true,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("lax $.metadata.tags[3]"),
                        "JSON_EXISTS(f0, 'lax $.metadata.tags[3]')",
                        false,
                        DataTypes.BOOLEAN())
                // This should pass, but is broken due to
                // https://issues.apache.org/jira/browse/CALCITE-4717.
                // .testResult(
                //        $("f0").jsonExists("lax $.metadata.references.url"),
                //        "JSON_EXISTS(f0, 'lax $.metadata.references.url')",
                //        true,
                //        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("lax $.metadata.references[0].url"),
                        "JSON_EXISTS(f0, 'lax $.metadata.references[0].url')",
                        true,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("lax $.metadata.references[0].invalid"),
                        "JSON_EXISTS(f0, 'lax $.metadata.references[0].invalid')",
                        false,
                        DataTypes.BOOLEAN())

                // ON ERROR
                .testResult(
                        $("f0").jsonExists("strict $.invalid", JsonExistsOnError.TRUE),
                        "JSON_EXISTS(f0, 'strict $.invalid' TRUE ON ERROR)",
                        true,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("strict $.invalid", JsonExistsOnError.FALSE),
                        "JSON_EXISTS(f0, 'strict $.invalid' FALSE ON ERROR)",
                        false,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonExists("strict $.invalid", JsonExistsOnError.UNKNOWN),
                        "JSON_EXISTS(f0, 'strict $.invalid' UNKNOWN ON ERROR)",
                        null,
                        DataTypes.BOOLEAN())
                .testSqlRuntimeError(
                        "JSON_EXISTS(f0, 'strict $.invalid' ERROR ON ERROR)",
                        "No results for path: $['invalid']")
                .testTableApiRuntimeError(
                        $("f0").jsonExists("strict $.invalid", JsonExistsOnError.ERROR),
                        "No results for path: $['invalid']");
    }

    private static TestSpec jsonValue() throws Exception {
        final String jsonValue = getJsonFromResource("/json/json-value.json");
        return TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_VALUE)
                .onFieldsWithData(jsonValue)
                .andDataTypes(DataTypes.STRING())

                // NULL and invalid types

                .testResult(
                        lit(null, DataTypes.STRING()).jsonValue("lax $"),
                        "JSON_VALUE(CAST(NULL AS STRING), 'lax $')",
                        null,
                        DataTypes.STRING(),
                        DataTypes.VARCHAR(2000))

                // RETURNING + Supported Data Types

                .testResult(
                        $("f0").jsonValue("$.type"),
                        "JSON_VALUE(f0, '$.type')",
                        "account",
                        DataTypes.STRING(),
                        DataTypes.VARCHAR(2000))
                .testResult(
                        $("f0").jsonValue("$.activated", DataTypes.BOOLEAN()),
                        "JSON_VALUE(f0, '$.activated' RETURNING BOOLEAN)",
                        true,
                        DataTypes.BOOLEAN())
                .testResult(
                        $("f0").jsonValue("$.age", DataTypes.INT()),
                        "JSON_VALUE(f0, '$.age' RETURNING INT)",
                        42,
                        DataTypes.INT())
                .testResult(
                        $("f0").jsonValue("$.balance", DataTypes.DOUBLE()),
                        "JSON_VALUE(f0, '$.balance' RETURNING DOUBLE)",
                        13.37,
                        DataTypes.DOUBLE())

                // ON EMPTY / ON ERROR

                .testResult(
                        $("f0").jsonValue(
                                        "lax $.invalid",
                                        DataTypes.STRING(),
                                        JsonValueOnEmptyOrError.NULL,
                                        null,
                                        JsonValueOnEmptyOrError.ERROR,
                                        null),
                        "JSON_VALUE(f0, 'lax $.invalid' NULL ON EMPTY ERROR ON ERROR)",
                        null,
                        DataTypes.STRING(),
                        DataTypes.VARCHAR(2000))
                .testResult(
                        $("f0").jsonValue(
                                        "lax $.invalid",
                                        DataTypes.INT(),
                                        JsonValueOnEmptyOrError.DEFAULT,
                                        42,
                                        JsonValueOnEmptyOrError.ERROR,
                                        null),
                        "JSON_VALUE(f0, 'lax $.invalid' RETURNING INTEGER DEFAULT 42 ON EMPTY ERROR ON ERROR)",
                        42,
                        DataTypes.INT())
                .testResult(
                        $("f0").jsonValue(
                                        "strict $.invalid",
                                        DataTypes.STRING(),
                                        JsonValueOnEmptyOrError.ERROR,
                                        null,
                                        JsonValueOnEmptyOrError.NULL,
                                        null),
                        "JSON_VALUE(f0, 'strict $.invalid' ERROR ON EMPTY NULL ON ERROR)",
                        null,
                        DataTypes.STRING(),
                        DataTypes.VARCHAR(2000))
                .testResult(
                        $("f0").jsonValue(
                                        "strict $.invalid",
                                        DataTypes.INT(),
                                        JsonValueOnEmptyOrError.NULL,
                                        null,
                                        JsonValueOnEmptyOrError.DEFAULT,
                                        42),
                        "JSON_VALUE(f0, 'strict $.invalid' RETURNING INTEGER NULL ON EMPTY DEFAULT 42 ON ERROR)",
                        42,
                        DataTypes.INT());
    }

    private static List<TestSpec> isJson() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData(1)
                        .andDataTypes(DataTypes.INT())
                        .testSqlValidationError(
                                "f0 IS JSON",
                                "Cannot apply 'IS JSON VALUE' to arguments of type '<INTEGER> IS JSON VALUE'. "
                                        + "Supported form(s): '<CHARACTER> IS JSON VALUE'")
                        .testTableApiValidationError(
                                $("f0").isJson(),
                                String.format("Invalid function call:%nIS_JSON(INT)")),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData((String) null)
                        .andDataTypes(DataTypes.STRING())
                        .testResult(
                                $("f0").isJson(),
                                "f0 IS JSON",
                                false,
                                DataTypes.BOOLEAN().notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData("a")
                        .andDataTypes(DataTypes.STRING())
                        .testResult(
                                $("f0").isJson(),
                                "f0 IS JSON",
                                false,
                                DataTypes.BOOLEAN().notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData("\"a\"")
                        .andDataTypes(DataTypes.STRING())
                        .testResult(
                                $("f0").isJson(), "f0 IS JSON", true, DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.VALUE),
                                "f0 IS JSON VALUE",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.SCALAR),
                                "f0 IS JSON SCALAR",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.ARRAY),
                                "f0 IS JSON ARRAY",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.OBJECT),
                                "f0 IS JSON OBJECT",
                                false,
                                DataTypes.BOOLEAN().notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IS_JSON)
                        .onFieldsWithData("{}")
                        .andDataTypes(DataTypes.STRING())
                        .testResult(
                                $("f0").isJson(), "f0 IS JSON", true, DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.VALUE),
                                "f0 IS JSON VALUE",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.SCALAR),
                                "f0 IS JSON SCALAR",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.ARRAY),
                                "f0 IS JSON ARRAY",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f0").isJson(JsonType.OBJECT),
                                "f0 IS JSON OBJECT",
                                true,
                                DataTypes.BOOLEAN().notNull()));
    }

    private static List<TestSpec> jsonQuery() throws Exception {
        final String jsonValue = getJsonFromResource("/json/json-query.json");
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_QUERY)
                        .onFieldsWithData((String) null)
                        .andDataTypes(DataTypes.STRING())
                        .testResult(
                                $("f0").jsonQuery("$"),
                                "JSON_QUERY(f0, '$')",
                                null,
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000)),
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_QUERY)
                        .onFieldsWithData(jsonValue)
                        .andDataTypes(DataTypes.STRING())

                        // Wrapping Behavior

                        .testResult(
                                $("f0").jsonQuery("$.a1", WITHOUT_ARRAY),
                                "JSON_QUERY(f0, '$.a1' WITHOUT WRAPPER)",
                                "[]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.a1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.a1' WITH CONDITIONAL WRAPPER)",
                                "[]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.o1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.o1' WITH CONDITIONAL WRAPPER)",
                                "[{}]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.a1", UNCONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.a1' WITH UNCONDITIONAL WRAPPER)",
                                "[[]]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.n1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.n1' WITH CONDITIONAL WRAPPER)",
                                "[1]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("$.s1", CONDITIONAL_ARRAY),
                                "JSON_QUERY(f0, '$.s1' WITH CONDITIONAL WRAPPER)",
                                "[\"Test\"]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))

                        // Empty Behavior

                        .testResult(
                                $("f0").jsonQuery("lax $.err1", WITHOUT_ARRAY, NULL, NULL),
                                "JSON_QUERY(f0, 'lax $.err1' NULL ON EMPTY)",
                                null,
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("lax $.err2", WITHOUT_ARRAY, EMPTY_ARRAY, NULL),
                                "JSON_QUERY(f0, 'lax $.err2' EMPTY ARRAY ON EMPTY)",
                                "[]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery("lax $.err3", WITHOUT_ARRAY, EMPTY_OBJECT, NULL),
                                "JSON_QUERY(f0, 'lax $.err3' EMPTY OBJECT ON EMPTY)",
                                "{}",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
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
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery(
                                                "strict $.err7", WITHOUT_ARRAY, NULL, EMPTY_ARRAY),
                                "JSON_QUERY(f0, 'strict $.err7' EMPTY ARRAY ON ERROR)",
                                "[]",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testResult(
                                $("f0").jsonQuery(
                                                "strict $.err8", WITHOUT_ARRAY, NULL, EMPTY_OBJECT),
                                "JSON_QUERY(f0, 'strict $.err8' EMPTY OBJECT ON ERROR)",
                                "{}",
                                DataTypes.STRING(),
                                DataTypes.VARCHAR(2000))
                        .testSqlRuntimeError(
                                "JSON_QUERY(f0, 'strict $.err9' ERROR ON ERROR)",
                                "No results for path")
                        .testTableApiRuntimeError(
                                $("f0").jsonQuery("strict $.err10", WITHOUT_ARRAY, NULL, ERROR),
                                "No results for path"));
    }

    // ---------------------------------------------------------------------------------------------

    private static String getJsonFromResource(String fileName) throws Exception {
        final InputStream jsonResource = JsonFunctionsITCase.class.getResourceAsStream(fileName);
        if (jsonResource == null) {
            throw new IllegalStateException(
                    String.format("%s: Missing test data.", JsonFunctionsITCase.class.getName()));
        }

        return IOUtils.toString(jsonResource, Charset.defaultCharset());
    }
}
