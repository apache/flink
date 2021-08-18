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
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.apache.commons.io.IOUtils;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;

/** Tests for built-in JSON functions. */
public class JsonFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() throws Exception {
        final List<TestSpec> testCases = new ArrayList<>();
        testCases.add(jsonExists());
        testCases.add(jsonValue());

        return testCases;
    }

    private static TestSpec jsonExists() throws Exception {
        final InputStream jsonResource =
                JsonFunctionsITCase.class.getResourceAsStream("/json/json-exists.json");
        if (jsonResource == null) {
            throw new IllegalStateException(
                    String.format("%s: Missing test data.", JsonFunctionsITCase.class.getName()));
        }

        final String jsonValue = IOUtils.toString(jsonResource, Charset.defaultCharset());
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
        final InputStream jsonResource =
                JsonFunctionsITCase.class.getResourceAsStream("/json/json-value.json");
        if (jsonResource == null) {
            throw new IllegalStateException(
                    String.format("%s: Missing test data.", JsonFunctionsITCase.class.getName()));
        }

        final String jsonValue = IOUtils.toString(jsonResource, Charset.defaultCharset());
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
}
