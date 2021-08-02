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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.apache.commons.io.IOUtils;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.nullOf;

/** Tests for {@link BuiltInFunctionDefinitions#JSON_EXISTS}. */
public class JsonExistsFunctionITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() throws Exception {
        final InputStream jsonResource =
                JsonExistsFunctionITCase.class.getResourceAsStream("/json/json-exists.json");
        if (jsonResource == null) {
            throw new IllegalStateException(
                    String.format(
                            "%s: Missing test data.", JsonExistsFunctionITCase.class.getName()));
        }

        final String jsonValue = IOUtils.toString(jsonResource, Charset.defaultCharset());
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.JSON_EXISTS)
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
                                "No results for path: $['invalid']"));
    }
}
