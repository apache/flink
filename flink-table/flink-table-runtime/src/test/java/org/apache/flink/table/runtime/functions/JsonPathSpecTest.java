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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonQueryReturnType;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonValueContext;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.flink.table.runtime.functions.JsonPathReaderTest.result;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a {@link JsonPathSpec} reads like its path given as a string, also where it takes a
 * value at keys and indexes directly.
 */
class JsonPathSpecTest {

    private static final JsonValueContext[] INPUTS = {
        SqlJsonUtils.jsonParse(
                "{\"a\":{\"b\":[1,{\"c\":\"x\"}]},\"s\":\"t\",\"n\":null,"
                        + "\"arr\":[[1,2],{\"0\":\"z\"}],\"num\":1.50}"),
        SqlJsonUtils.jsonParse("[1,{\"c\":2}]"),
        SqlJsonUtils.jsonParse("null"),
        SqlJsonUtils.jsonParse("\"scalar\""),
        // a document that failed to parse
        SqlJsonUtils.jsonParse("{")
    };

    @ParameterizedTest(name = "{0}")
    @ValueSource(
            strings = {
                "$.a.b[1].c",
                "$.s",
                "$",
                "$.a",
                "$.missing",
                "$.num",
                "$[1].c",
                // a JSON null, and a step past it
                "$.n",
                "$.n.x",
                // an index on an array of arrays, on an object, on a string and out of range
                "$.arr[0][1]",
                "$.arr[1][0]",
                "$.s[0]",
                "$.a.b[5]",
                // a key on an array
                "$.a.b.c",
                "lax $.s",
                // case and spaces around the mode
                "LAX   $.missing",
                "  Strict $.a.b",
                // a mode without a path, read as a path
                "lax",
                // paths that don't compile
                "$.[",
                "lax $.["
            })
    void testReadsLikeStringPath(String pathSpec) {
        // one spec for all inputs, so later reads use its compiled path
        final JsonPathSpec path = new JsonPathSpec(pathSpec);
        for (JsonValueContext input : INPUTS) {
            assertThat(
                            result(
                                    () ->
                                            SqlJsonUtils.jsonValue(
                                                    input,
                                                    path,
                                                    JsonValueOnEmptyOrError.NULL,
                                                    null,
                                                    JsonValueOnEmptyOrError.ERROR,
                                                    null)))
                    .isEqualTo(
                            result(
                                    () ->
                                            SqlJsonUtils.jsonValue(
                                                    input,
                                                    pathSpec,
                                                    JsonValueOnEmptyOrError.NULL,
                                                    null,
                                                    JsonValueOnEmptyOrError.ERROR,
                                                    null)));
            assertThat(
                            result(
                                    () ->
                                            SqlJsonUtils.jsonQueryParsed(
                                                    input,
                                                    path,
                                                    JsonQueryReturnType.STRING,
                                                    JsonQueryWrapper.WITHOUT_ARRAY,
                                                    JsonQueryOnEmptyOrError.NULL,
                                                    JsonQueryOnEmptyOrError.ERROR)))
                    .isEqualTo(
                            result(
                                    () ->
                                            SqlJsonUtils.jsonQueryParsed(
                                                    input,
                                                    pathSpec,
                                                    JsonQueryReturnType.STRING,
                                                    JsonQueryWrapper.WITHOUT_ARRAY,
                                                    JsonQueryOnEmptyOrError.NULL,
                                                    JsonQueryOnEmptyOrError.ERROR)));
        }
    }
}
