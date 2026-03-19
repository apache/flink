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

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;

public class UrlFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {

        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.URL_DECODE)
                        .onFieldsWithData(
                                "https%3A%2F%2Fflink.apache.org%2F",
                                "https://flink.apache.org/",
                                null,
                                "inva+lid%3A%2F%2Fuser%3Apass%40host%2Ffile%3Bparam%3Fquery%3Bp2",
                                "",
                                "illegal escape pattern test%")
                        .andDataTypes(STRING(), STRING(), STRING(), STRING(), STRING(), STRING())
                        .testResult(
                                $("f0").urlDecode(),
                                "url_decode(f0)",
                                "https://flink.apache.org/",
                                STRING())
                        .testResult(
                                $("f1").urlDecode(),
                                "url_decode(f1)",
                                "https://flink.apache.org/",
                                STRING())
                        .testResult(
                                $("f2").urlDecode(), "url_decode(f2)", null, STRING().nullable())
                        .testResult(
                                $("f3").urlDecode(),
                                "url_decode(f3)",
                                "inva lid://user:pass@host/file;param?query;p2",
                                STRING())
                        .testResult($("f4").urlDecode(), "url_decode(f4)", "", STRING())
                        .testResult($("f5").urlDecode(), "url_decode(f5)", null, STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE)
                        .onFieldsWithData(
                                "https%253A%252F%252Fflink.apache.org%252F",
                                "https://flink.apache.org/",
                                null,
                                "inva+lid%253A%252F%252Fuser%253Apass%2540host%252Ffile%253Bparam%253Fquery%253Bp2",
                                "",
                                "illegal escape pattern test%")
                        .andDataTypes(STRING(), STRING(), STRING(), STRING(), STRING(), STRING())
                        .testResult(
                                $("f0").urlDecodeRecursive(),
                                "url_decode_recursive(f0)",
                                "https://flink.apache.org/",
                                STRING())
                        .testResult(
                                $("f1").urlDecodeRecursive(),
                                "url_decode_recursive(f1)",
                                "https://flink.apache.org/",
                                STRING())
                        .testResult(
                                $("f2").urlDecodeRecursive(),
                                "url_decode_recursive(f2)",
                                null,
                                STRING().nullable())
                        .testResult(
                                $("f3").urlDecodeRecursive(),
                                "url_decode_recursive(f3)",
                                "inva lid://user:pass@host/file;param?query;p2",
                                STRING())
                        .testResult(
                                $("f4").urlDecodeRecursive(),
                                "url_decode_recursive(f4)",
                                "",
                                STRING())
                        .testResult(
                                $("f5").urlDecodeRecursive(),
                                "url_decode_recursive(f5)",
                                null,
                                STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE)
                        .onFieldsWithData(
                                "https%253A%252F%252Fflink.apache.org%252F",
                                "https%253A%252F%252Fflink.apache.org%252F",
                                "https%25253A%25252F%25252Fflink.apache.org%25252F",
                                null,
                                "https%253A%252F%252Fflink.apache.org%252F",
                                "test%253A%25")
                        .andDataTypes(STRING(), STRING(), STRING(), STRING(), STRING(), STRING())
                        // maxDepth=2: fully decodes double-encoded string
                        .testResult(
                                $("f0").urlDecodeRecursive(2),
                                "url_decode_recursive(f0, 2)",
                                "https://flink.apache.org/",
                                STRING())
                        // maxDepth=1: only decodes once
                        .testResult(
                                $("f1").urlDecodeRecursive(1),
                                "url_decode_recursive(f1, 1)",
                                "https%3A%2F%2Fflink.apache.org%2F",
                                STRING())
                        // maxDepth=3: fully decodes triple-encoded string
                        .testResult(
                                $("f2").urlDecodeRecursive(3),
                                "url_decode_recursive(f2, 3)",
                                "https://flink.apache.org/",
                                STRING())
                        // null input with maxDepth specified should return null
                        .testResult(
                                $("f3").urlDecodeRecursive(5),
                                "url_decode_recursive(f3, 5)",
                                null,
                                STRING().nullable())
                        // iteration > 0 and fails afterward: returns last successful result
                        .testResult(
                                $("f5").urlDecodeRecursive(10),
                                "url_decode_recursive(f5, 10)",
                                "test%3A%",
                                STRING()),
                // Test with different integer literal types (TINYINT, SMALLINT, BIGINT)
                // Note: SQL does not support typed integer literals (e.g., CAST(2 AS TINYINT)
                // is not considered a literal), so we only test Table API here.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE)
                        .onFieldsWithData(
                                "https%253A%252F%252Fflink.apache.org%252F",
                                "https%253A%252F%252Fflink.apache.org%252F",
                                "https%253A%252F%252Fflink.apache.org%252F")
                        .andDataTypes(STRING(), STRING(), STRING())
                        // Test with TINYINT literal (Table API only)
                        .testTableApiResult(
                                $("f0").urlDecodeRecursive(
                                                org.apache.flink.table.api.Expressions.lit(
                                                        (byte) 2)),
                                "https://flink.apache.org/",
                                STRING())
                        // Test with SMALLINT literal (Table API only)
                        .testTableApiResult(
                                $("f1").urlDecodeRecursive(
                                                org.apache.flink.table.api.Expressions.lit(
                                                        (short) 2)),
                                "https://flink.apache.org/",
                                STRING())
                        // Test with BIGINT literal (Table API only)
                        .testTableApiResult(
                                $("f2").urlDecodeRecursive(
                                                org.apache.flink.table.api.Expressions.lit(2L)),
                                "https://flink.apache.org/",
                                STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.URL_ENCODE)
                        .onFieldsWithData(
                                "https://flink.apache.org/",
                                "https%3A%2F%2Fflink.apache.org%2F",
                                null,
                                "inva lid://user:pass@host/file;param?query;p2",
                                "")
                        .andDataTypes(STRING(), STRING(), STRING(), STRING(), STRING())
                        .testResult(
                                $("f0").urlEncode(),
                                "url_encode(f0)",
                                "https%3A%2F%2Fflink.apache.org%2F",
                                STRING())
                        .testResult(
                                $("f1").urlEncode(),
                                "url_encode(f1)",
                                "https%253A%252F%252Fflink.apache.org%252F",
                                STRING())
                        .testResult(
                                $("f2").urlEncode(), "url_encode(f2)", null, STRING().nullable())
                        .testResult(
                                $("f3").urlEncode(),
                                "url_encode(f3)",
                                "inva+lid%3A%2F%2Fuser%3Apass%40host%2Ffile%3Bparam%3Fquery%3Bp2",
                                STRING())
                        .testResult($("f4").urlEncode(), "url_encode(f4)", "", STRING()));
    }
}
