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
                                "http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd",
                                null,
                                "inva+lid%3A%2F%2Fuser%3Apass%40host%2Ffile%3Bparam%3Fquery%3Bp2",
                                "")
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
                                $("f2").urlDecode(),
                                "url_decode(f2)",
                                "http://test?a=b&c=d",
                                STRING())
                        .testResult(
                                $("f3").urlDecode(), "url_decode(f3)", null, STRING().nullable())
                        .testResult(
                                $("f4").urlDecode(),
                                "url_decode(f4)",
                                "inva lid://user:pass@host/file;param?query;p2",
                                STRING())
                        .testResult($("f5").urlDecode(), "url_decode(f5)", "", STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.URL_DECODE)
                        .onFieldsWithData(
                                "https://flink.apache.org/",
                                "https%3A%2F%2Fflink.apache.org%2F",
                                "http://test?a=b&c=d",
                                null,
                                "inva lid://user:pass@host/file;param?query;p2",
                                "")
                        .andDataTypes(STRING(), STRING(), STRING(), STRING(), STRING(), STRING())
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
                                $("f2").urlEncode(),
                                "url_encode(f2)",
                                "http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd",
                                STRING())
                        .testResult(
                                $("f3").urlEncode(), "url_encode(f3)", null, STRING().nullable())
                        .testResult(
                                $("f4").urlEncode(),
                                "url_encode(f4)",
                                "inva+lid%3A%2F%2Fuser%3Apass%40host%2Ffile%3Bparam%3Fquery%3Bp2",
                                STRING())
                        .testResult($("f5").urlEncode(), "url_encode(f5)", "", STRING()));
    }
}
