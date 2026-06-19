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

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Tests for {@link BuiltInFunctionDefinitions#IS_VALID_UTF8} and {@link
 * BuiltInFunctionDefinitions#MAKE_VALID_UTF8}.
 */
public class Utf8FunctionsITCase extends BuiltInFunctionTestBase {

    private static final byte[] HELLO = "Hello".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MULTIBYTE = "é€😀".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INVALID_START = {(byte) 0x80};
    private static final byte[] TRUNCATED = {(byte) 0xE2, (byte) 0x82};
    private static final byte[] OVERLONG = {(byte) 0xC0, (byte) 0xAF};
    private static final byte[] SURROGATE = {(byte) 0xED, (byte) 0xA0, (byte) 0x80};

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(isValidUtf8Cases(), makeValidUtf8Cases());
    }

    private TestSetSpec isValidUtf8Cases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.IS_VALID_UTF8)
                .onFieldsWithData(
                        null, HELLO, MULTIBYTE, INVALID_START, TRUNCATED, OVERLONG, SURROGATE)
                .andDataTypes(
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES())
                .testSqlResult("IS_VALID_UTF8(f0)", null, DataTypes.BOOLEAN().nullable())
                .testSqlResult("IS_VALID_UTF8(f1)", true, DataTypes.BOOLEAN().nullable())
                .testSqlResult("IS_VALID_UTF8(f2)", true, DataTypes.BOOLEAN().nullable())
                .testSqlResult("IS_VALID_UTF8(f3)", false, DataTypes.BOOLEAN().nullable())
                .testSqlResult("IS_VALID_UTF8(f4)", false, DataTypes.BOOLEAN().nullable())
                .testSqlResult("IS_VALID_UTF8(f5)", false, DataTypes.BOOLEAN().nullable())
                .testSqlResult("IS_VALID_UTF8(f6)", false, DataTypes.BOOLEAN().nullable())
                // Table API method routes to the same definition.
                .testTableApiResult($("f1").isValidUtf8(), true, DataTypes.BOOLEAN().nullable())
                .testTableApiResult($("f3").isValidUtf8(), false, DataTypes.BOOLEAN().nullable());
    }

    private TestSetSpec makeValidUtf8Cases() {
        // Lenient decode equivalent to new String(b, UTF_8). Java follows the W3C
        // maximal-subpart rule: one replacement char per maximal ill-formed subpart.
        final byte[] mixed = {'A', 'B', (byte) 0x80, 'C', 'D'};
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAKE_VALID_UTF8)
                .onFieldsWithData(
                        null,
                        HELLO,
                        MULTIBYTE,
                        INVALID_START,
                        TRUNCATED,
                        OVERLONG,
                        SURROGATE,
                        mixed)
                .andDataTypes(
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES(),
                        DataTypes.BYTES())
                .testSqlResult("MAKE_VALID_UTF8(f0)", null, DataTypes.STRING().nullable())
                .testSqlResult("MAKE_VALID_UTF8(f1)", "Hello", DataTypes.STRING().nullable())
                .testSqlResult("MAKE_VALID_UTF8(f2)", "é€😀", DataTypes.STRING().nullable())
                .testSqlResult("MAKE_VALID_UTF8(f3)", "�", DataTypes.STRING().nullable())
                .testSqlResult("MAKE_VALID_UTF8(f4)", "�", DataTypes.STRING().nullable())
                .testSqlResult("MAKE_VALID_UTF8(f5)", "��", DataTypes.STRING().nullable())
                // JDK's UTF-8 decoder consumes the entire 3-byte surrogate attempt and emits
                // a single U+FFFD; this differs from the W3C maximal-subpart count of 3.
                .testSqlResult("MAKE_VALID_UTF8(f6)", "�", DataTypes.STRING().nullable())
                .testSqlResult("MAKE_VALID_UTF8(f7)", "AB�CD", DataTypes.STRING().nullable())
                .testTableApiResult($("f1").makeValidUtf8(), "Hello", DataTypes.STRING().nullable())
                .testTableApiResult($("f3").makeValidUtf8(), "�", DataTypes.STRING().nullable());
    }
}
