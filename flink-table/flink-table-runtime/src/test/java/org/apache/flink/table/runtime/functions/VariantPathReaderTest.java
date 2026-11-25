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

import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Tests that {@link VariantPathReader} reads what {@code PARSE_JSON} and access would. */
class VariantPathReaderTest {

    // objects, arrays, a path inside another, the same path twice, and steps of both kinds
    private static final Object[][] PATHS = {
        {"book", "title"},
        {"book"},
        {"book", "tags", 1},
        {"book", "title"},
        {"a", 0},
        {"a", 0, "b"},
        {0},
        {0, "x"},
        {"missing", "x"},
        {"?"},
        {"k" + (char) 0xD800}
    };

    // one reader for all cases: it keeps no state, as the instances of the generated code share it
    private static final VariantPathReader READER = new VariantPathReader(PATHS);

    private static final String BOOK =
            "{\"store\":\"Main\",\"book\":{\"author\":\"Melville\",\"title\":\"Moby Dick\","
                    + "\"tags\":[\"novel\",\"sea\"],\"year\":1851},\"open\":true}";

    private static Stream<Arguments> documents() {
        final String[] jsons = {
            BOOK,
            // values of other types at the steps
            "{\"book\":42}",
            "{\"book\":null}",
            "{\"book\":\"text\"}",
            "{\"book\":[1,2]}",
            "{\"book\":{\"title\":{\"nested\":[1,{\"x\":2}]}}}",
            "{\"a\":[{\"b\":1},{\"b\":2}]}",
            "{\"a\":[]}",
            "{\"a\":{\"0\":1}}",
            "[{\"x\":1},2]",
            "[]",
            "\"scalar\"",
            "null",
            "1",
            // duplicate keys, where the last one wins if they are allowed
            "{\"book\":{\"title\":\"first\"},\"book\":{\"title\":\"second\"}}",
            "{\"book\":{\"title\":\"first\"},\"book\":7}",
            "{\"store\":{\"x\":1,\"x\":2},\"book\":{\"title\":\"t\"}}",
            // an escaped key of a path
            "{\"\\u0062ook\":{\"title\":\"t\"}}",
            // many keys, with and without a duplicate after them
            manyKeys(20, null),
            manyKeys(20, "k17"),
            manyKeys(20, "k3"),
            // numbers that fail or change type in the skipped part
            "{\"store\":1e400,\"book\":{\"title\":\"t\"}}",
            "{\"store\":123456789012345678901234567890,\"book\":{\"title\":\"t\"}}",
            "{\"store\":-0.0,\"book\":{\"title\":1.5e3}}",
            // skipped arrays: scalars of each kind, nesting, duplicates, siblings, bad numbers
            "{\"store\":[1,\"s\",true,false,null,1.5,[2,{\"x\":1}]],"
                    + "\"book\":{\"title\":\"t\"}}",
            "{\"store\":[{\"x\":1,\"x\":2}],\"book\":{\"title\":\"t\"}}",
            "{\"store\":[{\"x\":1},{\"x\":1}],\"book\":{\"title\":\"t\"}}",
            "{\"store\":[1e400],\"book\":{\"title\":\"t\"}}",
            "{\"store\":[1",
            // duplicate keys in skipped objects below depth 1, and key sets of nested objects
            "{\"store\":{\"inner\":{\"x\":1,\"x\":2}},\"book\":{\"title\":\"t\"}}",
            "{\"store\":{\"x\":{\"x\":1},\"y\":1,\"x\":2},\"book\":{\"title\":\"t\"}}",
            "{\"store\":{\"a\":{\"b\":1},\"b\":2},\"book\":{\"title\":\"t\"}}",
            // a key set over 64 keys, replaced for the next object at the same depth
            "{\"store\":{" + keys(70) + "},\"other\":{\"k0\":1},\"book\":{\"title\":\"t\"}}",
            "{\"store\":{" + keys(70) + "},\"other\":{\"k0\":1,\"k0\":2}}",
            // malformed documents
            "",
            "{",
            "{\"book\":}",
            "{\"book\":{\"title\":\"t\"},}",
            "[1,]",
            // content after the document, which the full parse ignores
            "{\"book\":{\"title\":\"t\"}} trailing",
            // a large skipped value
            largeDocument()
        };
        return Stream.of(jsons)
                .flatMap(
                        json ->
                                Stream.of(
                                        arguments(json.getBytes(StandardCharsets.UTF_8), false),
                                        arguments(json.getBytes(StandardCharsets.UTF_8), true)));
    }

    @ParameterizedTest(name = "[{index}] allowDuplicateKeys={1}")
    @MethodSource("documents")
    void testReadsWhatTheFullParseReads(byte[] json, boolean allowDuplicateKeys) {
        assertReadsLikeFullParse(json, allowDuplicateKeys);
    }

    @ParameterizedTest(name = "[{index}] allowDuplicateKeys={1}")
    @MethodSource("invalidUtf8")
    void testInvalidUtf8InSkippedString(byte[] json, boolean allowDuplicateKeys) {
        assertReadsLikeFullParse(json, allowDuplicateKeys);
    }

    private static Stream<Arguments> invalidUtf8() {
        return Stream.of(
                        invalidUtf8In("{\"store\":\"", "\",\"book\":{\"title\":\"t\"}}"),
                        // in a skipped array
                        invalidUtf8In("{\"store\":[\"", "\"],\"book\":{\"title\":\"t\"}}"))
                .flatMap(json -> Stream.of(arguments(json, false), arguments(json, true)));
    }

    // a truncated character between prefix and suffix
    private static byte[] invalidUtf8In(String prefix, String suffix) {
        return withBytes(prefix, new byte[] {(byte) 0xC3}, suffix);
    }

    private static byte[] withBytes(String prefix, byte[] middle, String suffix) {
        final byte[] start = prefix.getBytes(StandardCharsets.UTF_8);
        final byte[] end = suffix.getBytes(StandardCharsets.UTF_8);
        final byte[] json = new byte[start.length + middle.length + end.length];
        System.arraycopy(start, 0, json, 0, start.length);
        System.arraycopy(middle, 0, json, start.length, middle.length);
        System.arraycopy(end, 0, json, start.length + middle.length, end.length);
        return json;
    }

    private static void assertReadsLikeFullParse(byte[] json, boolean allowDuplicateKeys) {
        Variant full = null;
        Exception fullError = null;
        try {
            full = BinaryVariantInternalBuilder.parseJson(json, allowDuplicateKeys);
        } catch (Exception e) {
            fullError = e;
        }
        Variant parsed = null;
        Exception parseError = null;
        try {
            parsed = READER.parse(BinaryStringData.fromBytes(json), allowDuplicateKeys);
        } catch (Exception e) {
            parseError = e;
        }
        if (fullError != null) {
            assertThat(parseError).isNotNull();
            assertThat(parseError.getClass()).isEqualTo(fullError.getClass());
            assertThat(parseError.getMessage()).isEqualTo(fullError.getMessage());
            return;
        }
        assertThat(parseError).isNull();
        for (Object[] path : PATHS) {
            assertThat(access(parsed, path))
                    .as(Arrays.toString(path))
                    .isEqualTo(access(full, path));
        }
    }

    // the path as the generated access applies it: the value as JSON, null, or the error
    private static String access(Variant variant, Object[] path) {
        Variant value = variant;
        for (Object step : path) {
            if (step instanceof Integer) {
                if (!value.isArray()) {
                    return "Integer index access on variant requires an array variant, but a "
                            + "non-array variant was provided.";
                }
                value = value.getElement((Integer) step);
            } else {
                if (!value.isObject()) {
                    return "String key access on variant requires an object variant, but a "
                            + "non-object variant was provided.";
                }
                value = value.getField((String) step);
            }
            if (value == null) {
                return null;
            }
        }
        // as JSON, since the field name dictionaries differ
        return value.toJson();
    }

    // an object of count keys, then the book, and the key again if not null
    private static String manyKeys(int count, String repeated) {
        final StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < count; i++) {
            sb.append("\"k").append(i).append("\":").append(i).append(',');
        }
        sb.append("\"book\":{\"title\":\"t\"}");
        if (repeated != null) {
            sb.append(",\"").append(repeated).append("\":0");
        }
        return sb.append('}').toString();
    }

    // the members "k0":0 to "k<count-1>":<count-1>
    private static String keys(int count) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":").append(i);
        }
        return sb.toString();
    }

    private static String largeDocument() {
        final StringBuilder sb = new StringBuilder("{\"store\":\"");
        while (sb.length() < 300_000) {
            sb.append("xxxxxxxxxx");
        }
        return sb.append("\",\"book\":{\"title\":\"t\"}}").toString();
    }
}
