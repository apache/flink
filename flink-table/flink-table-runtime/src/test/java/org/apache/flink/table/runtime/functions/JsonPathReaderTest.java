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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonQueryReturnType;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonValueContext;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Tests that {@link JsonPathReader} gives the JSON functions what the full parse gives them. */
class JsonPathReaderTest {

    // Groups of paths, each read by its own reader. An index on an object builds the object in
    // full, and so does the end of a path, so the groups keep them apart for the rest to be pruned.
    private static final String[][] PATH_GROUPS = {
        // keys and indexes from an object root, the same path twice
        {
            "$.book.title",
            "$['book']['title']",
            "$.book.tags[1]",
            "$.a[0].b",
            "$.missing.x",
            "$.open"
        },
        // an array root
        {"$[0].x", "$[1]"},
        // a path inside another
        {"$.book", "$.book.title"}
    };

    // one reader per group for all cases: it keeps no state, as the instances of the generated
    // code share it
    private static final JsonPathReader[] READERS =
            Arrays.stream(PATH_GROUPS)
                    .map(paths -> new JsonPathReader(steps(paths)))
                    .toArray(JsonPathReader[]::new);

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
            // a key on an array, and an index on an object
            "{\"book\":[1,2]}",
            "{\"book\":{\"tags\":{\"1\":\"x\",\"a\":2}}}",
            "{\"book\":{\"title\":{\"nested\":[1,{\"x\":2}]}}}",
            "{\"a\":[{\"b\":1},{\"b\":2}]}",
            "{\"a\":[]}",
            "[{\"x\":1},2]",
            "[]",
            "\"scalar\"",
            "null",
            "1",
            // duplicate keys, where the last one wins
            "{\"book\":{\"title\":\"first\"},\"book\":{\"title\":\"second\"}}",
            "{\"book\":{\"title\":\"first\"},\"book\":7}",
            // numbers at path ends and in skipped parts
            "{\"book\":{\"title\":1.50,\"year\":123456789012345678901234567890}}",
            "{\"store\":[1.5,{\"x\":-0.0}],\"book\":{\"title\":\"t\"}}",
            "{\"store\":1e2147483648,\"book\":{\"title\":\"t\"}}",
            // escapes in skipped and read parts
            "{\"store\":\"a\\\"b\\u0041\",\"book\":{\"title\":\"x\\ny\"}}",
            // malformed documents
            "",
            "{",
            "{\"book\":}",
            "{\"book\":{\"title\":\"t\"},}",
            "{\"store\":\"\\x\",\"book\":{\"title\":\"t\"}}",
            // content after the document, which the full parse ignores
            "{\"book\":{\"title\":\"t\"}} trailing"
        };
        return Stream.of(jsons).map(json -> arguments(json.getBytes(StandardCharsets.UTF_8)));
    }

    // documents that only bytes can hold
    private static Stream<Arguments> byteDocuments() {
        return Stream.of(
                        // a truncated character in a skipped string
                        withBytes("{\"store\":\"", new byte[] {(byte) 0xC3}, "\",\"book\":1}"),
                        // a byte order mark
                        withBytes("", new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, BOOK))
                .map(Arguments::arguments);
    }

    @ParameterizedTest(name = "[{index}]")
    @MethodSource({"documents", "byteDocuments"})
    void testBytes(byte[] json) {
        assertReadsLikeFullParse(BinaryStringData.fromBytes(json));
    }

    @ParameterizedTest(name = "[{index}]")
    @MethodSource("documents")
    void testString(byte[] json) {
        assertReadsLikeFullParse(
                BinaryStringData.fromString(new String(json, StandardCharsets.UTF_8)));
    }

    @Test
    void testSegments() {
        // bytes outside a single heap segment are copied first, as off the heap
        final byte[] json = BOOK.getBytes(StandardCharsets.UTF_8);
        final int half = json.length / 2;
        final MemorySegment[] segments = {
            MemorySegmentFactory.wrap(Arrays.copyOfRange(json, 0, half)),
            MemorySegmentFactory.wrap(Arrays.copyOfRange(json, half, json.length))
        };
        assertReadsLikeFullParse(BinaryStringData.fromAddress(segments, 0, json.length));
    }

    private static void assertReadsLikeFullParse(BinaryStringData json) {
        final JsonValueContext full = SqlJsonUtils.jsonParse(json.toString());
        for (int i = 0; i < PATH_GROUPS.length; i++) {
            assertReadsLikeFullParse(READERS[i].parse(json), full, PATH_GROUPS[i]);
        }
    }

    private static void assertReadsLikeFullParse(
            JsonValueContext parsed, JsonValueContext full, String[] paths) {
        // strict only, as lax returns null where strict fails; JSON_QUERY prints the whole value,
        // so JSON_LENGTH and JSON_TYPE, which read the same value, would add nothing
        for (String path : paths) {
            assertSame(
                    "JSON_VALUE " + path,
                    input ->
                            SqlJsonUtils.jsonValue(
                                    input,
                                    "strict " + path,
                                    JsonValueOnEmptyOrError.NULL,
                                    null,
                                    JsonValueOnEmptyOrError.ERROR,
                                    null),
                    full,
                    parsed);
            assertSame(
                    "JSON_QUERY " + path,
                    input ->
                            SqlJsonUtils.jsonQueryParsed(
                                    input,
                                    "strict " + path,
                                    JsonQueryReturnType.STRING,
                                    JsonQueryWrapper.WITHOUT_ARRAY,
                                    JsonQueryOnEmptyOrError.NULL,
                                    JsonQueryOnEmptyOrError.ERROR),
                    full,
                    parsed);
        }
    }

    private static void assertSame(
            String description,
            Function<JsonValueContext, Object> function,
            JsonValueContext full,
            JsonValueContext parsed) {
        assertThat(result(() -> function.apply(parsed)))
                .as(description)
                .isEqualTo(result(() -> function.apply(full)));
    }

    // the value with its class, or the error
    static String result(Callable<Object> call) {
        try {
            final Object value = call.call();
            return value == null ? "null" : value.getClass().getName() + ": " + value;
        } catch (Exception e) {
            return e.getClass().getName() + ": " + e.getMessage();
        }
    }

    private static Object[][] steps(String[] paths) {
        return Arrays.stream(paths).map(JsonPathReader::steps).toArray(Object[][]::new);
    }

    private static byte[] withBytes(String prefix, byte[] middle, String suffix) {
        return ArrayUtils.addAll(
                ArrayUtils.addAll(prefix.getBytes(StandardCharsets.UTF_8), middle),
                suffix.getBytes(StandardCharsets.UTF_8));
    }
}
