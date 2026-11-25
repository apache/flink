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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.StringUtf8Utils;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonValueContext;

import org.apache.flink.shaded.com.jayway.jsonpath.JsonPath;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a JSON document like {@link SqlJsonUtils#jsonParse}, but builds only what fixed paths
 * reach: the value at the end of a path in full, and the objects and arrays on the way with only
 * the members the paths need. An array keeps its length, with null for the elements no path needs,
 * so that JsonPath evaluates the paths on the result as on the whole document.
 *
 * <p>A document this reader fails on is parsed by {@link SqlJsonUtils#jsonParse}, which reports the
 * failure as before. Jackson's limit on the length of a string applies only to the strings this
 * reader builds.
 */
@Internal
public final class JsonPathReader implements Serializable {

    private static final long serialVersionUID = 1L;

    // a property or an index of a compiled JsonPath, as it prints them
    private static final Pattern STEP =
            Pattern.compile("\\['([^'\\\\]*)']|\\[(0|[1-9][0-9]{0,8})]");

    // Bytes are always UTF-8, as the decoding of the full parse reads them. Charset detection would
    // skip a BOM or read NUL bytes as UTF-16 or UTF-32.
    private static final JsonFactory BYTES_FACTORY =
            SqlJsonUtils.JSON_FACTORY
                    .rebuild()
                    .disable(JsonFactory.Feature.CHARSET_DETECTION)
                    .build();

    // read-only, so the reader is stateless
    private final JsonPathNode root;

    /**
     * @param paths the steps of each path, see {@link #steps}
     */
    public JsonPathReader(Object[][] paths) {
        this.root = JsonPathNode.of(paths, UnaryOperator.identity());
    }

    /**
     * The steps of a JSONPath that JsonPath reads as properties and indices from {@code $} only: a
     * {@link String} for a property, an {@link Integer} for an index. Null for any other path.
     */
    @Nullable
    public static Object[] steps(String path) {
        // JsonPath prints a name without escaping it, so a name with a quote, which only an escape
        // or double quotes allow, can't be read back from the compiled path
        if (path.indexOf('\\') >= 0 || path.indexOf('"') >= 0) {
            return null;
        }
        final String compiled;
        try {
            // as JsonPath reads it, like "$['address']['city']" for "$.address.city"
            compiled = JsonPath.compile(path).getPath();
        } catch (RuntimeException e) {
            return null;
        }
        if (!compiled.startsWith("$")) {
            return null;
        }
        final List<Object> steps = new ArrayList<>();
        final Matcher matcher = STEP.matcher(compiled);
        int position = 1;
        while (position < compiled.length()) {
            if (!matcher.find(position) || matcher.start() != position) {
                return null;
            }
            if (matcher.group(1) != null) {
                steps.add(matcher.group(1));
            } else {
                steps.add(Integer.parseInt(matcher.group(2)));
            }
            position = matcher.end();
        }
        return steps.toArray();
    }

    /** Like {@link #steps}, for a path after its optional {@code lax} or {@code strict}. */
    @Nullable
    public static Object[] stepsAfterMode(String pathSpec) {
        final Matcher matcher = SqlJsonUtils.JSON_PATH_BASE.matcher(pathSpec);
        return steps(matcher.matches() ? matcher.group(2) : pathSpec);
    }

    /**
     * Parses {@code json} for the paths, from its UTF-8 bytes unless it has a Java string already.
     */
    public JsonValueContext parse(StringData json) {
        final BinaryStringData string = (BinaryStringData) json;
        if (string.getJavaObject() != null) {
            // a lone surrogate of the string is encoded as '?', so the string is read as it is
            return parse(string.toString());
        }
        final MemorySegment[] segments = string.getSegments();
        final int offset;
        final byte[] bytes;
        final int size = string.getSizeInBytes();
        // null off the heap
        final byte[] heap = segments[0].getHeapMemory();
        if (heap != null && string.getOffset() + size <= segments[0].size()) {
            bytes = heap;
            offset = string.getOffset();
        } else {
            bytes = string.toBytes();
            offset = 0;
        }
        // bytes the decoder of the full parse replaces by U+FFFD may be read differently here
        if (StringUtf8Utils.firstInvalidUtf8ByteIndex(bytes, offset, size) < 0) {
            try (JsonParser parser = BYTES_FACTORY.createParser(bytes, offset, size)) {
                final JsonValueContext parsed = read(parser);
                if (parsed != null) {
                    return parsed;
                }
            } catch (Exception e) {
                // reported by the full parse
            }
        }
        return SqlJsonUtils.jsonParse(string.toString());
    }

    /** Parses {@code json} for the paths. */
    public JsonValueContext parse(String json) {
        try (JsonParser parser = SqlJsonUtils.JSON_FACTORY.createParser(json)) {
            final JsonValueContext parsed = read(parser);
            if (parsed != null) {
                return parsed;
            }
        } catch (Exception e) {
            // reported by the full parse
        }
        return SqlJsonUtils.jsonParse(json);
    }

    // null for a document without content
    @Nullable
    private JsonValueContext read(JsonParser parser) throws Exception {
        final JsonToken token = parser.nextToken();
        // like the full parse, what follows the root value isn't read
        return token == null ? null : JsonValueContext.withJavaObj(read(parser, token, root));
    }

    @Nullable
    private static Object read(JsonParser parser, JsonToken token, JsonPathNode node)
            throws Exception {
        if (token != JsonToken.START_OBJECT && token != JsonToken.START_ARRAY) {
            return scalar(parser, token);
        }
        // JsonPath reports an index on an object with the whole object, so it is built in full
        if (node.isEnd() || (token == JsonToken.START_OBJECT && node.hasElements())) {
            return SqlJsonUtils.VALUE_READER.readValue(parser);
        }
        if (token == JsonToken.START_OBJECT) {
            // the order of the fields isn't observed, and twice the children never resizes
            final Map<String, Object> object = new HashMap<>(2 * node.fieldCount());
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                final String name = parser.currentName();
                final JsonToken value = parser.nextToken();
                final JsonPathNode child = node.field(name);
                if (child == null) {
                    skip(parser, value);
                } else {
                    // the last of duplicate names wins, as in the full parse
                    object.put(name, read(parser, value, child));
                }
            }
            return object;
        }
        final List<Object> array = new ArrayList<>();
        JsonToken element;
        while ((element = parser.nextToken()) != JsonToken.END_ARRAY) {
            final JsonPathNode child = node.element(array.size());
            if (child == null) {
                skip(parser, element);
                array.add(null);
            } else {
                array.add(read(parser, element, child));
            }
        }
        return array;
    }

    // Skips the value like skipChildren, but converts the floats as the full parse does, which
    // fails on an exponent out of range.
    private static void skip(JsonParser parser, JsonToken token) throws IOException {
        if (token == JsonToken.VALUE_NUMBER_FLOAT) {
            parser.getDecimalValue();
            return;
        }
        if (!token.isStructStart()) {
            return;
        }
        int depth = 1;
        while (depth > 0) {
            final JsonToken next = parser.nextToken();
            if (next == null) {
                return;
            }
            if (next.isStructStart()) {
                depth++;
            } else if (next.isStructEnd()) {
                depth--;
            } else if (next == JsonToken.VALUE_NUMBER_FLOAT) {
                parser.getDecimalValue();
            }
        }
    }

    // as UntypedObjectDeserializer builds a scalar, with USE_BIG_DECIMAL_FOR_FLOATS
    @Nullable
    private static Object scalar(JsonParser parser, JsonToken token) throws IOException {
        switch (token) {
            case VALUE_STRING:
                return parser.getText();
            case VALUE_NUMBER_INT:
                // Integer, Long or BigInteger
                return parser.getNumberValue();
            case VALUE_NUMBER_FLOAT:
                return parser.getDecimalValue();
            case VALUE_TRUE:
                return Boolean.TRUE;
            case VALUE_FALSE:
                return Boolean.FALSE;
            case VALUE_NULL:
                return null;
            default:
                return SqlJsonUtils.VALUE_READER.readValue(parser);
        }
    }
}
