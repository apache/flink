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

package org.apache.flink.types.variant;

import org.apache.flink.types.variant.JsonTokenSource.Token;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies a custom {@link JsonTokenSource} produces byte-for-byte the same encoding as the {@code
 * parseJson(String)} path, and reaches error branches a real JSON parser never produces.
 */
class JsonTokenSourceTest {

    @Test
    void testCustomSourceMatchesStringPathByteForByte() throws IOException {
        // One document that spans every integer width, decimal, double, big integer, the scalars,
        // nesting and a non-ASCII key so the metadata dictionary is built from foreign tokens.
        final String jsonString =
                "{\"b\":1,\"s\":200,\"i\":40000,"
                        + "\"l\":3000000000,\"big\":9223372036854775808,"
                        + "\"dec\":3.14,\"dbl\":1e10,"
                        + "\"str\":\"hi\",\"t\":true,\"f\":false,\"n\":null,"
                        + "\"arr\":[1,[2.5,\"x\"]],\"obj\":{\"k\":\"v\"},"
                        + "\"schlüssel\":\"Grüße 世界 🚀\"}";
        final Object model =
                obj(
                        "b",
                        1,
                        "s",
                        200,
                        "i",
                        40000,
                        "l",
                        3000000000L,
                        "big",
                        new BigInteger("9223372036854775808"),
                        "dec",
                        new BigDecimal("3.14"),
                        "dbl",
                        1e10,
                        "str",
                        "hi",
                        "t",
                        true,
                        "f",
                        false,
                        "n",
                        null,
                        "arr",
                        List.of(1, List.of(new BigDecimal("2.5"), "x")),
                        "obj",
                        obj("k", "v"),
                        "schlüssel",
                        "Grüße 世界 🚀");

        final BinaryVariant fromString = BinaryVariantInternalBuilder.parseJson(jsonString, false);
        final BinaryVariant fromSource = parseSource(model);

        assertThat(fromSource.getValue()).isEqualTo(fromString.getValue());
        assertThat(fromSource.getMetadata()).isEqualTo(fromString.getMetadata());
    }

    @Test
    void testTruncatedObjectFromSourceIsRejected() {
        // START_OBJECT and a field name, but the stream ends before the field's value.
        final List<Tok> tokens = new ArrayList<>();
        tokens.add(new Tok(Token.START_OBJECT, null, null));
        tokens.add(new Tok(Token.FIELD_NAME, "a", null));

        assertThatThrownBy(
                        () ->
                                BinaryVariantInternalBuilder.parseJson(
                                        new ListJsonSource(tokens), false))
                .isInstanceOf(IOException.class);
    }

    private static BinaryVariant parseSource(Object model) throws IOException {
        final List<Tok> tokens = new ArrayList<>();
        emit(model, tokens);
        return BinaryVariantInternalBuilder.parseJson(new ListJsonSource(tokens), false);
    }

    // A tiny JSON model and its token walker, standing in for a format's own parse tree.

    private static Map<String, Object> obj(Object... keyValues) {
        final Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static void emit(Object node, List<Tok> out) {
        if (node instanceof Map) {
            out.add(new Tok(Token.START_OBJECT, null, null));
            for (final Map.Entry<String, Object> entry : ((Map<String, Object>) node).entrySet()) {
                out.add(new Tok(Token.FIELD_NAME, entry.getKey(), null));
                emit(entry.getValue(), out);
            }
            out.add(new Tok(Token.END_OBJECT, null, null));
        } else if (node instanceof List) {
            out.add(new Tok(Token.START_ARRAY, null, null));
            for (final Object element : (List<Object>) node) {
                emit(element, out);
            }
            out.add(new Tok(Token.END_ARRAY, null, null));
        } else if (node instanceof Number) {
            out.add(new Tok(Token.VALUE_NUMBER, null, node.toString()));
        } else if (node instanceof String) {
            out.add(new Tok(Token.VALUE_STRING, null, (String) node));
        } else if (node instanceof Boolean) {
            out.add(new Tok((Boolean) node ? Token.VALUE_TRUE : Token.VALUE_FALSE, null, null));
        } else if (node == null) {
            out.add(new Tok(Token.VALUE_NULL, null, null));
        } else {
            throw new IllegalArgumentException("Unsupported model node: " + node);
        }
    }

    private static final class Tok {
        private final Token token;
        private final String fieldName;
        private final String text;

        private Tok(Token token, String fieldName, String text) {
            this.token = token;
            this.fieldName = fieldName;
            this.text = text;
        }
    }

    /** A {@link JsonTokenSource} over an in-memory list of tokens. */
    private static final class ListJsonSource implements JsonTokenSource {
        private final List<Tok> tokens;
        private int index = -1;

        private ListJsonSource(List<Tok> tokens) {
            this.tokens = tokens;
        }

        @Override
        public Token next() {
            index++;
            return index < tokens.size() ? tokens.get(index).token : null;
        }

        @Override
        public String fieldName() {
            return tokens.get(index).fieldName;
        }

        @Override
        public String stringValue() {
            return tokens.get(index).text;
        }

        @Override
        public String numberText() {
            return tokens.get(index).text;
        }
    }
}
