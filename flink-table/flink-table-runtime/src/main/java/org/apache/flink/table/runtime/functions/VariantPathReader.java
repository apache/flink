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
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parses JSON like {@code PARSE_JSON}, but builds only what literal paths of keys and indexes
 * reach: values at path ends in full, objects on the way with the needed keys only, and arrays on
 * the way with nulls for unneeded elements. The rest is only checked, so a document fails as in
 * {@code PARSE_JSON}.
 *
 * <p>The result has its own field name dictionary, and the size limit applies to it, not to the
 * whole document.
 */
@Internal
public final class VariantPathReader implements Serializable {

    private static final long serialVersionUID = 1L;

    // read-only, so the reader is stateless
    private final JsonPathNode root;

    /**
     * @param paths the steps of each path: a {@link String} for {@code [key]}, a 0-based {@link
     *     Integer} for {@code [index]}
     */
    public VariantPathReader(Object[][] paths) {
        this.root = JsonPathNode.of(paths, VariantPathReader::storedKey);
    }

    /**
     * Parses {@code json} for the paths. Fails as {@link SqlJsonUtils#parseJsonToVariant}, except
     * for the size limit.
     */
    public Variant parse(StringData json, boolean allowDuplicateKeys) throws IOException {
        final byte[] bytes = json.toBytes();
        final Parsing parsing = new Parsing(allowDuplicateKeys);
        try (JsonParser parser = BinaryVariantInternalBuilder.createParser(bytes)) {
            parser.nextToken();
            // an empty document fails in the builder, as in the full parse
            parsing.read(parser, root, 0);
            return parsing.builder.build();
        }
    }

    /** The state of one parse. */
    private static final class Parsing {
        private static final int MAX_CLEARED_KEYS = 64;

        private final boolean allowDuplicateKeys;
        private final BinaryVariantInternalBuilder builder;
        // keys of the open objects, by depth, to find duplicates
        private final List<Set<String>> keysByDepth = new ArrayList<>();

        private Parsing(boolean allowDuplicateKeys) {
            this.allowDuplicateKeys = allowDuplicateKeys;
            this.builder = new BinaryVariantInternalBuilder(allowDuplicateKeys);
        }

        // builds the value at node; without a node, only checks it
        private void read(JsonParser parser, @Nullable JsonPathNode node, int depth)
                throws IOException {
            final JsonToken token = parser.currentToken();
            final boolean container =
                    token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY;
            if (node != null && (node.isEnd() || !container)) {
                // a path end, or a scalar on the way, so a deeper step fails as before
                builder.appendJson(parser);
                return;
            }
            if (!container) {
                builder.checkJsonScalar(parser);
                return;
            }
            final int start = builder.getWritePos();
            if (token == JsonToken.START_OBJECT) {
                final Set<String> keys = allowDuplicateKeys ? null : keysAt(depth);
                final ArrayList<BinaryVariantInternalBuilder.FieldEntry> fields =
                        node == null ? null : new ArrayList<>();
                boolean duplicate = false;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    // Jackson rejects an unpaired surrogate in a key, so keys are used as they are
                    final String name = parser.currentName();
                    parser.nextToken();
                    if (keys != null && !keys.add(name)) {
                        duplicate = true;
                    }
                    final JsonPathNode child = node == null ? null : node.field(name);
                    if (child != null) {
                        // the builder keeps the last of duplicate keys
                        builder.addField(fields, start, name);
                    }
                    read(parser, child, depth + 1);
                }
                if (duplicate) {
                    // at the object's end, as in the full parse
                    throw BinaryVariantInternalBuilder.VARIANT_DUPLICATE_KEY_EXCEPTION;
                }
                if (node != null) {
                    builder.finishWritingObject(start, fields);
                }
            } else {
                final ArrayList<Integer> offsets = node == null ? null : new ArrayList<>();
                int index = 0;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (node == null) {
                        read(parser, null, depth + 1);
                        continue;
                    }
                    offsets.add(builder.getWritePos() - start);
                    final JsonPathNode child = node.element(index++);
                    read(parser, child, depth + 1);
                    if (child == null) {
                        // keeps the index of the next elements
                        builder.appendNull();
                    }
                }
                if (node != null) {
                    builder.finishWritingArray(start, offsets);
                }
            }
        }

        // the emptied key set for a new object at depth
        private Set<String> keysAt(int depth) {
            while (keysByDepth.size() <= depth) {
                keysByDepth.add(new HashSet<>());
            }
            Set<String> keys = keysByDepth.get(depth);
            // clear() costs the table's size, which never shrinks, so a large set is replaced
            if (keys.size() > MAX_CLEARED_KEYS) {
                keys = new HashSet<>();
                keysByDepth.set(depth, keys);
            } else {
                keys.clear();
            }
            return keys;
        }
    }

    // a path key as stored: its UTF-8 bytes turn an unpaired surrogate into '?'
    private static String storedKey(String key) {
        for (int i = 0; i < key.length(); i++) {
            if (Character.isSurrogate(key.charAt(i))) {
                return new String(key.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
            }
        }
        return key;
    }
}
