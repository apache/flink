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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * A step of literal paths, see {@link VariantPathReader} and {@link JsonPathReader}: the steps that
 * follow it, by key and by 0-based index, and whether a path ends here.
 */
final class JsonPathNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, JsonPathNode> fields = new HashMap<>();
    private final Map<Integer, JsonPathNode> elements = new HashMap<>();
    // a path ends here, so the value is built in full
    private boolean isEnd;

    private JsonPathNode() {}

    /**
     * The tree of {@code paths}, each a {@link String} key or an {@link Integer} index per step,
     * with each key mapped by {@code keyOf}.
     */
    static JsonPathNode of(Object[][] paths, UnaryOperator<String> keyOf) {
        final JsonPathNode root = new JsonPathNode();
        for (Object[] path : paths) {
            JsonPathNode node = root;
            for (Object step : path) {
                node =
                        step instanceof Integer
                                ? node.elements.computeIfAbsent(
                                        (Integer) step, ignored -> new JsonPathNode())
                                : node.fields.computeIfAbsent(
                                        keyOf.apply((String) step), ignored -> new JsonPathNode());
            }
            node.isEnd = true;
        }
        return root;
    }

    @Nullable
    JsonPathNode field(String key) {
        return fields.get(key);
    }

    @Nullable
    JsonPathNode element(int index) {
        return elements.get(index);
    }

    int fieldCount() {
        return fields.size();
    }

    boolean hasElements() {
        return !elements.isEmpty();
    }

    boolean isEnd() {
        return isEnd;
    }
}
