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

package org.apache.flink.traces;

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.Map;

/** Basic implementation of {@link Span}. */
@Internal
public class SimpleSpan implements Span {

    private final String scope;
    private final String name;

    private final HashMap<String, Object> attributes = new HashMap<>();
    private long startTsMillis;
    private long endTsMillis;

    public SimpleSpan(
            String scope,
            String name,
            long startTsMillis,
            long endTsMillis,
            HashMap<String, Object> attributes) {

        this.scope = scope;
        this.name = name;
        this.startTsMillis = startTsMillis;
        this.endTsMillis = endTsMillis;
        this.attributes.putAll(attributes);
    }

    @Override
    public String getScope() {
        return scope;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getStartTsMillis() {
        return startTsMillis;
    }

    @Override
    public long getEndTsMillis() {
        return endTsMillis;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return SimpleSpan.class.getSimpleName()
                + "{"
                + "scope="
                + scope
                + ", name="
                + name
                + ", startTsMillis="
                + startTsMillis
                + ", endTsMillis="
                + endTsMillis
                + ", attributes="
                + attributes
                + "}";
    }
}
