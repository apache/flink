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

import org.apache.flink.annotation.Experimental;

import java.util.HashMap;

/** Builder used to construct {@link Span}. See {@link Span#builder(Class, String)}. */
@Experimental
public class SpanBuilder {
    private final HashMap<String, Object> attributes = new HashMap<>();
    private final Class<?> classScope;
    private final String name;
    private long startTsMillis;
    private long endTsMillis;

    /**
     * @param classScope Flink's convention is that the scope of each {@link Span} is defined by the
     *     class that is creating it. If you are building the {@link Span} in your class {@code
     *     MyClass}, as the {@code classScope} you should pass {@code MyClass.class}.
     * @param name Human read-able name of this span, that describes what does the built {@link
     *     Span} will represent.
     */
    SpanBuilder(Class<?> classScope, String name) {
        this.classScope = classScope;
        this.name = name;
    }

    public Span build() {
        long startTsMillisToBuild = startTsMillis;
        if (startTsMillisToBuild == 0) {
            startTsMillisToBuild = System.currentTimeMillis();
        }
        long endTsMillisToBuild = endTsMillis;
        if (endTsMillisToBuild == 0) {
            endTsMillisToBuild = startTsMillisToBuild;
        }
        return new SimpleSpan(
                classScope.getCanonicalName(),
                name,
                startTsMillisToBuild,
                endTsMillisToBuild,
                attributes);
    }

    /**
     * Optionally you can manually set the {@link Span}'s startTs. If not specified, {@code
     * System.currentTimeMillis()} will be used.
     */
    public SpanBuilder setStartTsMillis(long startTsMillis) {
        this.startTsMillis = startTsMillis;
        return this;
    }

    /**
     * Optionally you can manually set the {@link Span}'s endTs. If not specified, {@code
     * startTsMillis} will be used (see {@link #setStartTsMillis(long)})..
     */
    public SpanBuilder setEndTsMillis(long endTsMillis) {
        this.endTsMillis = endTsMillis;
        return this;
    }

    /** Additional attribute to be attached to this {@link Span}. */
    public SpanBuilder setAttribute(String key, String value) {
        attributes.put(key, value);
        return this;
    }

    /** Additional attribute to be attached to this {@link Span}. */
    public SpanBuilder setAttribute(String key, long value) {
        attributes.put(key, value);
        return this;
    }

    /** Additional attribute to be attached to this {@link Span}. */
    public SpanBuilder setAttribute(String key, double value) {
        attributes.put(key, value);
        return this;
    }
}
