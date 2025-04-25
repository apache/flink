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

package org.apache.flink.events;

import org.apache.flink.AttributeBuilder;
import org.apache.flink.annotation.Experimental;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Builder used to construct {@link Event}. See {@link Event#builder(Class, String)}. */
@Experimental
public class EventBuilder implements AttributeBuilder {
    private long observedTsMillis;
    private String name;
    private String classScope;
    private String body;
    private String severity;
    private final Map<String, Object> attributes;

    public EventBuilder(String classScope, String name) {
        this.classScope = classScope;
        this.name = name;
        this.observedTsMillis = 0L;
        this.body = "";
        this.severity = "";
        this.attributes = new HashMap<>();
    }

    public EventBuilder() {
        this("", "");
    }

    public EventBuilder(Class<?> classScope) {
        this(classScope, "");
    }

    public EventBuilder(Class<?> classScope, String name) {
        this(classScope.getCanonicalName(), name);
    }

    /** Sets the timestamp for when the event happened or was observed, in milliseconds. */
    public EventBuilder setObservedTsMillis(long observedTsMillis) {
        this.observedTsMillis = observedTsMillis;
        return this;
    }

    /** Sets the name of the event. */
    public EventBuilder setName(String name) {
        this.name = name;
        return this;
    }

    /** Sets the scope of the event, typically the fully qualified name of the emitting class. */
    public EventBuilder setClassScope(String classScope) {
        this.classScope = classScope;
        return this;
    }

    /** Sets the textual description of the event. */
    public EventBuilder setBody(String body) {
        this.body = body;
        return this;
    }

    /** Sets the severity of the event, e.g. DEBUG, INFO, ... */
    public EventBuilder setSeverity(String severity) {
        this.severity = severity;
        return this;
    }

    /** Additional attribute to be attached to this {@link Event}. */
    @Override
    public EventBuilder setAttribute(String key, String value) {
        attributes.put(key, value);
        return this;
    }

    /** Additional attribute to be attached to this {@link Event}. */
    @Override
    public EventBuilder setAttribute(String key, long value) {
        attributes.put(key, value);
        return this;
    }

    /** Additional attribute to be attached to this {@link Event}. */
    @Override
    public EventBuilder setAttribute(String key, double value) {
        attributes.put(key, value);
        return this;
    }

    /** Additional attribute to be attached to this {@link Event}. */
    @Override
    public EventBuilder setAttribute(String key, boolean value) {
        attributes.put(key, value);
        return this;
    }

    public String getName() {
        return name;
    }

    /** Builds the specified instance. */
    public Event build(Map<String, String> additionalVariables) {
        if (observedTsMillis == 0L) {
            observedTsMillis = System.currentTimeMillis();
        }
        attributes.putAll(additionalVariables);
        return new SimpleEvent(observedTsMillis, name, classScope, body, severity, attributes);
    }

    public Event build() {
        return build(Collections.emptyMap());
    }
}
