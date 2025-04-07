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

import org.apache.flink.annotation.Internal;

import java.util.Map;

/** Basic implementation of {@link Event}. */
@Internal
public class SimpleEvent implements Event {

    private final long observedTsMillis;
    private final String name;
    private final String classScope;
    private final String body;
    private final String severity;
    private final Map<String, Object> attributes;

    SimpleEvent(
            long observedTsMillis,
            String name,
            String classScope,
            String body,
            String severity,
            Map<String, Object> attributes) {
        this.observedTsMillis = observedTsMillis;
        this.name = name;
        this.classScope = classScope;
        this.body = body;
        this.severity = severity;
        this.attributes = attributes;
    }

    @Override
    public long getObservedTsMillis() {
        return observedTsMillis;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getClassScope() {
        return classScope;
    }

    @Override
    public String getBody() {
        return body;
    }

    @Override
    public String getSeverity() {
        return severity;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }
}
