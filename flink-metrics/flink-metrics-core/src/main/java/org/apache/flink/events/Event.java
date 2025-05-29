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

import org.apache.flink.annotation.Experimental;

import java.util.Map;

/**
 * {@link Event} represents an event that happened in Flink for reporting, e.g. a completed
 * checkpoint or a restart.
 */
@Experimental
public interface Event {

    static EventBuilder builder(Class<?> classScope, String name) {
        return new EventBuilder(classScope, name);
    }

    /** The timestamp for when the event happened or was observed, in milliseconds. */
    long getObservedTsMillis();

    /** The name of the event. */
    String getName();

    /** The scope of the event, typically the fully qualified name of the emitting class. */
    String getClassScope();

    /** Textual description of the event. */
    String getBody();

    /** Severity of the event, e.g. DEBUG, INFO, ... */
    String getSeverity();

    /**
     * Additional attributes for the event. Currently, returned values can be of type String, Long
     * or Double, however more types can be added in the future.
     */
    Map<String, Object> getAttributes();
}
