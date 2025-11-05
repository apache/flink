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

import java.util.List;
import java.util.Map;

/**
 * Span represents something that happened in Flink at certain point of time, that will be reported
 * to a {@link org.apache.flink.traces.reporter.TraceReporter}.
 */
@Experimental
public interface Span {

    static SpanBuilder builder(Class<?> classScope, String name) {
        return new SpanBuilder(classScope, name);
    }

    String getScope();

    String getName();

    long getStartTsMillis();

    long getEndTsMillis();

    /**
     * Currently returned values can be of type String, Long or Double, however more types can be
     * added in the future.
     */
    Map<String, Object> getAttributes();

    /** Returns the child spans (= nested). */
    List<Span> getChildren();
}
