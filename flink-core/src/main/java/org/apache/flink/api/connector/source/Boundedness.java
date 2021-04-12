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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The boundedness of a stream. A stream could either be "bounded" (a stream with finite records) or
 * "unbounded" (a stream with infinite records).
 */
@PublicEvolving
public enum Boundedness {
    /**
     * A BOUNDED stream is a stream with finite records.
     *
     * <p>In the context of sources, a BOUNDED stream expects the source to put a boundary of the
     * records it emits. Such boundaries could be number of records, number of bytes, elapsed time,
     * and so on. Such indication of how to bound a stream is typically passed to the sources via
     * configurations. When the sources emit a BOUNDED stream, Flink may leverage this property to
     * do specific optimizations in the execution.
     *
     * <p>Unlike unbounded streams, the bounded streams are usually order insensitive. That means
     * the source implementations may not have to keep track of the event times or watermarks.
     * Instead, a higher throughput would be preferred.
     */
    BOUNDED,

    /**
     * A CONTINUOUS_UNBOUNDED stream is a stream with infinite records.
     *
     * <p>In the context of sources, an infinite stream expects the source implementation to run
     * without an upfront indication to Flink that they will eventually stop. The sources may
     * eventually be terminated when users cancel the jobs or some source-specific condition is met.
     *
     * <p>A CONTINUOUS_UNBOUNDED stream may also eventually stop at some point. But before that
     * happens, Flink always assumes the sources are going to run forever.
     */
    CONTINUOUS_UNBOUNDED
}
