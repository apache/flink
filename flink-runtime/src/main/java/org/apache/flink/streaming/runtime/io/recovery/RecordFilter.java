/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A filter interface for determining whether a record should be processed.
 *
 * <p>This interface is used during recovery to filter records for ambiguous channel mappings. For
 * example, when the downstream node of a keyed exchange is scaled from 1 to 2, the state of the
 * output side on the upstream node needs to be replicated to both channels. The filter then checks
 * the deserialized records on both downstream subtasks and filters out the irrelevant records.
 *
 * @param <T> The type of the record value.
 */
@FunctionalInterface
@Internal
public interface RecordFilter<T> {

    /**
     * Tests whether the given record should be accepted.
     *
     * @param record The stream record to test.
     * @return {@code true} if the record should be accepted, {@code false} otherwise.
     */
    boolean filter(StreamRecord<T> record);

    /**
     * Returns a filter that accepts all records.
     *
     * @param <T> The type of the record value.
     * @return A filter that always returns {@code true}.
     */
    static <T> RecordFilter<T> acceptAll() {
        return record -> true;
    }
}
