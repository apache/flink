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

package org.apache.flink.runtime.taskprocessing;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/**
 * A context that preserves the necessary variables required by each operation, all operations for
 * one record will share the same element context.
 *
 * @param <R> The type of the record that extends {@link
 *     org.apache.flink.streaming.runtime.streamrecord.StreamElement}. TODO(FLIP-409): move
 *     StreamElement to flink-core or flink-runtime module.
 * @param <K> The type of the key inside the record.
 */
@Internal
public class RecordContext<R, K> extends ReferenceCounted {

    /** The record to be processed. */
    private final R record;

    /** The key inside the record. */
    private final K key;

    public RecordContext(R record, K key) {
        super(0);
        this.record = record;
        this.key = key;
    }

    public K getKey() {
        return this.key;
    }

    @Override
    protected void referenceCountReachedZero() {
        // TODO: release internal resources that this record context holds.
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordContext<?, ?> that = (RecordContext<?, ?>) o;
        if (!Objects.equals(record, that.record)) {
            return false;
        }
        return Objects.equals(key, that.key);
    }

    @Override
    public String toString() {
        return "RecordContext{" + "record=" + record + ", key=" + key + '}';
    }
}
