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

package org.apache.flink.table.runtime.util;

import java.io.IOException;
import java.util.Iterator;

/**
 * An internal iterator interface which presents a more restrictive API than {@link Iterator}. This
 * iterator can avoid using Tuple2 to wrap key and value.
 */
public interface KeyValueIterator<K, V> {

    /**
     * Advance this iterator by a single kv. Returns {@code false} if this iterator has no more kvs
     * and {@code true} otherwise. If this returns {@code true}, then the new kv can be retrieved by
     * calling {@link #getKey()} and {@link #getValue()}.
     */
    boolean advanceNext() throws IOException;

    /**
     * Retrieve the key from this iterator. This method is idempotent. It is illegal to call this
     * method after {@link #advanceNext()} has returned {@code false}.
     */
    K getKey();

    /**
     * Retrieve the value from this iterator. This method is idempotent. It is illegal to call this
     * method after {@link #advanceNext()} has returned {@code false}.
     */
    V getValue();
}
