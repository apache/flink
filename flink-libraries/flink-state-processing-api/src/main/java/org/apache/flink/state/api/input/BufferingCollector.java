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

package org.apache.flink.state.api.input;

import org.apache.flink.util.Collector;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * A collector that buffers elements in memory to be pulled later by downstream operators.
 *
 * @param <T> The type of the records being collected.
 */
final class BufferingCollector<T> implements Collector<T>, Iterator<T> {
    private final Queue<T> buffer;

    BufferingCollector() {
        this.buffer = new ArrayDeque<>(1);
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty();
    }

    @Override
    public T next() {
        return buffer.poll();
    }

    @Override
    public void collect(T record) {
        buffer.add(record);
    }

    @Override
    public void close() {}
}
