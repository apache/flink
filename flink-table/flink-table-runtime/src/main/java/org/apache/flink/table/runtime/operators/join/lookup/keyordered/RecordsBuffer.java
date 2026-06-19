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

package org.apache.flink.table.runtime.operators.join.lookup.keyordered;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link RecordsBuffer} is used to store the element that need to be processed in {@link
 * TableAsyncExecutionController}.
 */
public class RecordsBuffer<ELEMENT, KEY> {

    /**
     * The stream elements in this buffer have finished async operation but have not been output.
     */
    private final Map<KEY, Deque<ELEMENT>> finishedBuffer;

    /** The stream element in this buffer is being executed. */
    private final Map<KEY, ELEMENT> activeBuffer;

    /**
     * The element in that should wait until all preceding records on identical key finishing its
     * execution. After which the queueing element will move element into the active buffer.
     */
    private final Map<KEY, Deque<ELEMENT>> blockingBuffer;

    // ===== metrics =====
    private int blockingSize;
    private int finishSize;

    public RecordsBuffer() {
        this.activeBuffer = new ConcurrentHashMap<>();
        this.finishedBuffer = new ConcurrentHashMap<>();
        this.blockingBuffer = new ConcurrentHashMap<>();
        this.blockingSize = 0;
        this.finishSize = 0;
    }

    public void enqueueRecord(KEY key, ELEMENT record) {
        blockingBuffer.computeIfAbsent(key, k -> new LinkedList<>()).add(record);
        blockingSize++;
    }

    public Optional<ELEMENT> pop(KEY key) {
        if (!blockingBuffer.containsKey(key)) {
            return Optional.empty();
        }
        ELEMENT element = blockingBuffer.get(key).poll();
        if (element == null) {
            return Optional.empty();
        }
        blockingSize--;
        if (blockingBuffer.get(key).isEmpty()) {
            blockingBuffer.remove(key);
        }
        activeBuffer.put(key, element);
        return Optional.of(element);
    }

    public void finish(KEY key, ELEMENT element) {
        finishedBuffer.computeIfAbsent(key, k -> new LinkedList<>()).add(element);
        finishSize++;
        Preconditions.checkState(activeBuffer.containsKey(key));
        activeBuffer.remove(key);
    }

    public void output(KEY key, ELEMENT element) {
        Preconditions.checkState(finishedBuffer.containsKey(key));
        finishedBuffer.get(key).remove(element);
        finishSize--;
        if (finishedBuffer.get(key).isEmpty()) {
            finishedBuffer.remove(key);
        }
    }

    /** Collect all elements which are not emitted for snapshot. */
    public Map<KEY, Deque<ELEMENT>> pendingElements() {
        Map<KEY, Deque<ELEMENT>> mergedMap = new HashMap<>();
        finishedBuffer.forEach(
                (key, value) ->
                        mergedMap.merge(
                                key,
                                value,
                                (existingValue, newValue) -> {
                                    existingValue.addAll(newValue);
                                    return existingValue;
                                }));
        activeBuffer.forEach(
                (key, value) ->
                        mergedMap.computeIfAbsent(key, k -> new ArrayDeque<>()).push(value));
        blockingBuffer.forEach(
                (key, value) ->
                        mergedMap.merge(
                                key,
                                value,
                                (existingValue, newValue) -> {
                                    existingValue.addAll(newValue);
                                    return existingValue;
                                }));
        return mergedMap;
    }

    public String sizeToString() {
        int finishSize = 0;
        for (Deque<ELEMENT> deque : finishedBuffer.values()) {
            finishSize += deque.size();
        }
        int activeSize = activeBuffer.size();
        int blockingSize = blockingBuffer.size();
        for (Deque<ELEMENT> deque : blockingBuffer.values()) {
            blockingSize += deque.size();
        }
        return "finished buffer size = "
                + finishSize
                + " active buffer size = "
                + activeSize
                + " blocking buffer size = "
                + blockingSize;
    }

    public void close() {
        finishedBuffer.clear();
        activeBuffer.clear();
        blockingBuffer.clear();
    }

    // ===== metrics =====

    public int getBlockingSize() {
        return blockingSize;
    }

    public int getActiveSize() {
        return activeBuffer.size();
    }

    public int getFinishSize() {
        return finishSize;
    }

    // ===== visible for test =====

    @VisibleForTesting
    public Map<KEY, Deque<ELEMENT>> getFinishedBuffer() {
        return finishedBuffer;
    }

    @VisibleForTesting
    public Map<KEY, ELEMENT> getActiveBuffer() {
        return activeBuffer;
    }

    @VisibleForTesting
    public Map<KEY, Deque<ELEMENT>> getBlockingBuffer() {
        return blockingBuffer;
    }
}
