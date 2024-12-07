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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Key accounting unit holds the current in-flight key and tracks the corresponding ongoing records,
 * which is used to preserve the ordering of independent chained {@link
 * org.apache.flink.api.common.state.v2.StateFuture}.
 *
 * @param <K> the type of key
 */
public class KeyAccountingUnit<K> {
    /** The in-flight records that are being processed, their keys are different from each other. */
    private final Map<K, Object> noConflictInFlightRecords;

    public KeyAccountingUnit(int initCapacity) {
        this.noConflictInFlightRecords = new ConcurrentHashMap<>(initCapacity);
    }

    /**
     * Occupy a key for processing, the subsequent records with the same key would be blocked until
     * the previous key release.
     *
     * @return true if no one is occupying this key, and this record succeeds to take it.
     */
    public boolean occupy(Object record, K key) {
        return noConflictInFlightRecords.putIfAbsent(key, record) == null;
    }

    /** Release a key, which is invoked when a {@link RecordContext} is released. */
    public void release(Object record, K key) {
        if (noConflictInFlightRecords.remove(key) != record) {
            throw new IllegalStateException(
                    String.format(
                            "The record %s(%s) is trying to release key which it actually does not hold.",
                            record, key));
        }
    }

    @VisibleForTesting
    public int occupiedCount() {
        return noConflictInFlightRecords.size();
    }
}
