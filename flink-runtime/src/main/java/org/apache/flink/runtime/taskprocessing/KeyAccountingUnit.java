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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Key accounting unit holds the current in-flight key and tracks the corresponding ongoing records,
 * which is used to preserve the ordering of independent chained {@link
 * org.apache.flink.api.common.state.v2.StateFuture}.
 *
 * @param <R> the type of record
 * @param <K> the type of key
 */
@Internal
public class KeyAccountingUnit<R, K> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyAccountingUnit.class);

    /** The in-flight records that are being processed, their keys are different from each other. */
    private final Map<K, R> noConflictInFlightRecords;

    public KeyAccountingUnit() {
        this.noConflictInFlightRecords = new ConcurrentHashMap<>();
    }

    /**
     * Check if the record is available for processing. This method should be called in main task
     * thread. For the same record, this method can be reentered.
     *
     * @param record the record
     * @param key the key inside the record
     * @return true if the key is available
     */
    public boolean available(R record, K key) {
        if (noConflictInFlightRecords.containsKey(key)) {
            return noConflictInFlightRecords.get(key) == record;
        }
        return true;
    }

    /**
     * Occupy a key for processing, the subsequent records with the same key would be blocked until
     * the previous key release.
     */
    public void occupy(R record, K key) {
        if (!available(record, key)) {
            throw new IllegalStateException(
                    String.format("The record %s(%s) is already occupied.", record, key));
        }
        noConflictInFlightRecords.put(key, record);
        LOG.trace("occupy key {} for record {}", key, record);
    }

    /** Release a key, which is invoked when a {@link RecordContext} is released. */
    public void release(R record, K key) {
        R existingRecord = noConflictInFlightRecords.remove(key);
        LOG.trace("release key {} for record {}, existing record {}", key, record, existingRecord);
    }
}
