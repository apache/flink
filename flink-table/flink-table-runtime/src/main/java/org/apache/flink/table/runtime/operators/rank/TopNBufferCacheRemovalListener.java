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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalCause;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalNotification;

/**
 * A common cache removal listener for rank node.
 *
 * @param <V> is the value type of the cache.
 */
public class TopNBufferCacheRemovalListener<V> implements RemovalListener<RowData, V> {
    // Why not use the executionContext? because the AbstractTopNFunction relies on the keyContext.
    private final KeyContext keyContext;
    private final ThrowingConsumer<V, Exception> callBack;

    public TopNBufferCacheRemovalListener(
            KeyContext keyContext, ThrowingConsumer<V, Exception> callBack) {
        this.keyContext = keyContext;
        this.callBack = callBack;
    }

    @Override
    public void onRemoval(RemovalNotification<RowData, V> removalNotification) {
        if (removalNotification.getCause() != RemovalCause.SIZE
                || removalNotification.getValue() == null) {
            // Don't flush values to state if removed because of ttl
            return;
        }
        RowData previousKey = (RowData) keyContext.getCurrentKey();
        RowData partitionKey = removalNotification.getKey();
        V value = removalNotification.getValue();
        if (partitionKey == null || value == null) {
            return;
        }
        keyContext.setCurrentKey(partitionKey);
        try {
            callBack.accept(value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute callback", e);
        } finally {
            keyContext.setCurrentKey(previousKey);
        }
    }
}
