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

package org.apache.flink.table.runtime.operators.calc.async;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Inspired by {@link
 * org.apache.flink.table.runtime.operators.correlate.async.DelegatingAsyncTableResultFuture} but
 * for {@link org.apache.flink.table.functions.AsyncScalarFunction}.
 */
public class DelegatingAsyncResultFuture implements BiConsumer<Object, Throwable> {

    private final ResultFuture<Object> delegatedResultFuture;
    private final int totalResultSize;
    private final Map<Integer, Object> synchronousIndexToResults = new HashMap<>();
    private CompletableFuture<Object> future;
    private DataStructureConverter<Object, Object> converter;

    private int asyncIndex = -1;
    private RowKind rowKind;

    public DelegatingAsyncResultFuture(
            ResultFuture<Object> delegatedResultFuture, int totalResultSize) {
        this.delegatedResultFuture = delegatedResultFuture;
        this.totalResultSize = totalResultSize;
    }

    public synchronized void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    public synchronized void addSynchronousResult(int resultIndex, Object object) {
        synchronousIndexToResults.put(resultIndex, object);
    }

    public synchronized void addAsyncIndex(int resultIndex) {
        Preconditions.checkState(asyncIndex == -1);
        asyncIndex = resultIndex;
    }

    public CompletableFuture<?> createAsyncFuture(
            DataStructureConverter<Object, Object> converter) {
        Preconditions.checkState(future == null);
        Preconditions.checkState(this.converter == null);
        Preconditions.checkState(this.asyncIndex >= 0);
        future = new CompletableFuture<>();
        this.converter = converter;
        future.whenComplete(this);
        return future;
    }

    @Override
    public void accept(Object o, Throwable throwable) {
        if (throwable != null) {
            delegatedResultFuture.completeExceptionally(throwable);
        } else {
            try {
                delegatedResultFuture.complete(
                        () -> {
                            Object converted = converter.toInternal(o);
                            return Collections.singleton(createResult(converted));
                        });
            } catch (Throwable t) {
                delegatedResultFuture.completeExceptionally(t);
            }
        }
    }

    private RowData createResult(Object asyncResult) {
        GenericRowData result = new GenericRowData(totalResultSize);
        if (rowKind != null) {
            result.setRowKind(rowKind);
        }
        for (Map.Entry<Integer, Object> entry : synchronousIndexToResults.entrySet()) {
            result.setField(entry.getKey(), entry.getValue());
        }
        result.setField(asyncIndex, asyncResult);
        return result;
    }
}
