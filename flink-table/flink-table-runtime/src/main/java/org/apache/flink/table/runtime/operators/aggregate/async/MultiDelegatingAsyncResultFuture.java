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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * A class which collects multiple async calls and completes when all of them are done, calling back
 * on an underlying {@link CompletableFuture}.
 */
public class MultiDelegatingAsyncResultFuture implements BiConsumer<Object, Throwable> {

    /** The future that is completed when all async calls are done. */
    private final CompletableFuture<Object> delegatedResultFuture;

    /** The total number of async calls expected. */
    private final int totalAsyncCalls;

    /** The futures of the individual async calls. */
    private List<CompletableFuture<Object>> futures = new ArrayList<>();

    private SupplierWithException<Object, Exception> resultSupplier;

    public MultiDelegatingAsyncResultFuture(
            CompletableFuture<Object> delegatedResultFuture, int totalAsyncCalls) {
        this.delegatedResultFuture = delegatedResultFuture;
        this.totalAsyncCalls = totalAsyncCalls;
    }

    /** Creates a new future for one of the expected async calls, for it to call when it is done. */
    public CompletableFuture<?> createAsyncFuture() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        futures.add(future);
        Preconditions.checkState(futures.size() <= totalAsyncCalls, "Too many async calls.");

        if (futures.size() == totalAsyncCalls) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete(this);
        }
        return future;
    }

    /**
     * Sets the supplier that will be called to produce the final result once the async calls
     * complete.
     */
    public void setResultSupplier(SupplierWithException<Object, Exception> resultSupplier) {
        this.resultSupplier = resultSupplier;
    }

    @Override
    public void accept(Object object, Throwable throwable) {
        if (throwable != null) {
            delegatedResultFuture.completeExceptionally(throwable);
        } else {
            try {
                delegatedResultFuture.complete(resultSupplier.get());
            } catch (Throwable t) {
                delegatedResultFuture.completeExceptionally(t);
            }
        }
    }
}
