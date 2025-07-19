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

package org.apache.flink.table.runtime.operators.correlate.async;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Delegates actions of {@link java.util.concurrent.CompletableFuture} to {@link ResultFuture}. This
 * is used as a bridge between {@link org.apache.flink.table.functions.AsyncTableFunction} and
 * {@link org.apache.flink.streaming.api.functions.async.AsyncFunction}.
 */
public class DelegatingAsyncTableResultFuture implements BiConsumer<Collection<Object>, Throwable> {

    private final ResultFuture<Object> delegatedResultFuture;
    private final Function<Collection<Object>, Collection<Object>> wrapFunction;

    private final CompletableFuture<Collection<Object>> completableFuture;

    public DelegatingAsyncTableResultFuture(
            ResultFuture<Object> delegatedResultFuture,
            boolean needsWrapping,
            boolean isInternalResultType) {
        this.delegatedResultFuture = delegatedResultFuture;
        this.wrapFunction =
                needsWrapping
                        ? (isInternalResultType ? this::wrapInternal : this::wrapExternal)
                        : outs -> outs;
        this.completableFuture = new CompletableFuture<>();
        this.completableFuture.whenComplete(this);
    }

    @Override
    public void accept(Collection<Object> outs, Throwable throwable) {
        if (throwable != null) {
            delegatedResultFuture.completeExceptionally(throwable);
        } else {
            delegatedResultFuture.complete(wrapFunction.apply(outs));
        }
    }

    private Collection<Object> wrapInternal(Collection<Object> outs) {
        List<Object> wrapped = new ArrayList<>();
        for (Object value : outs) {
            wrapped.add(GenericRowData.of(value));
        }
        return wrapped;
    }

    private Collection<Object> wrapExternal(Collection<Object> outs) {
        List<Object> wrapped = new ArrayList<>();
        for (Object value : outs) {
            wrapped.add(Row.of(value));
        }
        return wrapped;
    }

    public CompletableFuture<Collection<Object>> getCompletableFuture() {
        return completableFuture;
    }
}
