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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Delegates actions of {@link java.util.concurrent.CompletableFuture} to {@link ResultFuture}. This
 * is used as a bridge between {@link org.apache.flink.table.functions.AsyncTableFunction} and
 * {@link org.apache.flink.streaming.api.functions.async.AsyncFunction}.
 */
public class DelegatingResultFuture<OUT> implements BiConsumer<Collection<OUT>, Throwable> {

    private final ResultFuture<OUT> delegatedResultFuture;
    private final CompletableFuture<Collection<OUT>> completableFuture;

    public DelegatingResultFuture(ResultFuture<OUT> delegatedResultFuture) {
        this.delegatedResultFuture = delegatedResultFuture;
        this.completableFuture = new CompletableFuture<>();
        this.completableFuture.whenComplete(this);
    }

    @Override
    public void accept(Collection<OUT> outs, Throwable throwable) {
        if (throwable != null) {
            delegatedResultFuture.completeExceptionally(throwable);
        } else {
            delegatedResultFuture.complete(outs);
        }
    }

    public CompletableFuture<Collection<OUT>> getCompletableFuture() {
        return completableFuture;
    }
}
