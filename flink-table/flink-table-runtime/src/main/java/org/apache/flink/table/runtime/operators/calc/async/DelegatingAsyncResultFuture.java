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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Inspired by {@link org.apache.flink.table.runtime.operators.join.lookup.DelegatingResultFuture}
 * for {@link org.apache.flink.table.functions.AsyncScalarFunction}.
 */
public class DelegatingAsyncResultFuture implements BiConsumer<Object, Throwable> {

    private final ResultFuture<Object> delegatedResultFuture;
    private final List<Object> synchronousResults = new ArrayList<>();
    private Function<Object, RowData> outputFactory;
    private CompletableFuture<Object> future;
    private CompletableFuture<Object> convertedFuture;

    public DelegatingAsyncResultFuture(ResultFuture<Object> delegatedResultFuture) {
        this.delegatedResultFuture = delegatedResultFuture;
    }

    public synchronized void addSynchronousResult(Object object) {
        synchronousResults.add(object);
    }

    public synchronized Object getSynchronousResult(int index) {
        return synchronousResults.get(index);
    }

    public void setOutputFactory(Function<Object, RowData> outputFactory) {
        this.outputFactory = outputFactory;
    }

    public CompletableFuture<?> createAsyncFuture(
            DataStructureConverter<Object, Object> converter) {
        Preconditions.checkState(future == null);
        Preconditions.checkState(convertedFuture == null);
        Preconditions.checkNotNull(outputFactory);
        future = new CompletableFuture<>();
        convertedFuture = future.thenApply(converter::toInternal);
        this.convertedFuture.whenComplete(this);
        return future;
    }

    @Override
    public void accept(Object o, Throwable throwable) {
        if (throwable != null) {
            delegatedResultFuture.completeExceptionally(throwable);
        } else {
            try {
                RowData rowData = outputFactory.apply(o);
                delegatedResultFuture.complete(java.util.Collections.singleton(rowData));
            } catch (Throwable t) {
                delegatedResultFuture.completeExceptionally(t);
            }
        }
    }
}
