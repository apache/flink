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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.forstdb.RocksIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** The ForSt {@link ForStDBIterRequest} which returns the keys of a ForStMapState. */
public class ForStDBMapKeyIterRequest<K, N, UK, UV> extends ForStDBIterRequest<K, N, UK, UV, UK> {

    private final InternalAsyncFuture<StateIterator<UK>> future;

    public ForStDBMapKeyIterRequest(
            ContextKey<K, N> contextKey,
            ForStMapState<K, N, UK, UV> table,
            StateRequestHandler stateRequestHandler,
            @Nullable RocksIterator rocksIterator,
            InternalAsyncFuture<StateIterator<UK>> future) {
        super(contextKey, table, stateRequestHandler, rocksIterator);
        this.future = future;
    }

    @Override
    public void completeStateFutureExceptionally(String message, Throwable ex) {
        future.completeExceptionally(message, ex);
    }

    @Override
    public Collection<UK> deserializeElement(List<RawEntry> entries, int userKeyOffset)
            throws IOException {
        Collection<UK> deserializedEntries = new ArrayList<>(entries.size());
        for (RawEntry en : entries) {
            // TODO: Determine whether this is from sync mode, and if so, filter the entries with
            // null value.
            deserializedEntries.add(deserializeUserKey(en.rawKeyBytes, userKeyOffset));
        }
        return deserializedEntries;
    }

    @Override
    public void buildIteratorAndCompleteFuture(Collection<UK> partialResult, boolean encounterEnd) {
        ForStMapIterator<UK> stateIterator =
                new ForStMapIterator<>(
                        table,
                        StateRequestType.MAP_ITER_KEY,
                        StateRequestType.ITERATOR_LOADING,
                        stateRequestHandler,
                        partialResult,
                        encounterEnd,
                        rocksIterator);

        future.complete(stateIterator);
    }
}
