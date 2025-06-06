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
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.forstdb.RocksIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** The ForSt {@link ForStDBIterRequest} which returns the values of a ForStMapState. */
public class ForStDBMapValueIterRequest<K, N, UK, UV> extends ForStDBIterRequest<K, N, UK, UV, UV> {
    private final InternalStateFuture<StateIterator<UV>> future;

    public ForStDBMapValueIterRequest(
            ContextKey<K, N> contextKey,
            ForStMapState<K, N, UK, UV> table,
            StateRequestHandler stateRequestHandler,
            @Nullable RocksIterator rocksIterator,
            InternalStateFuture<StateIterator<UV>> future) {
        super(contextKey, table, stateRequestHandler, rocksIterator);
        this.future = future;
    }

    @Override
    public void completeStateFutureExceptionally(String message, Throwable ex) {
        future.completeExceptionally(message, ex);
    }

    @Override
    public Collection<UV> deserializeElement(List<RawEntry> entries, int userKeyOffset)
            throws IOException {
        Collection<UV> deserializedEntries = new ArrayList<>(entries.size());
        for (RawEntry en : entries) {
            UV userValue = deserializeUserValue(en.rawValueBytes);
            if (userValue != null) {
                deserializedEntries.add(userValue);
            }
        }
        return deserializedEntries;
    }

    @Override
    public void buildIteratorAndCompleteFuture(Collection<UV> partialResult, boolean encounterEnd) {
        ForStMapIterator<UV> stateIterator =
                new ForStMapIterator<>(
                        table,
                        StateRequestType.MAP_ITER_VALUE,
                        StateRequestType.ITERATOR_LOADING,
                        stateRequestHandler,
                        partialResult,
                        encounterEnd,
                        rocksIterator);

        future.complete(stateIterator);
    }
}
