/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.v2.ttl;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.state.ttl.AbstractTtlDecorator;
import org.apache.flink.runtime.state.ttl.TtlStateContext;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;

/**
 * Base class for TTL logic wrappers of state objects. state V2 does not support
 * FULL_STATE_SCAN_SNAPSHOT and INCREMENTAL_CLEANUP, only supports ROCKSDB_COMPACTION_FILTER.
 * UpdateType#OnReadAndWrite is also not supported in state V2.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <SV> The type of values kept internally in state without TTL
 * @param <TTLSV> The type of values kept internally in state with TTL
 * @param <S> Type of originally wrapped state object
 */
abstract class AbstractTtlState<K, N, SV, TTLSV, S extends InternalKeyedState<K, N, TTLSV>>
        extends AbstractTtlDecorator<S> implements InternalKeyedState<K, N, SV> {
    /** This registered callback is to be called whenever state is accessed for read or write. */
    protected AbstractTtlState(TtlStateContext<S, SV> ttlStateContext) {
        super(ttlStateContext.original, ttlStateContext.config, ttlStateContext.timeProvider);
    }

    @Override
    public StateFuture<Void> asyncClear() {
        return original.asyncClear();
    }

    @Override
    public void clear() {
        original.clear();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        original.setCurrentNamespace(namespace);
    }
}
