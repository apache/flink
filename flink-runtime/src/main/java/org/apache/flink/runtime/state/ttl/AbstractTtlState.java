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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base class for TTL logic wrappers of state objects.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <SV> The type of values kept internally in state without TTL
 * @param <TTLSV> The type of values kept internally in state with TTL
 * @param <S> Type of originally wrapped state object
 */
abstract class AbstractTtlState<K, N, SV, TTLSV, S extends InternalKvState<K, N, TTLSV>>
        extends AbstractTtlDecorator<S> implements InternalKvState<K, N, SV> {
    private final TypeSerializer<SV> valueSerializer;

    /** This registered callback is to be called whenever state is accessed for read or write. */
    final Runnable accessCallback;

    AbstractTtlState(TtlStateContext<S, SV> ttlStateContext) {
        super(ttlStateContext.original, ttlStateContext.config, ttlStateContext.timeProvider);
        this.valueSerializer = ttlStateContext.valueSerializer;
        this.accessCallback = ttlStateContext.accessCallback;
    }

    <SE extends Throwable, CE extends Throwable, T> T getWithTtlCheckAndUpdate(
            SupplierWithException<TtlValue<T>, SE> getter,
            ThrowingConsumer<TtlValue<T>, CE> updater)
            throws SE, CE {
        return getWithTtlCheckAndUpdate(getter, updater, original::clear);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return original.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return original.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<SV> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        original.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<SV> safeValueSerializer) {
        throw new FlinkRuntimeException("Queryable state is not currently supported with TTL.");
    }

    @Override
    public void clear() {
        original.clear();
        accessCallback.run();
    }

    /**
     * Check if state has expired or not and update it if it has partially expired.
     *
     * @return either non expired (possibly updated) state or null if the state has expired.
     */
    @Nullable
    public abstract TTLSV getUnexpiredOrNull(@Nonnull TTLSV ttlValue);

    @Override
    public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException();
    }
}
