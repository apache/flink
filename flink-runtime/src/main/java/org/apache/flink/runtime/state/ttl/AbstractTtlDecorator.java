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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * Base class for TTL logic wrappers.
 *
 * @param <T> Type of originally wrapped object
 */
public abstract class AbstractTtlDecorator<T> {
    /** Wrapped original state handler. */
    protected final T original;

    protected final StateTtlConfig config;

    protected final TtlTimeProvider timeProvider;

    /** Whether to renew expiration timestamp on state read access. */
    protected final boolean updateTsOnRead;

    /** Whether to renew expiration timestamp on state read access. */
    protected final boolean returnExpired;

    /** State value time to live in milliseconds. */
    protected final long ttl;

    protected AbstractTtlDecorator(
            T original, StateTtlConfig config, TtlTimeProvider timeProvider) {
        Preconditions.checkNotNull(original);
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(timeProvider);
        this.original = original;
        this.config = config;
        this.timeProvider = timeProvider;
        this.updateTsOnRead = config.getUpdateType() == StateTtlConfig.UpdateType.OnReadAndWrite;
        this.returnExpired =
                config.getStateVisibility()
                        == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;
        this.ttl = config.getTimeToLive().toMillis();
    }

    public <V> V getUnexpired(TtlValue<V> ttlValue) {
        return ttlValue == null || (!returnExpired && expired(ttlValue))
                ? null
                : ttlValue.getUserValue();
    }

    public <V> boolean expired(TtlValue<V> ttlValue) {
        return TtlUtils.expired(ttlValue, ttl, timeProvider);
    }

    public <V> TtlValue<V> wrapWithTs(V value) {
        return TtlUtils.wrapWithTs(value, timeProvider.currentTimestamp());
    }

    public <V> TtlValue<V> rewrapWithNewTs(TtlValue<V> ttlValue) {
        return wrapWithTs(ttlValue.getUserValue());
    }

    public <SE extends Throwable, CE extends Throwable, CLE extends Throwable, V>
            V getWithTtlCheckAndUpdate(
                    SupplierWithException<TtlValue<V>, SE> getter,
                    ThrowingConsumer<TtlValue<V>, CE> updater,
                    ThrowingRunnable<CLE> stateClear)
                    throws SE, CE, CLE {
        TtlValue<V> ttlValue = getWrappedWithTtlCheckAndUpdate(getter, updater, stateClear);
        return ttlValue == null ? null : ttlValue.getUserValue();
    }

    public <SE extends Throwable, CE extends Throwable, CLE extends Throwable, V>
            TtlValue<V> getWrappedWithTtlCheckAndUpdate(
                    SupplierWithException<TtlValue<V>, SE> getter,
                    ThrowingConsumer<TtlValue<V>, CE> updater,
                    ThrowingRunnable<CLE> stateClear)
                    throws SE, CE, CLE {
        TtlValue<V> ttlValue = getter.get();
        if (ttlValue == null) {
            return null;
        } else if (expired(ttlValue)) {
            stateClear.run();
            if (!returnExpired) {
                return null;
            }
        } else if (updateTsOnRead) {
            updater.accept(rewrapWithNewTs(ttlValue));
        }
        return ttlValue;
    }

    protected <T> T getElementWithTtlCheck(TtlValue<T> ttlValue) {
        if (ttlValue == null) {
            return null;
        } else if (expired(ttlValue)) {
            // don't clear state here cause forst is LSM-tree based.
            if (!returnExpired) {
                return null;
            }
        }
        return ttlValue.getUserValue();
    }
}
