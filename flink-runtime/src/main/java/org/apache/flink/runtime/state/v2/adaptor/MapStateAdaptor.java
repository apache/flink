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

package org.apache.flink.runtime.state.v2.adaptor;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;

/**
 * An adaptor that transforms {@link org.apache.flink.runtime.state.internal.InternalMapState} into
 * {@link org.apache.flink.runtime.state.v2.internal.InternalMapState}.
 */
public class MapStateAdaptor<K, N, UK, UV>
        extends StateAdaptor<
                K, N, org.apache.flink.runtime.state.internal.InternalMapState<K, N, UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    public MapStateAdaptor(
            org.apache.flink.runtime.state.internal.InternalMapState<K, N, UK, UV> mapState) {
        super(mapState);
    }

    @Override
    public StateFuture<UV> asyncGet(UK key) {
        try {
            return StateFutureUtils.completedFuture(delegatedState.get(key));
        } catch (Exception e) {
            throw new RuntimeException("Error while get value from raw MapState", e);
        }
    }

    @Override
    public UV get(UK key) {
        try {
            return delegatedState.get(key);
        } catch (Exception e) {
            throw new RuntimeException("Error while get value from raw MapState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncPut(UK key, UV value) {
        try {
            delegatedState.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating value to raw MapState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void put(UK key, UV value) {
        try {
            delegatedState.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating value to raw MapState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncPutAll(Map<UK, UV> map) {
        try {
            delegatedState.putAll(map);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating values to raw MapState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void putAll(Map<UK, UV> map) {
        try {
            delegatedState.putAll(map);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating values to raw MapState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncRemove(UK key) {
        try {
            delegatedState.remove(key);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating values to raw MapState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void remove(UK key) {
        try {
            delegatedState.remove(key);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating values to raw MapState", e);
        }
    }

    @Override
    public StateFuture<Boolean> asyncContains(UK key) {
        try {
            return StateFutureUtils.completedFuture(delegatedState.contains(key));
        } catch (Exception e) {
            throw new RuntimeException("Error while checking key from raw MapState", e);
        }
    }

    @Override
    public boolean contains(UK key) {
        try {
            return delegatedState.contains(key);
        } catch (Exception e) {
            throw new RuntimeException("Error while checking key from raw MapState", e);
        }
    }

    @Override
    public StateFuture<StateIterator<Map.Entry<UK, UV>>> asyncEntries() {
        try {
            return StateFutureUtils.completedFuture(
                    new CompleteStateIterator<>(delegatedState.entries()));
        } catch (Exception e) {
            throw new RuntimeException("Error while get entries from raw MapState", e);
        }
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        try {
            return delegatedState.entries();
        } catch (Exception e) {
            throw new RuntimeException("Error while get entries from raw MapState", e);
        }
    }

    @Override
    public StateFuture<StateIterator<UK>> asyncKeys() {
        try {
            return StateFutureUtils.completedFuture(
                    new CompleteStateIterator<>(delegatedState.keys()));
        } catch (Exception e) {
            throw new RuntimeException("Error while get keys from raw MapState", e);
        }
    }

    @Override
    public Iterable<UK> keys() {
        try {
            return delegatedState.keys();
        } catch (Exception e) {
            throw new RuntimeException("Error while get keys from raw MapState", e);
        }
    }

    @Override
    public StateFuture<StateIterator<UV>> asyncValues() {
        try {
            return StateFutureUtils.completedFuture(
                    new CompleteStateIterator<>(delegatedState.values()));
        } catch (Exception e) {
            throw new RuntimeException("Error while get values from raw MapState", e);
        }
    }

    @Override
    public Iterable<UV> values() {
        try {
            return delegatedState.values();
        } catch (Exception e) {
            throw new RuntimeException("Error while get values from raw MapState", e);
        }
    }

    @Override
    public StateFuture<Boolean> asyncIsEmpty() {
        try {
            return StateFutureUtils.completedFuture(delegatedState.isEmpty());
        } catch (Exception e) {
            throw new RuntimeException("Error while checking existence from raw MapState", e);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return delegatedState.isEmpty();
        } catch (Exception e) {
            throw new RuntimeException("Error while checking existence from raw MapState", e);
        }
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        try {
            return delegatedState.iterator();
        } catch (Exception e) {
            throw new RuntimeException("Error while get entries from raw MapState", e);
        }
    }
}
