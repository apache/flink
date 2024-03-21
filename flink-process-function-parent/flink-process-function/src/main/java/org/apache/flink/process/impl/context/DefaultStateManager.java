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

package org.apache.flink.process.impl.context;

import org.apache.flink.process.api.context.StateManager;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The default implementation of {@link StateManager}. This class supports eagerly set and reset the
 * current key. The following rules must be observed:
 *
 * <p>1. The current key must be reset if setCurrentKey is called.
 *
 * <p>2. If setCurrentKey is called and not reset, getCurrentKey should always return this key.
 */
public class DefaultStateManager implements StateManager {
    /** This is used to store the original key when we overwrite the current key. */
    private Optional<Object> oldKey = Optional.empty();

    /**
     * Retrieve the current key. When {@link #currentKeySetter} receives a key, this must return
     * that key until it is reset.
     */
    private final Supplier<Optional<Object>> currentKeySupplier;

    private final Consumer<Object> currentKeySetter;

    public DefaultStateManager(
            Supplier<Optional<Object>> currentKeySupplier, Consumer<Object> currentKeySetter) {
        this.currentKeySupplier = currentKeySupplier;
        this.currentKeySetter = currentKeySetter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> Optional<K> getCurrentKey() {
        try {
            return (Optional<K>) currentKeySupplier.get();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public void setCurrentKey(Object key) {
        oldKey = currentKeySupplier.get();
        currentKeySetter.accept(key);
    }

    public void resetCurrentKey() {
        oldKey.ifPresent(currentKeySetter);
    }
}
