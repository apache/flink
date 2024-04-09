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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.context.StateManager;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The default implementation of {@link StateManager}. This class supports eagerly set and reset the
 * current key.
 */
public class DefaultStateManager implements StateManager {

    /**
     * Retrieve the current key. When {@link #currentKeySetter} receives a key, this must return
     * that key until it is reset.
     */
    private final Supplier<Object> currentKeySupplier;

    private final Consumer<Object> currentKeySetter;

    public DefaultStateManager(
            Supplier<Object> currentKeySupplier, Consumer<Object> currentKeySetter) {
        this.currentKeySupplier = currentKeySupplier;
        this.currentKeySetter = currentKeySetter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> K getCurrentKey() {
        return (K) currentKeySupplier.get();
    }

    /**
     * This method should be used to run a block of code with a specific key context. The original
     * key must be reset after the block is executed.
     */
    public void executeInKeyContext(Runnable runnable, Object key) {
        final Object oldKey = currentKeySupplier.get();
        setCurrentKey(key);
        try {
            runnable.run();
        } finally {
            resetCurrentKey(oldKey);
        }
    }

    private void setCurrentKey(Object key) {
        currentKeySetter.accept(key);
    }

    private void resetCurrentKey(Object oldKey) {
        currentKeySetter.accept(oldKey);
    }
}
