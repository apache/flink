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
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.core.state.StateFutureUtils;

import java.io.IOException;

/**
 * A wrapper that transforms {@link org.apache.flink.api.common.state.ValueState} into {@link
 * ValueState}.
 *
 * @param <V> Type of the value in the state.
 */
public class ValueStateWrapper<V> implements ValueState<V> {
    private final org.apache.flink.api.common.state.ValueState<V> valueState;

    public ValueStateWrapper(org.apache.flink.api.common.state.ValueState<V> valueState) {
        this.valueState = valueState;
    }

    public StateFuture<V> asyncValue() {
        try {
            return StateFutureUtils.completedFuture(valueState.value());
        } catch (Exception e) {
            throw new RuntimeException("Error while getting value from raw ValueState", e);
        }
    }

    public StateFuture<Void> asyncUpdate(V value) {
        try {
            valueState.update(value);
        } catch (IOException e) {
            throw new RuntimeException("Error while updating value from raw ValueState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    public V value() {
        try {
            return valueState.value();
        } catch (Exception e) {
            throw new RuntimeException("Error while getting value from raw ValueState", e);
        }
    }

    public void update(V value) {
        try {
            valueState.update(value);
        } catch (IOException e) {
            throw new RuntimeException("Error while updating value from raw ValueState", e);
        }
    }

    public StateFuture<Void> asyncClear() {
        valueState.clear();
        return StateFutureUtils.completedVoidFuture();
    }

    public void clear() {
        valueState.clear();
    }
}
