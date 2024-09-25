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
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;

import java.io.IOException;

/**
 * An adaptor that transforms {@link org.apache.flink.runtime.state.internal.InternalValueState}
 * into {@link org.apache.flink.runtime.state.v2.internal.InternalValueState}.
 */
public class ValueStateAdaptor<K, N, V>
        extends StateAdaptor<
                K, N, org.apache.flink.runtime.state.internal.InternalValueState<K, N, V>>
        implements InternalValueState<K, N, V> {

    public ValueStateAdaptor(
            org.apache.flink.runtime.state.internal.InternalValueState<K, N, V> valueState) {
        super(valueState);
    }

    @Override
    public StateFuture<V> asyncValue() {
        try {
            return StateFutureUtils.completedFuture(delegatedState.value());
        } catch (Exception e) {
            throw new RuntimeException("Error while getting value from raw ValueState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncUpdate(V value) {
        try {
            delegatedState.update(value);
        } catch (IOException e) {
            throw new RuntimeException("Error while updating value to raw ValueState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public V value() {
        try {
            return delegatedState.value();
        } catch (Exception e) {
            throw new RuntimeException("Error while getting value from raw ValueState", e);
        }
    }

    @Override
    public void update(V value) {
        try {
            delegatedState.update(value);
        } catch (IOException e) {
            throw new RuntimeException("Error while updating value to raw ValueState", e);
        }
    }
}
