/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.function.FunctionWithException;

/**
 * Envelope that carries whether the wrapped value is deterministic or not.
 *
 * @param <T> type of the wrapped value
 */
public class DeterminismEnvelope<T> {

    private final T value;

    private final boolean isDeterministic;

    private DeterminismEnvelope(T value, boolean isDeterministic) {
        this.value = value;
        this.isDeterministic = isDeterministic;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    public T unwrap() {
        return value;
    }

    public <V, E extends Exception> DeterminismEnvelope<V> map(
            FunctionWithException<? super T, ? extends V, E> mapper) throws E {
        final V newValue = mapper.apply(value);

        if (isDeterministic) {
            return deterministicValue(newValue);
        } else {
            return nondeterministicValue(newValue);
        }
    }

    public static <T> DeterminismEnvelope<T> deterministicValue(T value) {
        return new DeterminismEnvelope<>(value, true);
    }

    public static <T> DeterminismEnvelope<T> nondeterministicValue(T value) {
        return new DeterminismEnvelope<>(value, false);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
