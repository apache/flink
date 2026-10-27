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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

/**
 * Strategy for inferring a function call's intermediate result data type (i.e. state entry).
 *
 * <p>For {@link ProcessTableFunction}, returning {@link Optional#empty()} from {@link
 * #inferType(CallContext)} excludes the state entry from a specific call. A function's {@code
 * eval()} and {@code onTimer()} methods must not declare a parameter for it. This allows declaring
 * a superset of state entries while the strategy decides what a call needs.
 *
 * <p>For aggregating functions, returning {@link Optional#empty()} from {@link
 * #inferType(CallContext)} is not supported as the accumulator must always be inferable.
 */
@PublicEvolving
public interface StateTypeStrategy extends TypeStrategy {

    static StateTypeStrategy of(TypeStrategy typeStrategy) {
        return new DefaultStateTypeStrategy(typeStrategy, null, false);
    }

    static StateTypeStrategy of(TypeStrategy typeStrategy, @Nullable Duration timeToLive) {
        return new DefaultStateTypeStrategy(typeStrategy, timeToLive, false);
    }

    static StateTypeStrategy of(
            TypeStrategy typeStrategy, @Nullable Duration timeToLive, boolean broadcast) {
        return new DefaultStateTypeStrategy(typeStrategy, timeToLive, broadcast);
    }

    /**
     * Infers the data type of the state entry from the given function call.
     *
     * <p>For {@link ProcessTableFunction}, returning {@link Optional#empty()} means that the state
     * entry is not used by this call. It is then excluded from the call's state, and the function's
     * {@code eval()} and {@code onTimer()} methods must not declare a parameter for it.
     *
     * <p>For aggregating functions, the accumulator must always be inferable. Returning {@link
     * Optional#empty()} fails with a validation exception.
     */
    @Override
    Optional<DataType> inferType(CallContext callContext);

    /**
     * The time-to-live (TTL) duration that automatically cleans up the state entry.
     *
     * <p>Returning {@code Optional.empty()} will fall back to default behavior. Returning a value
     * equal or greater than 0 means setting a custom TTL for this state entry and ignoring the
     * global defaults.
     */
    Optional<Duration> getTimeToLive(CallContext callContext);

    /**
     * Whether the state entry is backed by broadcast (operator) state. In case of broadcast, the
     * state is shared across all sets instead of being scoped to a set defined by the PARTITION BY
     * clause.
     */
    default boolean isBroadcast() {
        return false;
    }
}
