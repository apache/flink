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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link StateDescriptor} for {@link org.apache.flink.api.common.state.v2.AggregatingState}.
 *
 * <p>The type internally stored in the state is the type of the {@code Accumulator} of the {@code
 * AggregateFunction}.
 *
 * @param <IN> The type of the values that are added to the state.
 * @param <ACC> The type of the accumulator (intermediate aggregation state).
 * @param <OUT> The type of the values that are returned from the state.
 */
public class AggregatingStateDescriptor<IN, ACC, OUT> extends StateDescriptor<ACC> {

    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    /**
     * Create a new state descriptor with the given name, function, and type.
     *
     * @param stateId The (unique) name for the state.
     * @param aggregateFunction The {@code AggregateFunction} used to aggregate the state.
     * @param typeInfo The type of the accumulator. The accumulator is stored in the state.
     */
    public AggregatingStateDescriptor(
            @Nonnull String stateId,
            @Nonnull AggregateFunction<IN, ACC, OUT> aggregateFunction,
            @Nonnull TypeInformation<ACC> typeInfo) {
        super(stateId, typeInfo);
        this.aggregateFunction = checkNotNull(aggregateFunction);
    }

    /**
     * Create a new state descriptor with the given name, function, and type.
     *
     * @param stateId The (unique) name for the state.
     * @param aggregateFunction The {@code AggregateFunction} used to aggregate the state.
     * @param typeInfo The type of the accumulator. The accumulator is stored in the state.
     * @param serializerConfig The serializer related config used to generate TypeSerializer.
     */
    public AggregatingStateDescriptor(
            @Nonnull String stateId,
            @Nonnull AggregateFunction<IN, ACC, OUT> aggregateFunction,
            @Nonnull TypeInformation<ACC> typeInfo,
            SerializerConfig serializerConfig) {
        super(stateId, typeInfo, serializerConfig);
        this.aggregateFunction = checkNotNull(aggregateFunction);
    }

    /** Returns the Aggregate function for this state. */
    public AggregateFunction<IN, ACC, OUT> getAggregateFunction() {
        return aggregateFunction;
    }

    @Override
    public Type getType() {
        return Type.AGGREGATING;
    }
}
