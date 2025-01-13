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

package org.apache.flink.api.common.state.v2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link StateDescriptor} for {@link org.apache.flink.api.common.state.v2.ReducingState}.
 *
 * @param <T> The type of the values that can be added to the state.
 */
@Experimental
public class ReducingStateDescriptor<T> extends StateDescriptor<T> {

    private final ReduceFunction<T> reduceFunction;

    /**
     * Creates a new {@code ReducingStateDescriptor} with the given name and default value.
     *
     * @param name The (unique) name for the state.
     * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
     * @param typeInfo The type of the values in the state.
     */
    public ReducingStateDescriptor(
            @Nonnull String name,
            @Nonnull ReduceFunction<T> reduceFunction,
            @Nonnull TypeInformation<T> typeInfo) {
        super(name, typeInfo);
        this.reduceFunction = checkNotNull(reduceFunction);
    }

    /**
     * Create a new {@code ReducingStateDescriptor} with the given stateId and the given type
     * serializer.
     *
     * @param stateId The (unique) stateId for the state.
     * @param serializer The type serializer for the values in the state.
     */
    public ReducingStateDescriptor(
            @Nonnull String stateId,
            @Nonnull ReduceFunction<T> reduceFunction,
            @Nonnull TypeSerializer<T> serializer) {
        super(stateId, serializer);
        this.reduceFunction = checkNotNull(reduceFunction);
    }

    /** Returns the reduce function to be used for the reducing state. */
    public ReduceFunction<T> getReduceFunction() {
        return reduceFunction;
    }

    @Override
    public Type getType() {
        return Type.REDUCING;
    }
}
