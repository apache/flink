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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;

/**
 * {@link StateDescriptor} for {@link ValueState}. This can be used to create partitioned value
 * state internally.
 *
 * @param <T> The type of the values that the value state can hold.
 */
@Experimental
public class ValueStateDescriptor<T> extends StateDescriptor<T> {

    /**
     * Creates a new {@code ValueStateDescriptor} with the given stateId and type.
     *
     * @param stateId The (unique) stateId for the state.
     * @param typeInfo The type of the values in the state.
     */
    public ValueStateDescriptor(@Nonnull String stateId, @Nonnull TypeInformation<T> typeInfo) {
        super(stateId, typeInfo);
    }

    /**
     * Create a new {@code ValueStateDescriptor} with the given stateId and the given type
     * serializer.
     *
     * @param stateId The (unique) stateId for the state.
     * @param serializer The type serializer for the values in the state.
     */
    public ValueStateDescriptor(@Nonnull String stateId, @Nonnull TypeSerializer<T> serializer) {
        super(stateId, serializer);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and type
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #ValueStateDescriptor(String, TypeInformation)} constructor.
     *
     * @param stateId The (unique) name for the state.
     * @param typeClass The type of the values in the state.
     */
    public ValueStateDescriptor(@Nonnull String stateId, @Nonnull Class<T> typeClass) {
        super(stateId, typeClass);
    }

    @Override
    public Type getType() {
        return Type.VALUE;
    }
}
