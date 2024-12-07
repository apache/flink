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

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * {@link StateDescriptor} for {@link ValueState}. This can be used to create partitioned value
 * state internally.
 *
 * @param <T> The type of the values that the value state can hold.
 */
public class ValueStateDescriptor<T> extends StateDescriptor<T> {

    /**
     * Creates a new {@code ValueStateDescriptor} with the given stateId and type.
     *
     * @param stateId The (unique) stateId for the state.
     * @param typeInfo The type of the values in the state.
     */
    public ValueStateDescriptor(String stateId, TypeInformation<T> typeInfo) {
        super(stateId, typeInfo);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given stateId and type.
     *
     * @param stateId The (unique) stateId for the state.
     * @param typeInfo The type of the values in the state.
     * @param serializerConfig The serializer related config used to generate {@code
     *     TypeSerializer}.
     */
    public ValueStateDescriptor(
            String stateId, TypeInformation<T> typeInfo, SerializerConfig serializerConfig) {
        super(stateId, typeInfo, serializerConfig);
    }

    @Override
    public Type getType() {
        return Type.VALUE;
    }
}
