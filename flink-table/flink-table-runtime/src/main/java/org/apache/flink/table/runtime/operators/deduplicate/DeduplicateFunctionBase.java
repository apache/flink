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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/**
 * Base class for deduplicate function.
 *
 * @param <T> Type of the value in the state.
 * @param <K> Type of the key.
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
abstract class DeduplicateFunctionBase<T, K, IN, OUT> extends KeyedProcessFunction<K, IN, OUT> {

    private static final long serialVersionUID = 1L;

    // the TypeInformation of the values in the state.
    protected final TypeInformation<T> typeInfo;
    protected final long stateRetentionTime;
    protected final TypeSerializer<OUT> serializer;
    // state stores previous message under the key.
    protected ValueState<T> state;

    public DeduplicateFunctionBase(
            TypeInformation<T> typeInfo, TypeSerializer<OUT> serializer, long stateRetentionTime) {
        this.typeInfo = typeInfo;
        this.stateRetentionTime = stateRetentionTime;
        this.serializer = serializer;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        ValueStateDescriptor<T> stateDesc =
                new ValueStateDescriptor<>("deduplicate-state", typeInfo);
        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
        if (ttlConfig.isEnabled()) {
            stateDesc.enableTimeToLive(ttlConfig);
        }
        state = getRuntimeContext().getState(stateDesc);
    }
}
