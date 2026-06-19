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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;

import java.util.List;

/** A keyed {@link AbstractBroadcastStateTransformation}. */
@Internal
public class KeyedBroadcastStateTransformation<KEY, IN1, IN2, OUT>
        extends AbstractBroadcastStateTransformation<IN1, IN2, OUT> {

    private final KeyedBroadcastProcessFunction<KEY, IN1, IN2, OUT> userFunction;

    private final TypeInformation<KEY> stateKeyType;

    private final KeySelector<IN1, KEY> keySelector;

    public KeyedBroadcastStateTransformation(
            final String name,
            final Transformation<IN1> inputStream,
            final Transformation<IN2> broadcastStream,
            final KeyedBroadcastProcessFunction<KEY, IN1, IN2, OUT> userFunction,
            final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
            final TypeInformation<KEY> keyType,
            final KeySelector<IN1, KEY> keySelector,
            final TypeInformation<OUT> outTypeInfo,
            final int parallelism,
            final boolean parallelismConfigured) {
        super(
                name,
                inputStream,
                broadcastStream,
                broadcastStateDescriptors,
                outTypeInfo,
                parallelism,
                parallelismConfigured);
        this.userFunction = userFunction;

        this.stateKeyType = keyType;
        this.keySelector = keySelector;
        updateManagedMemoryStateBackendUseCase(true /* we have keyed state */);
    }

    public KeyedBroadcastProcessFunction<KEY, IN1, IN2, OUT> getUserFunction() {
        return userFunction;
    }

    public TypeInformation<KEY> getStateKeyType() {
        return stateKeyType;
    }

    public KeySelector<IN1, KEY> getKeySelector() {
        return keySelector;
    }
}
