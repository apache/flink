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
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import java.util.List;

/** A non-keyed {@link AbstractBroadcastStateTransformation}. */
@Internal
public class BroadcastStateTransformation<IN1, IN2, OUT>
        extends AbstractBroadcastStateTransformation<IN1, IN2, OUT> {

    private final BroadcastProcessFunction<IN1, IN2, OUT> userFunction;

    public BroadcastStateTransformation(
            final String name,
            final Transformation<IN1> inputStream,
            final Transformation<IN2> broadcastStream,
            final BroadcastProcessFunction<IN1, IN2, OUT> userFunction,
            final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
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
        updateManagedMemoryStateBackendUseCase(false /* not keyed */);
    }

    public BroadcastProcessFunction<IN1, IN2, OUT> getUserFunction() {
        return userFunction;
    }
}
