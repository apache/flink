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
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for Broadcast State transformations. In a nutshell, this transformation allows to take
 * a broadcast (non-keyed) stream, connect it with another keyed or non-keyed stream, and apply a
 * function on the resulting connected stream.
 *
 * <p>For more information see the <a
 * href="https://nightlies.apache.org/flink/flink-docs-stable/dev/stream/state/broadcast_state.html">
 * Broadcast State Pattern documentation page</a>.
 *
 * @param <IN1> The type of the elements in the non-broadcasted input.
 * @param <IN2> The type of the elements in the broadcasted input.
 * @param <OUT> The type of the elements that result from this transformation.
 */
@Internal
public class AbstractBroadcastStateTransformation<IN1, IN2, OUT>
        extends PhysicalTransformation<OUT> {

    private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

    private final Transformation<IN1> regularInput;

    private final Transformation<IN2> broadcastInput;

    private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

    protected AbstractBroadcastStateTransformation(
            final String name,
            final Transformation<IN1> regularInput,
            final Transformation<IN2> broadcastInput,
            final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
            final TypeInformation<OUT> outTypeInfo,
            final int parallelism) {
        super(name, outTypeInfo, parallelism);
        this.regularInput = checkNotNull(regularInput);
        this.broadcastInput = checkNotNull(broadcastInput);
        this.broadcastStateDescriptors = broadcastStateDescriptors;
    }

    protected AbstractBroadcastStateTransformation(
            final String name,
            final Transformation<IN1> regularInput,
            final Transformation<IN2> broadcastInput,
            final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
            final TypeInformation<OUT> outTypeInfo,
            final int parallelism,
            final boolean parallelismConfigured) {
        super(name, outTypeInfo, parallelism, parallelismConfigured);
        this.regularInput = checkNotNull(regularInput);
        this.broadcastInput = checkNotNull(broadcastInput);
        this.broadcastStateDescriptors = broadcastStateDescriptors;
    }

    public Transformation<IN2> getBroadcastInput() {
        return broadcastInput;
    }

    public Transformation<IN1> getRegularInput() {
        return regularInput;
    }

    public List<MapStateDescriptor<?, ?>> getBroadcastStateDescriptors() {
        return broadcastStateDescriptors;
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {
        this.chainingStrategy = checkNotNull(chainingStrategy);
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        final List<Transformation<?>> predecessors = new ArrayList<>();
        predecessors.add(this);
        predecessors.add(regularInput);
        predecessors.add(broadcastInput);
        return predecessors;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        final List<Transformation<?>> predecessors = new ArrayList<>();
        predecessors.add(regularInput);
        predecessors.add(broadcastInput);
        return predecessors;
    }
}
