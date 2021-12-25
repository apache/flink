/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PhysicalTransformation} for a {@link
 * DataStream#assignTimestampsAndWatermarks(WatermarkStrategy)}.
 *
 * @param <IN> The input and output type of the transformation.
 */
@Internal
public class TimestampsAndWatermarksTransformation<IN> extends PhysicalTransformation<IN> {

    private final Transformation<IN> input;
    private final WatermarkStrategy<IN> watermarkStrategy;

    private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param parallelism The parallelism of this {@code Transformation}
     * @param input The input transformation of this {@code Transformation}
     * @param watermarkStrategy The {@link WatermarkStrategy} to use
     */
    public TimestampsAndWatermarksTransformation(
            String name,
            int parallelism,
            Transformation<IN> input,
            WatermarkStrategy<IN> watermarkStrategy) {
        super(name, input.getOutputType(), parallelism);
        this.input = input;
        this.watermarkStrategy = watermarkStrategy;
    }

    /** Returns the {@code TypeInformation} for the elements of the input. */
    public TypeInformation<IN> getInputType() {
        return input.getOutputType();
    }

    /** Returns the {@code WatermarkStrategy} to use. */
    public WatermarkStrategy<IN> getWatermarkStrategy() {
        return watermarkStrategy;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        List<Transformation<?>> transformations = Lists.newArrayList();
        transformations.add(this);
        transformations.addAll(input.getTransitivePredecessors());
        return transformations;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {
        this.chainingStrategy = checkNotNull(chainingStrategy);
    }
}
