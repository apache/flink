/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stub transformation that is later connected to an existing {@link Transformation}. This
 * implementation is useful when the full DAG of transformations cannot be constructed at once. For
 * example, an SQL query may not be fully defined at the time of its creation and later refined
 * (e.g., connecting side outputs).
 *
 * <p>Usage:
 *
 * <p>To create a stub which will later connect to an upstream operator, use {@link
 * #create(TypeInformation, Predicate)}:
 *
 * <pre>{@code
 * TypeInformation<T> typeInfo = ...; // expected output type of the upstream transformation
 * StubTransformation<T> inputStub = StubTransformation.create(typeInfo,
 *     t -> t.getName().equals("decoupled-transformation"));
 * Transformation<T> output = new PartitionTransformation<>(inputStub, new ShufflePartitioner<T>());
 * ...
 * }</pre>
 *
 * <p>The upstream part that may be created before or after the stub:
 *
 * <pre>{@code
 * Transformation<T> input = new OneInputTransformation<>(typeInfo, "decoupled-transformation", ...);
 * ... // independent subtopology
 * }</pre>
 *
 * <p>While translating the Transformation, the stub will find one or more upstream transformations
 * based on the predicate provided in the {@link StubTransformation} and implicitly connect
 * upstreams to the downstream transformations of the {@link StubTransformation} which effectively
 * means that it replaces itself with the union (all) of the upstream transformations.
 */
@Internal
public class StubTransformation<T> extends Transformation<T> {
    private final Predicate<Transformation<?>> upstreamFinder;
    private final Function<Transformation<?>, Transformation<?>> inputAdjuster;

    /**
     * Creates a new {@link StubTransformation} with the given name, output type, and parallelism.
     * All the properties can be modified later when the transformation is properly connected.
     */
    private StubTransformation(
            String name,
            TypeInformation<T> outputType,
            int parallelism,
            Predicate<Transformation<?>> upstreamFinder,
            Function<Transformation<?>, Transformation<?>> inputAdjuster) {
        super(name, outputType, parallelism);
        this.upstreamFinder = upstreamFinder;
        this.inputAdjuster = inputAdjuster;
    }

    /**
     * Creates a new {@link StubTransformation} with the given type information and predicate to
     * find the upstream transformation.
     */
    public static <T> StubTransformation<T> create(
            TypeInformation<T> typeInformation, Predicate<Transformation<?>> upstreamFinder) {
        return create(typeInformation, upstreamFinder, Function.identity());
    }

    /**
     * Creates a new {@link StubTransformation} with the given type information and predicate to
     * find the upstream transformation and additionally allows the upstream transformation to be
     * adjusted before connecting it to the downstream transformations. A common use case for the
     * adjuster is to attach the downstream transformations to the side-output of the upstream
     * transformation.
     */
    public static <T> StubTransformation<T> create(
            TypeInformation<T> typeInformation,
            Predicate<Transformation<?>> upstreamFinder,
            Function<Transformation<?>, Transformation<?>> inputAdjuster) {
        checkNotNull(upstreamFinder, "upstreamFinder must not be null");
        checkNotNull(inputAdjuster, "inputAdjuster must not be null");
        return new StubTransformation<>(
                "unconnected", typeInformation, 1, upstreamFinder, inputAdjuster);
    }

    public Function<Transformation<?>, Transformation<?>> getInputAdjuster() {
        return inputAdjuster;
    }

    public Predicate<Transformation<?>> getUpstreamFinder() {
        return upstreamFinder;
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        // This transformation does not have any inputs.
        // It will be implicitly connected to an upstream transformation during translation.
        return List.of(this);
    }

    @Override
    public List<Transformation<?>> getInputs() {
        // This transformation does not have any inputs.
        // It will be implicitly connected to an upstream transformation during translation.
        return List.of();
    }
}
