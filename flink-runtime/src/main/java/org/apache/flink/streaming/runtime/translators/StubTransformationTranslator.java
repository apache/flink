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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.StubTransformation;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link StubTransformation}.
 *
 * <p>The translator finds the upstream transformation based on the predicate provided in the {@link
 * StubTransformation} and implicitly connects upstream to the downstream transformations of the
 * {@link StubTransformation}.
 *
 * @param <OUT> The type of the elements that result from the {@link StubTransformation} being
 *     translated.
 */
@Internal
public class StubTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, StubTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final StubTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final StubTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    private Collection<Integer> translateInternal(
            final StubTransformation<OUT> stubTransformation, final Context context) {
        checkNotNull(stubTransformation);
        checkNotNull(context);

        Collection<Transformation<?>> upstreams =
                findUpstreams(stubTransformation, context.getSinkTransformations());

        List<Transformation<?>> inputs = extractInputs(upstreams, stubTransformation);

        return inputs.stream()
                .flatMap(input -> context.transform(input).stream())
                .collect(Collectors.toList());
    }

    private List<Transformation<?>> extractInputs(
            Collection<Transformation<?>> upstreams, StubTransformation<OUT> stubTransformation) {
        List<Transformation<?>> inputs =
                upstreams.stream()
                        .map(stubTransformation.getInputAdjuster())
                        .collect(Collectors.toList());
        for (Transformation<?> input : inputs) {
            checkState(
                    input.getOutputType().equals(stubTransformation.getOutputType()),
                    "The output type of the input transformation does not match the expected output type of the StubTransformation. StubTransformation: %s, Input Transformation: %s, Expected Output Type: %s, Actual Output Type: %s",
                    stubTransformation,
                    input,
                    stubTransformation.getOutputType(),
                    input.getOutputType());
        }
        return inputs;
    }

    private Collection<Transformation<?>> findUpstreams(
            StubTransformation<?> stubTransformation, Collection<Transformation<?>> sinks) {
        Predicate<Transformation<?>> upstreamFinder = stubTransformation.getUpstreamFinder();
        Map<Integer, Transformation<?>> upstreams = new HashMap<>();
        for (Transformation<?> sink : sinks) {
            for (Transformation<?> transformation : sink.getTransitivePredecessors()) {
                if (upstreamFinder.test(transformation)) {
                    upstreams.put(transformation.getId(), transformation);
                }
            }
        }
        if (upstreams.isEmpty()) {
            throw new IllegalStateException(
                    "No upstream transformation found for StubTransformation: "
                            + stubTransformation);
        }
        return upstreams.values();
    }
}
