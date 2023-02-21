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
import org.apache.flink.api.dag.Transformation;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * This transformation represents a union of several input {@link Transformation Transformations}.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code UnionTransformation}
 */
@Internal
public class UnionTransformation<T> extends Transformation<T> {
    private final List<Transformation<T>> inputs;

    /**
     * Creates a new {@code UnionTransformation} from the given input {@code Transformations}.
     *
     * <p>The input {@code Transformations} must all have the same type.
     *
     * @param inputs The list of input {@code Transformations}
     */
    public UnionTransformation(List<Transformation<T>> inputs) {
        super("Union", inputs.get(0).getOutputType(), inputs.get(0).getParallelism());

        for (Transformation<T> input : inputs) {
            if (!input.getOutputType().equals(getOutputType())) {
                throw new UnsupportedOperationException("Type mismatch in input " + input);
            }
        }

        this.inputs = Lists.newArrayList(inputs);
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return new ArrayList<>(inputs);
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        for (Transformation<T> input : inputs) {
            result.addAll(input.getTransitivePredecessors());
        }
        return result;
    }
}
