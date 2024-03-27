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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.TransformationTranslator;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * This Transformation is a phantom transformation which is used to expose a default parallelism to
 * downstream.
 *
 * <p>It is used only when the parallelism of the source transformation differs from the default
 * parallelism, ensuring that the parallelism of downstream operations is not affected.
 *
 * <p>Moreover, this transformation does not have a corresponding {@link TransformationTranslator},
 * meaning it will not become a node in the StreamGraph.
 *
 * @param <T> The type of the elements in the input {@code Transformation}
 */
@Internal
public class SourceTransformationWrapper<T> extends Transformation<T> {

    private final Transformation<T> input;

    public SourceTransformationWrapper(Transformation<T> input) {
        super(
                "ChangeToDefaultParallel",
                input.getOutputType(),
                ExecutionConfig.PARALLELISM_DEFAULT);
        this.input = input;
    }

    public Transformation<T> getInput() {
        return input;
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        result.addAll(input.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }
}
