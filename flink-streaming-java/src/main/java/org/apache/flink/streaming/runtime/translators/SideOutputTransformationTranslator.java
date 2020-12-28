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
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link SideOutputTransformation}.
 *
 * @param <OUT> The type of the elements that result from the {@code SideOutputTransformation} being
 *     translated.
 */
@Internal
public class SideOutputTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, SideOutputTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final SideOutputTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final SideOutputTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    private Collection<Integer> translateInternal(
            final SideOutputTransformation<OUT> transformation, final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        final StreamGraph streamGraph = context.getStreamGraph();
        final List<Transformation<?>> parentTransformations = transformation.getInputs();
        checkState(
                parentTransformations.size() == 1,
                "Expected exactly one input transformation but found "
                        + parentTransformations.size());

        final List<Integer> virtualResultIds = new ArrayList<>();
        final Transformation<?> parentTransformation = parentTransformations.get(0);
        for (int inputId : context.getStreamNodeIds(parentTransformation)) {
            final int virtualId = Transformation.getNewNodeId();
            streamGraph.addVirtualSideOutputNode(inputId, virtualId, transformation.getOutputTag());
            virtualResultIds.add(virtualId);
        }
        return virtualResultIds;
    }
}
