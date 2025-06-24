/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Translator for {@link CacheTransformationTranslator}. */
@Internal
public class CacheTransformationTranslator<OUT, T extends CacheTransformation<OUT>>
        extends SimpleTransformationTranslator<OUT, T> {

    public static final String CACHE_CONSUMER_OPERATOR_NAME = "CacheRead";
    public static final String CACHE_PRODUCER_OPERATOR_NAME = "CacheWrite";

    @Override
    protected Collection<Integer> translateForBatchInternal(T transformation, Context context) {
        if (!transformation.isCached()) {
            final List<Transformation<?>> inputs = transformation.getInputs();
            Preconditions.checkState(
                    inputs.size() == 1, "There could be only one transformation input to cache");
            Transformation<?> input = inputs.get(0);
            if (input instanceof PhysicalTransformation) {
                return physicalTransformationProduceCache(transformation, context, input);
            } else if (input instanceof SideOutputTransformation) {
                return sideOutputTransformationProduceCache(
                        transformation, context, (SideOutputTransformation<?>) input);
            } else {
                throw new RuntimeException(
                        String.format("Unsupported transformation %s", input.getClass()));
            }
        } else {
            return consumeCache(transformation, context);
        }
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(T transformation, Context context) {
        if (transformation.isCached()) {
            return consumeCache(transformation, context);
        } else {
            final List<Transformation<?>> inputs = transformation.getInputs();
            Preconditions.checkState(
                    inputs.size() == 1, "There could be only one transformation input to cache");
            return context.getStreamNodeIds(inputs.get(0));
        }
    }

    private Collection<Integer> sideOutputTransformationProduceCache(
            T transformation, Context context, SideOutputTransformation<?> input) {
        final StreamGraph streamGraph = context.getStreamGraph();
        // SideOutput Transformation has only one input
        final Transformation<?> physicalTransformation = input.getInputs().get(0);

        final Collection<Integer> cacheNodeIds = context.getStreamNodeIds(physicalTransformation);

        Preconditions.checkState(
                cacheNodeIds.size() == 1, "We expect only one stream node for the input transform");

        final Integer cacheNodeId = cacheNodeIds.iterator().next();

        addCacheProduceNode(streamGraph, transformation, context, physicalTransformation);

        final int virtualId = Transformation.getNewNodeId();
        streamGraph.addVirtualSideOutputNode(cacheNodeId, virtualId, input.getOutputTag());
        streamGraph.addEdge(
                virtualId,
                transformation.getId(),
                0,
                new IntermediateDataSetID(transformation.getDatasetId()));
        return Collections.singletonList(virtualId);
    }

    private List<Integer> physicalTransformationProduceCache(
            T transformation, Context context, Transformation<?> input) {
        final StreamGraph streamGraph = context.getStreamGraph();
        final Collection<Integer> cachedNodeIds = context.getStreamNodeIds(input);

        Preconditions.checkState(
                cachedNodeIds.size() == 1,
                "We expect only one stream node for the input transform");

        final Integer cacheNodeId = cachedNodeIds.iterator().next();

        addCacheProduceNode(streamGraph, transformation, context, input);

        streamGraph.addEdge(
                cacheNodeId,
                transformation.getId(),
                0,
                new IntermediateDataSetID(transformation.getDatasetId()));
        return Collections.singletonList(cacheNodeId);
    }

    private void addCacheProduceNode(
            StreamGraph streamGraph,
            T cacheTransformation,
            Context context,
            Transformation<?> input) {
        final SimpleOperatorFactory<OUT> operatorFactory =
                SimpleOperatorFactory.of(new NoOpStreamOperator<>());
        operatorFactory.setChainingStrategy(ChainingStrategy.HEAD);
        streamGraph.addOperator(
                cacheTransformation.getId(),
                context.getSlotSharingGroup(),
                cacheTransformation.getCoLocationGroupKey(),
                operatorFactory,
                cacheTransformation.getInputs().get(0).getOutputType(),
                null,
                CACHE_PRODUCER_OPERATOR_NAME);

        streamGraph.setParallelism(
                cacheTransformation.getId(),
                input.getParallelism(),
                input.isParallelismConfigured());
        streamGraph.setMaxParallelism(cacheTransformation.getId(), input.getMaxParallelism());
    }

    private List<Integer> consumeCache(T transformation, Context context) {
        final StreamGraph streamGraph = context.getStreamGraph();
        final SimpleOperatorFactory<OUT> operatorFactory =
                SimpleOperatorFactory.of(new IdentityStreamOperator<>());
        final TypeInformation<OUT> outputType =
                transformation.getTransformationToCache().getOutputType();
        streamGraph.addLegacySource(
                transformation.getId(),
                context.getSlotSharingGroup(),
                transformation.getCoLocationGroupKey(),
                operatorFactory,
                outputType,
                outputType,
                CACHE_CONSUMER_OPERATOR_NAME);
        streamGraph.setParallelism(
                transformation.getId(),
                transformation.getTransformationToCache().getParallelism(),
                transformation.isParallelismConfigured());
        streamGraph.setMaxParallelism(
                transformation.getId(),
                transformation.getTransformationToCache().getMaxParallelism());
        final StreamNode streamNode = streamGraph.getStreamNode(transformation.getId());
        streamNode.setConsumeClusterDatasetId(
                new IntermediateDataSetID(transformation.getDatasetId()));
        return Collections.singletonList(transformation.getId());
    }

    /**
     * The {@link NoOpStreamOperator} acts as a dummy sink so that the upstream can produce the
     * intermediate dataset to be cached.
     *
     * @param <T> The output type of the operator, which is the type of the cached intermediate
     *     dataset as well.
     */
    public static class NoOpStreamOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {
        private static final long serialVersionUID = 4517845269225218313L;

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            // do nothing
        }
    }

    /**
     * The {@link IdentityStreamOperator} acts as a dummy source to consume cached intermediate
     * dataset.
     *
     * @param <T> The output type of the operator, which is the type of the cached intermediate *
     *     dataset as well.
     */
    public static class IdentityStreamOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {
        private static final long serialVersionUID = 4517845269225218313L;

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }
    }
}
