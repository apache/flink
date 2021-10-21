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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.operators.sink.BatchCommitterHandler;
import org.apache.flink.streaming.runtime.operators.sink.CommitterHandler;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.ForwardCommittingHandler;
import org.apache.flink.streaming.runtime.operators.sink.GlobalBatchCommitterHandler;
import org.apache.flink.streaming.runtime.operators.sink.GlobalStreamingCommitterHandler;
import org.apache.flink.streaming.runtime.operators.sink.NoopCommitterHandler;
import org.apache.flink.streaming.runtime.operators.sink.SinkOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingCommitterHandler;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.util.graph.StreamGraphUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkState;

/** A {@link TransformationTranslator} for the {@link SinkTransformation}. */
@Internal
public class SinkTransformationTranslator<InputT, CommT, WriterStateT, GlobalCommT>
        implements TransformationTranslator<
                Object, SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT>> {

    public static final TypeInformation<byte[]> BYTES = TypeInformation.of(byte[].class);
    protected static final Logger LOG = LoggerFactory.getLogger(SinkTransformationTranslator.class);

    /**
     * Creates a pipeline with 3 operators:
     *
     * <ul>
     *   <li>A {@link SinkOperatorFactory} with the (stateful) {@link SinkWriter} with parallelism
     *       p.
     *   <li>An optional {@link CommitterOperatorFactory} with the {@link Committer} and parallelism
     *       p. The committer is separate from the writer to create a separate failover region.
     *   <li>An optional {@link CommitterOperatorFactory} with the {@link GlobalCommitter} and
     *       parallelism 1.
     * </ul>
     */
    @Override
    public Collection<Integer> translateForBatch(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
            Context context) {

        StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), transformation);

        boolean hasGlobalCommitter =
                transformation.getSink().getGlobalCommittableSerializer().isPresent();
        boolean hasCommitter = hasCommitter(transformation, hasGlobalCommitter);

        int parallelism = getParallelism(transformation, context);
        CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> factory =
                hasCommitter || hasGlobalCommitter
                        ? new ForwardCommittingHandler.Factory<>()
                        : new NoopCommitterHandler.Factory<>();
        int upstreamId =
                addWriterAndCommitter(
                        transformation,
                        parallelism,
                        "Sink %s Writer",
                        factory,
                        hasCommitter || hasGlobalCommitter,
                        context);

        if (hasCommitter) {
            final StreamGraph streamGraph = context.getStreamGraph();
            streamGraph.addVirtualPartitionNode(
                    upstreamId,
                    Transformation.getNewNodeId(),
                    new ForwardPartitioner<>(),
                    StreamExchangeMode.BATCH);
            upstreamId =
                    addCommitter(
                            upstreamId,
                            transformation,
                            parallelism,
                            transformation.getMaxParallelism(),
                            "Sink %s Committer",
                            new BatchCommitterHandler.Factory<>(),
                            hasGlobalCommitter,
                            context);
        }

        if (hasGlobalCommitter) {
            addCommitter(
                    upstreamId,
                    transformation,
                    1,
                    1,
                    "Sink %s Global Committer",
                    new GlobalBatchCommitterHandler.Factory<>(),
                    false,
                    context);
        }
        return Collections.emptyList();
    }

    /**
     * Creates a pipeline with 2 operators:
     *
     * <ul>
     *   <li>A {@link SinkOperatorFactory} with the (stateful) {@link SinkWriter} and the optional
     *       {@link Committer} with parallelism p.
     *   <li>An optional {@link CommitterOperatorFactory} with the {@link GlobalCommitter} and
     *       parallelism 1.
     * </ul>
     */
    @Override
    public Collection<Integer> translateForStreaming(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
            Context context) {

        StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), transformation);

        boolean hasGlobalCommitter =
                transformation.getSink().getGlobalCommittableSerializer().isPresent();
        boolean hasCommitter = hasCommitter(transformation, hasGlobalCommitter);
        CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> committerHandlerFactory;
        if (hasCommitter) {
            committerHandlerFactory = new StreamingCommitterHandler.Factory<>();
        } else {
            committerHandlerFactory = new NoopCommitterHandler.Factory<>();
        }

        int parallelism = getParallelism(transformation, context);
        int upstreamId =
                addWriterAndCommitter(
                        transformation,
                        parallelism,
                        hasCommitter ? "Sink %s" : "Sink %s Writer",
                        committerHandlerFactory,
                        hasGlobalCommitter,
                        context);

        if (hasGlobalCommitter) {
            addCommitter(
                    upstreamId,
                    transformation,
                    1,
                    1,
                    "Sink %s Global Committer",
                    new GlobalStreamingCommitterHandler.Factory<>(),
                    false,
                    context);
        }
        return Collections.emptyList();
    }

    private boolean hasCommitter(
            SinkTransformation<?, ?, ?, ?> transformation, boolean hasGlobalCommitter) {
        try {
            return transformation.getSink().getCommittableSerializer().isPresent()
                            && !hasGlobalCommitter
                    || transformation.getSink().createCommitter().isPresent();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Cannot create commmitter for " + transformation.getSink(), e);
        }
    }

    /**
     * Add a sink writer node to the stream graph.
     *
     * @param sinkTransformation The transformation that the writer belongs to
     * @param parallelism The parallelism of the writer
     * @param emitCommittables
     * @return The stream node id of the writer
     */
    private int addWriterAndCommitter(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            int parallelism,
            String format,
            CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> committerHandlerFactory,
            boolean emitCommittables,
            Context context) {

        checkState(sinkTransformation.getInputs().size() == 1);

        final StreamOperatorFactory<byte[]> factory =
                new SinkOperatorFactory<>(
                        sinkTransformation.getSink(), committerHandlerFactory, emitCommittables);
        final ChainingStrategy chainingStrategy = sinkTransformation.getChainingStrategy();
        if (chainingStrategy != null) {
            factory.setChainingStrategy(chainingStrategy);
        }

        @SuppressWarnings("unchecked")
        final Transformation<InputT> input =
                (Transformation<InputT>) sinkTransformation.getInputs().get(0);
        final TypeInformation<InputT> inputTypeInfo = input.getOutputType();

        return addOperatorToStreamGraph(
                factory,
                context.getStreamNodeIds(input),
                inputTypeInfo,
                BYTES,
                String.format(format, sinkTransformation.getName()),
                sinkTransformation.getUid(),
                parallelism,
                sinkTransformation.getMaxParallelism(),
                sinkTransformation,
                context);
    }

    private int addCommitter(
            int inputId,
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            int parallelism,
            int maxParallelism,
            String format,
            CommitterHandler.Factory<? super Sink<?, CommT, ?, GlobalCommT>, CommT>
                    committerHandlerFactory,
            boolean emitDownstream,
            Context context) {
        return addOperatorToStreamGraph(
                new CommitterOperatorFactory<>(
                        sinkTransformation.getSink(), committerHandlerFactory, emitDownstream),
                Collections.singletonList(inputId),
                BYTES,
                BYTES,
                String.format(format, sinkTransformation.getName()),
                String.format(format, sinkTransformation.getUid()),
                parallelism,
                maxParallelism,
                sinkTransformation,
                context);
    }

    private int getParallelism(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            Context context) {
        return sinkTransformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                ? sinkTransformation.getParallelism()
                : context.getStreamGraph().getExecutionConfig().getParallelism();
    }

    /**
     * Add a operator to the {@link StreamGraph}.
     *
     * @param operatorFactory The operator factory
     * @param inputs A collection of upstream stream node ids.
     * @param inTypeInfo The input type information of the operator
     * @param outTypInfo The output type information of the operator
     * @param name The name of the operator.
     * @param uid The uid of the operator.
     * @param parallelism The parallelism of the operator
     * @param maxParallelism The max parallelism of the operator
     * @param sinkTransformation The sink transformation which the operator belongs to
     * @return The stream node id of the operator
     */
    private <IN, OUT> int addOperatorToStreamGraph(
            StreamOperatorFactory<OUT> operatorFactory,
            Collection<Integer> inputs,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypInfo,
            String name,
            @Nullable String uid,
            int parallelism,
            int maxParallelism,
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            Context context) {
        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        final int transformationId = Transformation.getNewNodeId();

        streamGraph.addOperator(
                transformationId,
                slotSharingGroup,
                sinkTransformation.getCoLocationGroupKey(),
                operatorFactory,
                inTypeInfo,
                outTypInfo,
                name);

        streamGraph.setParallelism(transformationId, parallelism);
        streamGraph.setMaxParallelism(transformationId, maxParallelism);

        StreamGraphUtils.configureBufferTimeout(
                streamGraph,
                transformationId,
                sinkTransformation,
                context.getDefaultBufferTimeout());
        if (uid != null) {
            streamGraph.setTransformationUID(transformationId, uid);
        }

        for (int input : inputs) {
            streamGraph.addEdge(input, transformationId, 0);
        }

        return transformationId;
    }
}
