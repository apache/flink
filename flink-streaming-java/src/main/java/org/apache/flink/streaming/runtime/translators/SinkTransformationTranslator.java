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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkOperatorFactory;
import org.apache.flink.streaming.util.graph.StreamGraphUtils;
import org.apache.flink.util.FlinkRuntimeException;

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

    protected static final Logger LOG = LoggerFactory.getLogger(SinkTransformationTranslator.class);

    @Override
    public Collection<Integer> translateForBatch(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
            Context context) {

        StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), transformation);
        final int parallelism = getParallelism(transformation, context);

        try {
            internalTranslate(transformation, parallelism, true, context);
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Could not add the Committer or GlobalCommitter to the stream graph.", e);
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<Integer> translateForStreaming(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
            Context context) {

        StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), transformation);

        final int parallelism = getParallelism(transformation, context);

        try {
            internalTranslate(transformation, parallelism, false, context);
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Could not add the Committer or GlobalCommitter to the stream graph.", e);
        }

        return Collections.emptyList();
    }

    /**
     * Add the sink operators to the stream graph.
     *
     * @param sinkTransformation The sink transformation that committer and global committer belongs
     *     to.
     * @param writerParallelism The parallelism of the writer operator.
     * @param batch Specifies if this sink is executed in batch mode.
     */
    private void internalTranslate(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            int writerParallelism,
            boolean batch,
            Context context)
            throws IOException {

        StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), sinkTransformation);

        Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = sinkTransformation.getSink();
        boolean needsCommitterOperator =
                batch && sink.getCommittableSerializer().isPresent()
                        || sink.getGlobalCommittableSerializer().isPresent();
        final int writerId =
                addWriterAndCommitter(
                        sinkTransformation,
                        writerParallelism,
                        batch,
                        needsCommitterOperator,
                        context);

        if (needsCommitterOperator) {
            addGlobalCommitter(writerId, sinkTransformation, batch, context);
        }
    }

    /**
     * Add a sink writer node to the stream graph.
     *
     * @param sinkTransformation The transformation that the writer belongs to
     * @param parallelism The parallelism of the writer
     * @param batch Specifies if this sink is executed in batch mode.
     * @param shouldEmit Specifies whether the write should emit committables.
     * @return The stream node id of the writer
     */
    private int addWriterAndCommitter(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            int parallelism,
            boolean batch,
            boolean shouldEmit,
            Context context) {

        Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = sinkTransformation.getSink();
        checkState(sinkTransformation.getInputs().size() == 1);
        @SuppressWarnings("unchecked")
        final Transformation<InputT> input =
                (Transformation<InputT>) sinkTransformation.getInputs().get(0);
        final TypeInformation<InputT> inputTypeInfo = input.getOutputType();

        final StreamOperatorFactory<byte[]> factory =
                new SinkOperatorFactory<>(sink, batch, shouldEmit);

        final ChainingStrategy chainingStrategy = sinkTransformation.getChainingStrategy();

        if (chainingStrategy != null) {
            factory.setChainingStrategy(chainingStrategy);
        }

        final String format = batch && shouldEmit ? "Sink %s Writer" : "Sink %s";

        return addOperatorToStreamGraph(
                factory,
                context.getStreamNodeIds(input),
                inputTypeInfo,
                TypeInformation.of(byte[].class),
                String.format(format, sinkTransformation.getName()),
                sinkTransformation.getUid(),
                parallelism,
                sinkTransformation.getMaxParallelism(),
                sinkTransformation,
                context);
    }

    /**
     * Try to add a sink global committer to the stream graph.
     *
     * @param inputId The global committer's input stream node id.
     * @param sinkTransformation The transformation that the global committer belongs to.
     * @param batch Specifies if this sink is executed in batch mode.
     */
    private void addGlobalCommitter(
            int inputId,
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            boolean batch,
            Context context) {

        Sink<InputT, CommT, WriterStateT, GlobalCommT> sink = sinkTransformation.getSink();

        final String format = batch ? "Sink %s Committer" : "Sink %s Global Committer";

        addOperatorToStreamGraph(
                new CommitterOperatorFactory<>(sink, batch),
                Collections.singletonList(inputId),
                TypeInformation.of(byte[].class),
                null,
                String.format(format, sinkTransformation.getName()),
                sinkTransformation.getUid() == null
                        ? null
                        : String.format(format, sinkTransformation.getUid()),
                1,
                1,
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
