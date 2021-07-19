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
import org.apache.flink.api.connector.sink.CommittingSink;
import org.apache.flink.api.connector.sink.GlobalCommittingSink;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.StatefulSink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.operators.sink.BatchCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.BatchGlobalCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.CommittableTypeInformation;
import org.apache.flink.streaming.runtime.operators.sink.StatefulSinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatelessSinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingGlobalCommitterOperatorFactory;
import org.apache.flink.streaming.util.graph.StreamGraphUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link TransformationTranslator} for the {@link SinkTransformation}. */
@Internal
public class SinkTransformationTranslator<InputT, CommT, WriterStateT, GlobalCommT>
        implements TransformationTranslator<
                Object, SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT>> {

    protected static final Logger LOG = LoggerFactory.getLogger(SinkTransformationTranslator.class);

    // Currently we only support load the state from streaming file sink;
    private static final String PREVIOUS_SINK_STATE_NAME = "bucket-states";

    @Override
    public Collection<Integer> translateForBatch(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
            Context context) {

        StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), transformation);
        final int parallelism = getParallelism(transformation, context);

        try {
            internalTranslate(
                    transformation,
                    parallelism,
                    PREVIOUS_SINK_STATE_NAME,
                    BatchCommitterOperatorFactory::new,
                    1,
                    1,
                    BatchGlobalCommitterOperatorFactory::new,
                    context);
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
            internalTranslate(
                    transformation,
                    parallelism,
                    PREVIOUS_SINK_STATE_NAME,
                    StreamingCommitterOperatorFactory::new,
                    parallelism,
                    transformation.getMaxParallelism(),
                    StreamingGlobalCommitterOperatorFactory::new,
                    context);
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
     * @param previousSinkStateName The state name of previous sink's state.
     * @param committerFactory The committer operator factory.
     * @param committerParallelism The parallelism of the committer operator.
     * @param committerMaxParallelism The max parallelism of the committer operator.
     * @param globalCommitterFactory The global committer operator factory.
     */
    private void internalTranslate(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            int writerParallelism,
            @SuppressWarnings("SameParameterValue") @Nullable String previousSinkStateName,
            Function<
                            CommittingSink<InputT, CommT, WriterStateT>,
                            OneInputStreamOperatorFactory<CommT, CommT>>
                    committerFactory,
            int committerParallelism,
            int committerMaxParallelism,
            Function<
                            GlobalCommittingSink<InputT, CommT, WriterStateT, GlobalCommT>,
                            OneInputStreamOperatorFactory<CommT, GlobalCommT>>
                    globalCommitterFactory,
            Context context)
            throws IOException {

        StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), sinkTransformation);

        final int writerId =
                addWriter(sinkTransformation, writerParallelism, previousSinkStateName, context);

        final int committerId =
                addCommitter(
                        writerId,
                        sinkTransformation,
                        committerFactory,
                        committerParallelism,
                        committerMaxParallelism,
                        context);

        addGlobalCommitter(
                committerId > 0 ? committerId : writerId,
                sinkTransformation,
                globalCommitterFactory,
                context);
    }

    /**
     * Add a sink writer node to the stream graph.
     *
     * @param sinkTransformation The transformation that the writer belongs to
     * @param parallelism The parallelism of the writer
     * @param previousSinkStateName The state name of previous sink's state.
     * @return The stream node id of the writer
     */
    private int addWriter(
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            int parallelism,
            @Nullable String previousSinkStateName,
            Context context) {
        final boolean hasState = sinkTransformation.getSink() instanceof StatefulSink;
        checkState(sinkTransformation.getInputs().size() == 1);
        @SuppressWarnings("unchecked")
        final Transformation<InputT> input =
                (Transformation<InputT>) sinkTransformation.getInputs().get(0);
        final TypeInformation<InputT> inputTypeInfo = input.getOutputType();

        final StreamOperatorFactory<CommT> writer =
                hasState
                        ? new StatefulSinkWriterOperatorFactory<>(
                                (StatefulSink<InputT, WriterStateT>) sinkTransformation.getSink(),
                                previousSinkStateName)
                        : new StatelessSinkWriterOperatorFactory<>(sinkTransformation.getSink());

        final String prefix = "Sink Writer:";
        final ChainingStrategy chainingStrategy = sinkTransformation.getChainingStrategy();

        if (chainingStrategy != null) {
            writer.setChainingStrategy(chainingStrategy);
        }

        return addOperatorToStreamGraph(
                writer,
                context.getStreamNodeIds(input),
                inputTypeInfo,
                extractCommittableTypeInformation(sinkTransformation.getSink()),
                String.format("%s %s", prefix, sinkTransformation.getName()),
                sinkTransformation.getUid(),
                parallelism,
                sinkTransformation.getMaxParallelism(),
                sinkTransformation,
                context);
    }

    /**
     * Try to add a sink committer to the stream graph.
     *
     * @param inputId The committer's input stream node id
     * @param sinkTransformation The transformation that the committer belongs to
     * @param committerFactory The committer operator's factory
     * @param parallelism The parallelism of the committer
     * @return The stream node id of the committer or -1 if the sink topology does not include a
     *     committer.
     */
    private int addCommitter(
            int inputId,
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            Function<
                            CommittingSink<InputT, CommT, WriterStateT>,
                            OneInputStreamOperatorFactory<CommT, CommT>>
                    committerFactory,
            int parallelism,
            int maxParallelism,
            Context context) {

        if (!(sinkTransformation.getSink() instanceof CommittingSink)) {
            return -1;
        }

        final String prefix = "Sink Committer:";
        CommittingSink<InputT, CommT, WriterStateT> committingSink =
                (CommittingSink<InputT, CommT, WriterStateT>) sinkTransformation.getSink();
        final CommittableTypeInformation<CommT> committableTypeInfo =
                extractCommittableTypeInformation(committingSink);

        return addOperatorToStreamGraph(
                committerFactory.apply(committingSink),
                Collections.singletonList(inputId),
                committableTypeInfo,
                committableTypeInfo,
                String.format("%s %s", prefix, sinkTransformation.getName()),
                sinkTransformation.getUid() == null
                        ? null
                        : String.format("%s %s", prefix, sinkTransformation.getUid()),
                parallelism,
                maxParallelism,
                sinkTransformation,
                context);
    }

    /**
     * Try to add a sink global committer to the stream graph.
     *
     * @param inputId The global committer's input stream node id.
     * @param sinkTransformation The transformation that the global committer belongs to
     * @param globalCommitterFactory The global committer factory
     */
    private void addGlobalCommitter(
            int inputId,
            SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
            Function<
                            GlobalCommittingSink<InputT, CommT, WriterStateT, GlobalCommT>,
                            OneInputStreamOperatorFactory<CommT, GlobalCommT>>
                    globalCommitterFactory,
            Context context) {

        if (!(sinkTransformation.getSink() instanceof GlobalCommittingSink)) {
            return;
        }

        final String prefix = "Sink Global Committer:";

        GlobalCommittingSink<InputT, CommT, WriterStateT, GlobalCommT> committingSink =
                (GlobalCommittingSink<InputT, CommT, WriterStateT, GlobalCommT>)
                        sinkTransformation.getSink();
        addOperatorToStreamGraph(
                globalCommitterFactory.apply(committingSink),
                Collections.singletonList(inputId),
                extractCommittableTypeInformation(committingSink),
                null,
                String.format("%s %s", prefix, sinkTransformation.getName()),
                sinkTransformation.getUid() == null
                        ? null
                        : String.format("%s %s", prefix, sinkTransformation.getUid()),
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

    @Nullable
    private CommittableTypeInformation<CommT> extractCommittableTypeInformation(Sink<InputT> sink) {
        return sink instanceof CommittingSink
                ? extractCommittableTypeInformation(
                        (CommittingSink<InputT, CommT, WriterStateT>) sink)
                : null;
    }

    private CommittableTypeInformation<CommT> extractCommittableTypeInformation(
            CommittingSink<InputT, CommT, WriterStateT> sink) {
        final Type committableType = TypeExtractor.getParameterType(Sink.class, sink.getClass(), 1);
        LOG.debug(
                "Extracted committable type [{}] from sink [{}].",
                committableType.toString(),
                sink.getClass().getCanonicalName());
        return new CommittableTypeInformation<>(
                typeToClass(committableType),
                ((CommittingSink<?, CommT, ?>) sink)::getCommittableSerializer);
    }
}
