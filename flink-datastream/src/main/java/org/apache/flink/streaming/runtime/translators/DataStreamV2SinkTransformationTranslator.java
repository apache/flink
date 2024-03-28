/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.stream.AbstractDataStream;
import org.apache.flink.datastream.impl.stream.NonKeyedPartitionStreamImpl;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** The {@link TransformationTranslator} for the {@link DataStreamV2SinkTransformation}. */
@Internal
public class DataStreamV2SinkTransformationTranslator<Input, Output>
        implements TransformationTranslator<Output, DataStreamV2SinkTransformation<Input, Output>> {

    private static final String COMMITTER_NAME = "Committer";
    private static final String WRITER_NAME = "Writer";

    @Override
    public Collection<Integer> translateForBatch(
            DataStreamV2SinkTransformation<Input, Output> transformation, Context context) {
        return translateInternal(transformation, context, true);
    }

    @Override
    public Collection<Integer> translateForStreaming(
            DataStreamV2SinkTransformation<Input, Output> transformation, Context context) {
        return translateInternal(transformation, context, false);
    }

    private Collection<Integer> translateInternal(
            DataStreamV2SinkTransformation<Input, Output> transformation,
            Context context,
            boolean batch) {
        SinkExpander<Input> expander =
                new SinkExpander<>(
                        transformation.getInputStream(),
                        transformation.getSink(),
                        transformation,
                        context,
                        batch);
        expander.expand();
        return Collections.emptyList();
    }

    @SuppressWarnings("rawtypes,unchecked")
    public static void registerSinkTransformationTranslator() throws Exception {
        final Field translatorMapField =
                StreamGraphGenerator.class.getDeclaredField("translatorMap");
        translatorMapField.setAccessible(true);
        final Map<Class<? extends Transformation>, TransformationTranslator<?, ?>> translatorMap =
                (Map<Class<? extends Transformation>, TransformationTranslator<?, ?>>)
                        translatorMapField.get(null);
        final Field underlyingMapField = translatorMap.getClass().getDeclaredField("m");
        underlyingMapField.setAccessible(true);
        final Map<Class<? extends Transformation>, TransformationTranslator<?, ?>> underlyingMap =
                (Map<Class<? extends Transformation>, TransformationTranslator<?, ?>>)
                        underlyingMapField.get(translatorMap);

        underlyingMap.put(
                DataStreamV2SinkTransformation.class,
                new DataStreamV2SinkTransformationTranslator<>());
    }

    /**
     * Expands the Sink to a sub-topology. Currently, user-defined topologies are not supported.
     * That is, sub-topologies will contain only committers and writers.
     */
    private static class SinkExpander<T> {
        private final DataStreamV2SinkTransformation<T, ?> transformation;
        private final Sink<T> sink;
        private final Context context;
        private final AbstractDataStream<T> inputStream;
        private final ExecutionEnvironmentImpl executionEnvironment;
        private final boolean isCheckpointingEnabled;
        private final boolean isBatchMode;

        public SinkExpander(
                AbstractDataStream<T> inputStream,
                Sink<T> sink,
                DataStreamV2SinkTransformation<T, ?> transformation,
                Context context,
                boolean isBatchMode) {
            this.inputStream = inputStream;
            this.executionEnvironment = inputStream.getEnvironment();
            this.isCheckpointingEnabled =
                    executionEnvironment.getCheckpointCfg().isCheckpointingEnabled();
            this.transformation = transformation;
            this.sink = sink;
            this.context = context;
            this.isBatchMode = isBatchMode;
        }

        private void expand() {

            final int sizeBefore = executionEnvironment.getTransformations().size();

            AbstractDataStream<T> prewritten = inputStream;

            if (sink instanceof SupportsPreWriteTopology) {
                throw new UnsupportedOperationException(
                        "Sink with pre-write topology is not supported for DataStream v2 atm.");
            } else if (sink instanceof SupportsPreCommitTopology) {
                throw new UnsupportedOperationException(
                        "Sink with pre-commit topology is not supported for DataStream v2 atm.");
            } else if (sink instanceof SupportsPostCommitTopology) {
                throw new UnsupportedOperationException(
                        "Sink with post-commit topology is not supported for DataStream v2 atm.");
            }

            if (sink instanceof SupportsCommitter) {
                addCommittingTopology(sink, prewritten);
            } else {
                adjustTransformations(
                        prewritten,
                        input ->
                                StreamUtils.transformOneInputOperator(
                                        WRITER_NAME,
                                        input,
                                        CommittableMessageTypeInfo.noOutput(),
                                        new SinkWriterOperatorFactory<>(sink)),
                        sink instanceof SupportsConcurrentExecutionAttempts);
            }

            final List<Transformation<?>> sinkTransformations =
                    executionEnvironment
                            .getTransformations()
                            .subList(sizeBefore, executionEnvironment.getTransformations().size());
            sinkTransformations.forEach(context::transform);

            // Remove all added sink sub-transformations to avoid duplications and allow additional
            // expansions
            while (executionEnvironment.getTransformations().size() > sizeBefore) {
                executionEnvironment
                        .getTransformations()
                        .remove(executionEnvironment.getTransformations().size() - 1);
            }
        }

        private <CommT> void addCommittingTopology(
                Sink<T> sink, AbstractDataStream<T> inputStream) {
            SupportsCommitter<CommT> committingSink = (SupportsCommitter<CommT>) sink;
            TypeInformation<CommittableMessage<CommT>> committableTypeInformation =
                    CommittableMessageTypeInfo.of(committingSink::getCommittableSerializer);

            adjustTransformations(
                    addWriter(sink, inputStream, committableTypeInformation),
                    stream ->
                            StreamUtils.transformOneInputOperator(
                                    COMMITTER_NAME,
                                    stream,
                                    committableTypeInformation,
                                    new CommitterOperatorFactory<>(
                                            committingSink, isBatchMode, isCheckpointingEnabled)),
                    false);
        }

        private <WriteResultT> AbstractDataStream<CommittableMessage<WriteResultT>> addWriter(
                Sink<T> sink,
                AbstractDataStream<T> inputStream,
                TypeInformation<CommittableMessage<WriteResultT>> typeInformation) {
            AbstractDataStream<CommittableMessage<WriteResultT>> written =
                    adjustTransformations(
                            inputStream,
                            input ->
                                    StreamUtils.transformOneInputOperator(
                                            WRITER_NAME,
                                            input,
                                            typeInformation,
                                            new SinkWriterOperatorFactory<>(sink)),
                            sink instanceof SupportsConcurrentExecutionAttempts);

            return addFailOverRegion(written);
        }

        /** Adds a batch exchange that materializes the output first. */
        private <I> AbstractDataStream<I> addFailOverRegion(AbstractDataStream<I> input) {
            return new NonKeyedPartitionStreamImpl<>(
                    input.getEnvironment(),
                    new PartitionTransformation<>(
                            input.getTransformation(),
                            new ForwardPartitioner<>(),
                            StreamExchangeMode.BATCH));
        }

        private <I, R> R adjustTransformations(
                AbstractDataStream<I> inputStream,
                Function<AbstractDataStream<I>, R> action,
                boolean supportsConcurrentExecutionAttempts) {
            int numTransformsBefore = executionEnvironment.getTransformations().size();
            R result = action.apply(inputStream);
            List<Transformation<?>> transformations = executionEnvironment.getTransformations();
            List<Transformation<?>> expandedTransformations =
                    transformations.subList(numTransformsBefore, transformations.size());

            final CustomSinkOperatorUidHashes operatorsUidHashes =
                    CustomSinkOperatorUidHashes.DEFAULT;
            for (Transformation<?> subTransformation : expandedTransformations) {
                // Set the operator uid hashes to support stateful upgrades without prior uids
                setOperatorUidHashIfPossible(
                        subTransformation, WRITER_NAME, operatorsUidHashes.getWriterUidHash());
                setOperatorUidHashIfPossible(
                        subTransformation,
                        COMMITTER_NAME,
                        operatorsUidHashes.getCommitterUidHash());
                setOperatorUidHashIfPossible(
                        subTransformation,
                        StandardSinkTopologies.GLOBAL_COMMITTER_TRANSFORMATION_NAME,
                        operatorsUidHashes.getGlobalCommitterUidHash());

                concatUid(
                        subTransformation,
                        Transformation::getUid,
                        Transformation::setUid,
                        subTransformation.getName());

                concatProperty(
                        subTransformation,
                        Transformation::getCoLocationGroupKey,
                        Transformation::setCoLocationGroupKey);

                concatProperty(subTransformation, Transformation::getName, Transformation::setName);

                concatProperty(
                        subTransformation,
                        Transformation::getDescription,
                        Transformation::setDescription);

                Optional<SlotSharingGroup> ssg = transformation.getSlotSharingGroup();

                if (ssg.isPresent() && !subTransformation.getSlotSharingGroup().isPresent()) {
                    subTransformation.setSlotSharingGroup(ssg.get());
                }

                // Since customized topology is not supported, inherit the parallelism value from
                // the sinkTransformation is enough.
                subTransformation.setParallelism(transformation.getParallelism());

                if (subTransformation.getMaxParallelism() < 0
                        && transformation.getMaxParallelism() > 0) {
                    subTransformation.setMaxParallelism(transformation.getMaxParallelism());
                }

                if (subTransformation instanceof PhysicalTransformation) {
                    PhysicalTransformation<?> physicalSubTransformation =
                            (PhysicalTransformation<?>) subTransformation;

                    if (transformation.getChainingStrategy() != null) {
                        physicalSubTransformation.setChainingStrategy(
                                transformation.getChainingStrategy());
                    }

                    // overrides the supportsConcurrentExecutionAttempts of transformation because
                    // it's not allowed to specify fine-grained concurrent execution attempts yet
                    physicalSubTransformation.setSupportsConcurrentExecutionAttempts(
                            supportsConcurrentExecutionAttempts);
                }
            }

            return result;
        }

        private void setOperatorUidHashIfPossible(
                Transformation<?> transformation,
                String writerName,
                @Nullable String operatorUidHash) {
            if (operatorUidHash == null || !transformation.getName().equals(writerName)) {
                return;
            }
            transformation.setUidHash(operatorUidHash);
        }

        private void concatUid(
                Transformation<?> subTransformation,
                Function<Transformation<?>, String> getter,
                BiConsumer<Transformation<?>, String> setter,
                @Nullable String transformationName) {
            if (transformationName != null && getter.apply(transformation) != null) {
                // Use the same uid pattern than for Sink V1. We deliberately decided to use the uid
                // pattern of Flink 1.13 because 1.14 did not have a dedicated committer operator.
                if (transformationName.equals(COMMITTER_NAME)) {
                    final String committerFormat = "Sink Committer: %s";
                    setter.accept(
                            subTransformation,
                            String.format(committerFormat, getter.apply(transformation)));
                    return;
                }
                // Set the writer operator uid to the sinks uid to support state migrations
                if (transformationName.equals(WRITER_NAME)) {
                    setter.accept(subTransformation, getter.apply(transformation));
                    return;
                }

                // Use the same uid pattern than for Sink V1 in Flink 1.14.
                if (transformationName.equals(
                        StandardSinkTopologies.GLOBAL_COMMITTER_TRANSFORMATION_NAME)) {
                    final String committerFormat = "Sink %s Global Committer";
                    setter.accept(
                            subTransformation,
                            String.format(committerFormat, getter.apply(transformation)));
                    return;
                }
            }
            concatProperty(subTransformation, getter, setter);
        }

        private void concatProperty(
                Transformation<?> subTransformation,
                Function<Transformation<?>, String> getter,
                BiConsumer<Transformation<?>, String> setter) {
            if (getter.apply(transformation) != null && getter.apply(subTransformation) != null) {
                setter.accept(
                        subTransformation,
                        getter.apply(transformation) + ": " + getter.apply(subTransformation));
            }
        }
    }
}
