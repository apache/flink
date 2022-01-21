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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A {@link org.apache.flink.streaming.api.graph.TransformationTranslator} for the {@link
 * org.apache.flink.streaming.api.transformations.SinkTransformation}.
 */
@Internal
public class SinkTransformationTranslator<Input, Output>
        implements TransformationTranslator<Output, SinkTransformation<Input, Output>> {

    private static final String COMMITTER_NAME = "Committer";
    private static final String WRITER_NAME = "Writer";

    @Override
    public Collection<Integer> translateForBatch(
            SinkTransformation<Input, Output> transformation, Context context) {
        return translateForStreaming(transformation, context);
    }

    @Override
    public Collection<Integer> translateForStreaming(
            SinkTransformation<Input, Output> transformation, Context context) {

        SinkExpander<Input> expander =
                new SinkExpander<>(
                        transformation.getInputStream(),
                        transformation.getSink(),
                        transformation,
                        context);
        expander.expand();
        return Collections.emptyList();
    }

    /**
     * Expands the FLIP-143 Sink to a subtopology. Each part of the topology is created after the
     * previous part of the topology has been completely configured by the user. For example, if a
     * user explicitly sets the parallelism of the sink, each part of the subtopology can rely on
     * the input having that parallelism.
     */
    private static class SinkExpander<T> {
        private final SinkTransformation<T, ?> transformation;
        private final Sink<T> sink;
        private final Context context;
        private final DataStream<T> inputStream;
        private final StreamExecutionEnvironment executionEnvironment;
        private final int environmentParallelism;

        public SinkExpander(
                DataStream<T> inputStream,
                Sink<T> sink,
                SinkTransformation<T, ?> transformation,
                Context context) {
            this.inputStream = inputStream;
            this.executionEnvironment = inputStream.getExecutionEnvironment();
            this.environmentParallelism = executionEnvironment.getParallelism();
            this.transformation = transformation;
            this.sink = sink;
            this.context = context;
        }

        private void expand() {
            // Reset the environment parallelism temporarily to configure the sub topology
            // parallelism
            executionEnvironment.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);

            final int sizeBefore = executionEnvironment.getTransformations().size();

            DataStream<T> prewritten = inputStream;
            if (sink instanceof WithPreWriteTopology) {
                prewritten =
                        adjustTransformations(
                                prewritten, ((WithPreWriteTopology<T>) sink)::addPreWriteTopology);
            }

            if (sink instanceof TwoPhaseCommittingSink) {
                addCommittingTopology(sink, prewritten);
            } else {
                adjustTransformations(
                        prewritten,
                        input ->
                                input.transform(
                                        WRITER_NAME,
                                        CommittableMessageTypeInfo.noOutput(),
                                        new SinkWriterOperatorFactory<>(sink)));
            }
            final List<Transformation<?>> sinkTransformations =
                    executionEnvironment
                            .getTransformations()
                            .subList(sizeBefore, executionEnvironment.getTransformations().size());
            sinkTransformations.forEach(context::transform);

            // Remove all added sink subtransformations to avoid duplications and allow additional
            // expansions
            while (executionEnvironment.getTransformations().size() > sizeBefore) {
                executionEnvironment
                        .getTransformations()
                        .remove(executionEnvironment.getTransformations().size() - 1);
            }
            // Restore the previous parallelism of the environment
            executionEnvironment.setParallelism(environmentParallelism);
        }

        private <CommT> void addCommittingTopology(Sink<T> sink, DataStream<T> inputStream) {
            TwoPhaseCommittingSink<T, CommT> committingSink =
                    (TwoPhaseCommittingSink<T, CommT>) sink;
            TypeInformation<CommittableMessage<CommT>> typeInformation =
                    CommittableMessageTypeInfo.of(committingSink::getCommittableSerializer);

            DataStream<CommittableMessage<CommT>> written =
                    adjustTransformations(
                            inputStream,
                            input ->
                                    input.transform(
                                            WRITER_NAME,
                                            typeInformation,
                                            new SinkWriterOperatorFactory<>(sink)));

            DataStream<CommittableMessage<CommT>> precommitted = addFailOverRegion(written);

            if (sink instanceof WithPreCommitTopology) {
                precommitted =
                        adjustTransformations(
                                precommitted,
                                ((WithPreCommitTopology<T, CommT>) sink)::addPreCommitTopology);
            }
            DataStream<CommittableMessage<CommT>> committed =
                    adjustTransformations(
                            precommitted,
                            pc ->
                                    pc.transform(
                                            COMMITTER_NAME,
                                            typeInformation,
                                            new CommitterOperatorFactory<>(committingSink)));
            if (sink instanceof WithPostCommitTopology) {
                DataStream<CommittableMessage<CommT>> postcommitted = addFailOverRegion(committed);
                adjustTransformations(
                        postcommitted,
                        pc -> {
                            ((WithPostCommitTopology<T, CommT>) sink).addPostCommitTopology(pc);
                            return null;
                        });
            }
        }

        /**
         * Adds a batch exchange that materializes the output first. This is a no-op in STREAMING.
         */
        private <I> DataStream<I> addFailOverRegion(DataStream<I> input) {
            return new DataStream<>(
                    executionEnvironment,
                    new PartitionTransformation<>(
                            input.getTransformation(),
                            new ForwardPartitioner<>(),
                            StreamExchangeMode.BATCH));
        }

        private <I, R> R adjustTransformations(
                DataStream<I> inputStream, Function<DataStream<I>, R> action) {
            int numTransformsBefore = executionEnvironment.getTransformations().size();
            R result = action.apply(inputStream);
            List<Transformation<?>> transformations = executionEnvironment.getTransformations();
            List<Transformation<?>> expandedTransformations =
                    transformations.subList(numTransformsBefore, transformations.size());
            for (Transformation<?> subTransformation : expandedTransformations) {
                // Skip overwriting the parallelism for the global committer
                if (subTransformation.getName() == null
                        || !subTransformation
                                .getName()
                                .equals(
                                        StandardSinkTopologies
                                                .GLOBAL_COMMITTER_TRANSFORMATION_NAME)) {
                    subTransformation.setParallelism(transformation.getParallelism());
                }
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
                if (subTransformation.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT) {
                    // The parallelism of the transformation is by default the env parallelism, or
                    // it is overwritten by the user.
                    subTransformation.setParallelism(transformation.getParallelism());
                }
                if (subTransformation.getMaxParallelism() < 0
                        && transformation.getMaxParallelism() > 0) {
                    subTransformation.setMaxParallelism(transformation.getMaxParallelism());
                }
                if (transformation.getChainingStrategy() == null
                        || !(subTransformation instanceof PhysicalTransformation)) {
                    continue;
                }
                ((PhysicalTransformation<?>) subTransformation)
                        .setChainingStrategy(transformation.getChainingStrategy());
            }
            return result;
        }

        private void concatUid(
                Transformation<?> subTransformation,
                Function<Transformation<?>, String> getter,
                BiConsumer<Transformation<?>, String> setter,
                @Nullable String transformationName) {
            if (transformationName != null && getter.apply(transformation) != null) {
                if (transformationName.equals(COMMITTER_NAME)) {
                    setter.accept(
                            subTransformation,
                            getter.apply(transformation) + ": " + COMMITTER_NAME);
                    return;
                }
                // Set the writer operator uid to the sinks uid to support state migrations
                if (transformationName.equals(WRITER_NAME)) {
                    setter.accept(subTransformation, getter.apply(transformation));
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
