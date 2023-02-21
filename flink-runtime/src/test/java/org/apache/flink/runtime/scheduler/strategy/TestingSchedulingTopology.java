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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.SchedulingPipelinedRegionComputeUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A simple scheduling topology for testing purposes. */
public class TestingSchedulingTopology implements SchedulingTopology {

    // Use linked map here to so we can get the values in inserted order
    private final Map<ExecutionVertexID, TestingSchedulingExecutionVertex>
            schedulingExecutionVertices = new LinkedHashMap<>();

    private final Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
            schedulingResultPartitions = new HashMap<>();

    private Map<ExecutionVertexID, TestingSchedulingPipelinedRegion> vertexRegions;

    @Override
    public Iterable<TestingSchedulingExecutionVertex> getVertices() {
        return Collections.unmodifiableCollection(schedulingExecutionVertices.values());
    }

    @Override
    public TestingSchedulingExecutionVertex getVertex(final ExecutionVertexID executionVertexId) {
        final TestingSchedulingExecutionVertex executionVertex =
                schedulingExecutionVertices.get(executionVertexId);
        if (executionVertex == null) {
            throw new IllegalArgumentException("can not find vertex: " + executionVertexId);
        }
        return executionVertex;
    }

    @Override
    public TestingSchedulingResultPartition getResultPartition(
            final IntermediateResultPartitionID intermediateResultPartitionId) {
        final TestingSchedulingResultPartition resultPartition =
                schedulingResultPartitions.get(intermediateResultPartitionId);
        if (resultPartition == null) {
            throw new IllegalArgumentException(
                    "can not find partition: " + intermediateResultPartitionId);
        }
        return resultPartition;
    }

    @Override
    public void registerSchedulingTopologyListener(SchedulingTopologyListener listener) {}

    @Override
    public Iterable<SchedulingPipelinedRegion> getAllPipelinedRegions() {
        return new HashSet<>(getVertexRegions().values());
    }

    @Override
    public SchedulingPipelinedRegion getPipelinedRegionOfVertex(ExecutionVertexID vertexId) {
        return getVertexRegions().get(vertexId);
    }

    private Map<ExecutionVertexID, TestingSchedulingPipelinedRegion> getVertexRegions() {
        if (vertexRegions == null) {
            generatePipelinedRegions();
        }
        return vertexRegions;
    }

    private void generatePipelinedRegions() {
        vertexRegions = new HashMap<>();

        final Set<Set<SchedulingExecutionVertex>> rawRegions =
                SchedulingPipelinedRegionComputeUtil.computePipelinedRegions(
                        getVertices(), this::getVertex, this::getResultPartition);

        for (Set<SchedulingExecutionVertex> rawRegion : rawRegions) {
            final Set<TestingSchedulingExecutionVertex> vertices =
                    rawRegion.stream()
                            .map(vertex -> schedulingExecutionVertices.get(vertex.getId()))
                            .collect(Collectors.toSet());
            final TestingSchedulingPipelinedRegion region =
                    new TestingSchedulingPipelinedRegion(vertices);
            for (TestingSchedulingExecutionVertex vertex : vertices) {
                vertexRegions.put(vertex.getId(), region);
            }
        }
    }

    private void resetPipelinedRegions() {
        vertexRegions = null;
    }

    void addSchedulingExecutionVertex(TestingSchedulingExecutionVertex schedulingExecutionVertex) {
        checkState(!schedulingExecutionVertices.containsKey(schedulingExecutionVertex.getId()));

        schedulingExecutionVertices.put(
                schedulingExecutionVertex.getId(), schedulingExecutionVertex);
        updateVertexResultPartitions(schedulingExecutionVertex);
        resetPipelinedRegions();
    }

    private void updateVertexResultPartitions(
            final TestingSchedulingExecutionVertex schedulingExecutionVertex) {
        addSchedulingResultPartitions(schedulingExecutionVertex.getConsumedResults());
        addSchedulingResultPartitions(schedulingExecutionVertex.getProducedResults());
    }

    private void addSchedulingResultPartitions(
            final Iterable<TestingSchedulingResultPartition> resultPartitions) {
        for (TestingSchedulingResultPartition schedulingResultPartition : resultPartitions) {
            schedulingResultPartitions.put(
                    schedulingResultPartition.getId(), schedulingResultPartition);
        }
    }

    void addSchedulingExecutionVertices(List<TestingSchedulingExecutionVertex> vertices) {
        for (TestingSchedulingExecutionVertex vertex : vertices) {
            addSchedulingExecutionVertex(vertex);
        }
    }

    public SchedulingExecutionVerticesBuilder addExecutionVertices() {
        return new SchedulingExecutionVerticesBuilder();
    }

    public TestingSchedulingExecutionVertex newExecutionVertex() {
        return newExecutionVertex(new JobVertexID(), 0);
    }

    public TestingSchedulingExecutionVertex newExecutionVertex(ExecutionState executionState) {
        final TestingSchedulingExecutionVertex newVertex =
                TestingSchedulingExecutionVertex.newBuilder()
                        .withExecutionState(executionState)
                        .build();
        addSchedulingExecutionVertex(newVertex);
        return newVertex;
    }

    public TestingSchedulingExecutionVertex newExecutionVertex(
            final JobVertexID jobVertexId, final int subtaskIndex) {
        final TestingSchedulingExecutionVertex newVertex =
                TestingSchedulingExecutionVertex.withExecutionVertexID(jobVertexId, subtaskIndex);
        addSchedulingExecutionVertex(newVertex);
        return newVertex;
    }

    public TestingSchedulingTopology connect(
            final TestingSchedulingExecutionVertex producer,
            final TestingSchedulingExecutionVertex consumer) {

        return connect(producer, consumer, ResultPartitionType.PIPELINED);
    }

    public TestingSchedulingTopology connect(
            TestingSchedulingExecutionVertex producer,
            TestingSchedulingExecutionVertex consumer,
            ResultPartitionType resultPartitionType) {

        connectConsumersToProducers(
                Collections.singletonList(consumer),
                Collections.singletonList(producer),
                new IntermediateDataSetID(),
                resultPartitionType,
                ResultPartitionState.ALL_DATA_PRODUCED);

        updateVertexResultPartitions(producer);
        updateVertexResultPartitions(consumer);

        resetPipelinedRegions();

        return this;
    }

    public ProducerConsumerConnectionBuilder connectPointwise(
            final List<TestingSchedulingExecutionVertex> producers,
            final List<TestingSchedulingExecutionVertex> consumers) {

        return new ProducerConsumerPointwiseConnectionBuilder(producers, consumers);
    }

    public ProducerConsumerConnectionBuilder connectAllToAll(
            final List<TestingSchedulingExecutionVertex> producers,
            final List<TestingSchedulingExecutionVertex> consumers) {

        return new ProducerConsumerAllToAllConnectionBuilder(producers, consumers);
    }

    private static List<TestingSchedulingResultPartition> connectConsumersToProducers(
            final List<TestingSchedulingExecutionVertex> consumers,
            final List<TestingSchedulingExecutionVertex> producers,
            final IntermediateDataSetID intermediateDataSetId,
            final ResultPartitionType resultPartitionType,
            final ResultPartitionState resultPartitionState) {

        final List<TestingSchedulingResultPartition> resultPartitions = new ArrayList<>();

        final ConnectionResult connectionResult =
                connectConsumersToProducersById(
                        consumers.stream()
                                .map(SchedulingExecutionVertex::getId)
                                .collect(Collectors.toList()),
                        producers.stream()
                                .map(SchedulingExecutionVertex::getId)
                                .collect(Collectors.toList()),
                        intermediateDataSetId,
                        resultPartitionType);

        final ConsumedPartitionGroup consumedPartitionGroup =
                connectionResult.getConsumedPartitionGroup();
        final ConsumerVertexGroup consumerVertexGroup = connectionResult.getConsumerVertexGroup();

        final TestingSchedulingResultPartition.Builder resultPartitionBuilder =
                new TestingSchedulingResultPartition.Builder()
                        .withIntermediateDataSetID(intermediateDataSetId)
                        .withResultPartitionType(resultPartitionType)
                        .withResultPartitionState(resultPartitionState);

        for (int i = 0; i < producers.size(); i++) {
            final TestingSchedulingExecutionVertex producer = producers.get(i);
            final IntermediateResultPartitionID partitionId =
                    connectionResult.getResultPartitions().get(i);
            final TestingSchedulingResultPartition resultPartition =
                    resultPartitionBuilder
                            .withPartitionNum(partitionId.getPartitionNumber())
                            .build();

            producer.addProducedPartition(resultPartition);

            resultPartition.setProducer(producer);
            resultPartitions.add(resultPartition);
            resultPartition.registerConsumedPartitionGroup(consumedPartitionGroup);
            resultPartition.addConsumerGroup(consumerVertexGroup);

            if (resultPartition.getState() == ResultPartitionState.ALL_DATA_PRODUCED) {
                consumedPartitionGroup.partitionFinished();
            }
        }

        final Map<IntermediateResultPartitionID, TestingSchedulingResultPartition>
                consumedPartitionById =
                        resultPartitions.stream()
                                .collect(
                                        Collectors.toMap(
                                                TestingSchedulingResultPartition::getId,
                                                Function.identity()));
        for (TestingSchedulingExecutionVertex consumer : consumers) {
            consumer.addConsumedPartitionGroup(consumedPartitionGroup, consumedPartitionById);
        }

        return resultPartitions;
    }

    public static ConnectionResult connectConsumersToProducersById(
            final List<ExecutionVertexID> consumers,
            final List<ExecutionVertexID> producers,
            final IntermediateDataSetID intermediateDataSetId,
            final ResultPartitionType resultPartitionType) {

        final List<IntermediateResultPartitionID> resultPartitions = new ArrayList<>();
        for (ExecutionVertexID producer : producers) {
            final IntermediateResultPartitionID resultPartition =
                    new IntermediateResultPartitionID(
                            intermediateDataSetId, producer.getSubtaskIndex());
            resultPartitions.add(resultPartition);
        }

        final ConsumedPartitionGroup consumedPartitionGroup =
                createConsumedPartitionGroup(
                        consumers.size(), resultPartitions, resultPartitionType);
        final ConsumerVertexGroup consumerVertexGroup =
                createConsumerVertexGroup(consumers, resultPartitionType);

        consumedPartitionGroup.setConsumerVertexGroup(consumerVertexGroup);
        consumerVertexGroup.setConsumedPartitionGroup(consumedPartitionGroup);

        return new ConnectionResult(resultPartitions, consumedPartitionGroup, consumerVertexGroup);
    }

    private static ConsumedPartitionGroup createConsumedPartitionGroup(
            final int numConsumers,
            final List<IntermediateResultPartitionID> consumedPartitions,
            final ResultPartitionType resultPartitionType) {
        return ConsumedPartitionGroup.fromMultiplePartitions(
                numConsumers, consumedPartitions, resultPartitionType);
    }

    private static ConsumerVertexGroup createConsumerVertexGroup(
            final List<ExecutionVertexID> consumers,
            final ResultPartitionType resultPartitionType) {
        return ConsumerVertexGroup.fromMultipleVertices(consumers, resultPartitionType);
    }

    /**
     * The result of connecting a set of consumers to their producers, including the created result
     * partitions and the consumption groups.
     */
    public static class ConnectionResult {
        private final List<IntermediateResultPartitionID> resultPartitions;
        private final ConsumedPartitionGroup consumedPartitionGroup;
        private final ConsumerVertexGroup consumerVertexGroup;

        public ConnectionResult(
                final List<IntermediateResultPartitionID> resultPartitions,
                final ConsumedPartitionGroup consumedPartitionGroup,
                final ConsumerVertexGroup consumerVertexGroup) {
            this.resultPartitions = checkNotNull(resultPartitions);
            this.consumedPartitionGroup = checkNotNull(consumedPartitionGroup);
            this.consumerVertexGroup = checkNotNull(consumerVertexGroup);
        }

        public List<IntermediateResultPartitionID> getResultPartitions() {
            return resultPartitions;
        }

        public ConsumedPartitionGroup getConsumedPartitionGroup() {
            return consumedPartitionGroup;
        }

        public ConsumerVertexGroup getConsumerVertexGroup() {
            return consumerVertexGroup;
        }
    }

    /** Builder for {@link TestingSchedulingResultPartition}. */
    public abstract class ProducerConsumerConnectionBuilder {

        protected final List<TestingSchedulingExecutionVertex> producers;

        protected final List<TestingSchedulingExecutionVertex> consumers;

        protected ResultPartitionType resultPartitionType = ResultPartitionType.BLOCKING;

        protected ResultPartitionState resultPartitionState =
                ResultPartitionState.ALL_DATA_PRODUCED;

        protected ProducerConsumerConnectionBuilder(
                final List<TestingSchedulingExecutionVertex> producers,
                final List<TestingSchedulingExecutionVertex> consumers) {
            this.producers = producers;
            this.consumers = consumers;
        }

        public ProducerConsumerConnectionBuilder withResultPartitionType(
                final ResultPartitionType resultPartitionType) {
            this.resultPartitionType = resultPartitionType;
            return this;
        }

        public ProducerConsumerConnectionBuilder withResultPartitionState(
                final ResultPartitionState state) {
            this.resultPartitionState = state;
            return this;
        }

        public List<TestingSchedulingResultPartition> finish() {
            final List<TestingSchedulingResultPartition> resultPartitions = connect();

            producers.stream()
                    .forEach(TestingSchedulingTopology.this::updateVertexResultPartitions);
            consumers.stream()
                    .forEach(TestingSchedulingTopology.this::updateVertexResultPartitions);

            return resultPartitions;
        }

        protected abstract List<TestingSchedulingResultPartition> connect();
    }

    /**
     * Builder for {@link TestingSchedulingResultPartition} of {@link
     * DistributionPattern#POINTWISE}.
     */
    private class ProducerConsumerPointwiseConnectionBuilder
            extends ProducerConsumerConnectionBuilder {

        private ProducerConsumerPointwiseConnectionBuilder(
                final List<TestingSchedulingExecutionVertex> producers,
                final List<TestingSchedulingExecutionVertex> consumers) {
            super(producers, consumers);
            // currently we only support one to one
            checkState(producers.size() == consumers.size());
        }

        @Override
        protected List<TestingSchedulingResultPartition> connect() {
            final List<TestingSchedulingResultPartition> resultPartitions = new ArrayList<>();
            final IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();
            for (int idx = 0; idx < producers.size(); idx++) {
                resultPartitions.addAll(
                        connectConsumersToProducers(
                                Collections.singletonList(consumers.get(idx)),
                                Collections.singletonList(producers.get(idx)),
                                intermediateDataSetId,
                                resultPartitionType,
                                resultPartitionState));
            }
            return resultPartitions;
        }
    }

    /**
     * Builder for {@link TestingSchedulingResultPartition} of {@link
     * DistributionPattern#ALL_TO_ALL}.
     */
    private class ProducerConsumerAllToAllConnectionBuilder
            extends ProducerConsumerConnectionBuilder {

        private ProducerConsumerAllToAllConnectionBuilder(
                final List<TestingSchedulingExecutionVertex> producers,
                final List<TestingSchedulingExecutionVertex> consumers) {
            super(producers, consumers);
        }

        @Override
        protected List<TestingSchedulingResultPartition> connect() {
            return connectConsumersToProducers(
                    consumers,
                    producers,
                    new IntermediateDataSetID(),
                    resultPartitionType,
                    resultPartitionState);
        }
    }

    /** Builder for {@link TestingSchedulingExecutionVertex}. */
    public class SchedulingExecutionVerticesBuilder {

        private JobVertexID jobVertexId = new JobVertexID();

        private int parallelism = 1;

        public SchedulingExecutionVerticesBuilder withParallelism(final int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public SchedulingExecutionVerticesBuilder withJobVertexID(final JobVertexID jobVertexId) {
            this.jobVertexId = jobVertexId;
            return this;
        }

        public List<TestingSchedulingExecutionVertex> finish() {
            final List<TestingSchedulingExecutionVertex> vertices = new ArrayList<>();
            for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
                vertices.add(createTestingSchedulingExecutionVertex(subtaskIndex));
            }

            TestingSchedulingTopology.this.addSchedulingExecutionVertices(vertices);

            return vertices;
        }

        private TestingSchedulingExecutionVertex createTestingSchedulingExecutionVertex(
                final int subtaskIndex) {
            return TestingSchedulingExecutionVertex.newBuilder()
                    .withExecutionVertexID(jobVertexId, subtaskIndex)
                    .build();
        }
    }
}
