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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class to build StreamConfig for chain of operators. */
public class StreamConfigChainer<OWNER> {
    public static final int MAIN_NODE_ID = 0;
    private final OWNER owner;
    private final StreamConfig headConfig;
    private final Map<Integer, StreamConfig> chainedConfigs = new HashMap<>();
    private final int numberOfNonChainedOutputs;
    private int bufferTimeout;

    private StreamConfig tailConfig;
    private int chainIndex = MAIN_NODE_ID;

    private final List<List<NonChainedOutput>> outEdgesInOrder = new LinkedList<>();

    private boolean setTailNonChainedOutputs = true;

    StreamConfigChainer(
            OperatorID headOperatorID,
            StreamConfig headConfig,
            OWNER owner,
            int numberOfNonChainedOutputs) {
        this.owner = checkNotNull(owner);
        this.headConfig = checkNotNull(headConfig);
        this.tailConfig = checkNotNull(headConfig);
        this.numberOfNonChainedOutputs = numberOfNonChainedOutputs;

        head(headOperatorID);
    }

    private void head(OperatorID headOperatorID) {
        headConfig.setOperatorID(headOperatorID);
        headConfig.setChainStart();
        headConfig.setChainIndex(chainIndex);
    }

    /**
     * @deprecated Use {@link #chain(TypeSerializer)} or {@link #chain(TypeSerializer,
     *     TypeSerializer)} instead.
     */
    @Deprecated
    public <T> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperator<T, T> operator,
            TypeSerializer<T> typeSerializer,
            boolean createKeyedStateBackend) {
        return chain(operatorID, operator, typeSerializer, typeSerializer, createKeyedStateBackend);
    }

    /**
     * @deprecated Use {@link #chain(TypeSerializer)} or {@link #chain(TypeSerializer,
     *     TypeSerializer)} instead.
     */
    @Deprecated
    public <T> StreamConfigChainer<OWNER> chain(
            OneInputStreamOperator<T, T> operator, TypeSerializer<T> typeSerializer) {
        return chain(new OperatorID(), operator, typeSerializer);
    }

    /**
     * @deprecated Use {@link #chain(TypeSerializer)} or {@link #chain(TypeSerializer,
     *     TypeSerializer)} instead.
     */
    @Deprecated
    public <T> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperator<T, T> operator,
            TypeSerializer<T> typeSerializer) {
        return chain(operatorID, operator, typeSerializer, typeSerializer, false);
    }

    /**
     * @deprecated Use {@link #chain(TypeSerializer)} or {@link #chain(TypeSerializer,
     *     TypeSerializer)} instead.
     */
    @Deprecated
    public <T> StreamConfigChainer<OWNER> chain(
            OneInputStreamOperatorFactory<T, T> operatorFactory, TypeSerializer<T> typeSerializer) {
        return chain(new OperatorID(), operatorFactory, typeSerializer);
    }

    /**
     * @deprecated Use {@link #chain(TypeSerializer)} or {@link #chain(TypeSerializer,
     *     TypeSerializer)} instead.
     */
    @Deprecated
    public <T> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperatorFactory<T, T> operatorFactory,
            TypeSerializer<T> typeSerializer) {
        return chain(operatorID, operatorFactory, typeSerializer, typeSerializer, false);
    }

    /**
     * @deprecated Use {@link #chain(TypeSerializer)} or {@link #chain(TypeSerializer,
     *     TypeSerializer)} instead.
     */
    @Deprecated
    private <IN, OUT> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperator<IN, OUT> operator,
            TypeSerializer<IN> inputSerializer,
            TypeSerializer<OUT> outputSerializer,
            boolean createKeyedStateBackend) {
        return chain(
                operatorID,
                SimpleOperatorFactory.of(operator),
                inputSerializer,
                outputSerializer,
                createKeyedStateBackend);
    }

    /**
     * @deprecated Use {@link #chain(TypeSerializer)} or {@link #chain(TypeSerializer,
     *     TypeSerializer)} instead.
     */
    @Deprecated
    public <IN, OUT> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeSerializer<IN> inputSerializer,
            TypeSerializer<OUT> outputSerializer,
            boolean createKeyedStateBackend) {

        chainIndex++;

        StreamEdge streamEdge =
                new StreamEdge(
                        new StreamNode(
                                tailConfig.getChainIndex(),
                                null,
                                null,
                                (StreamOperator<?>) null,
                                null,
                                null),
                        new StreamNode(
                                chainIndex, null, null, (StreamOperator<?>) null, null, null),
                        0,
                        null,
                        null);
        streamEdge.setBufferTimeout(bufferTimeout);
        tailConfig.setChainedOutputs(Collections.singletonList(streamEdge));
        tailConfig = new StreamConfig(new Configuration());
        tailConfig.setStreamOperatorFactory(checkNotNull(operatorFactory));
        tailConfig.setOperatorID(checkNotNull(operatorID));
        tailConfig.setupNetworkInputs(inputSerializer);
        tailConfig.setTypeSerializerOut(outputSerializer);
        if (createKeyedStateBackend) {
            // used to test multiple stateful operators chained in a single task.
            tailConfig.setStateKeySerializer(inputSerializer);
            tailConfig.setStateBackendUsesManagedMemory(true);
            tailConfig.setManagedMemoryFractionOperatorOfUseCase(
                    ManagedMemoryUseCase.STATE_BACKEND, 1.0);
        }
        tailConfig.setChainIndex(chainIndex);
        tailConfig.serializeAllConfigs();

        chainedConfigs.put(chainIndex, tailConfig);

        return this;
    }

    public <T> StreamConfigEdgeChainer<OWNER, T, T> chain(TypeSerializer<T> typeSerializer) {
        return chain(typeSerializer, typeSerializer);
    }

    public <IN, OUT> StreamConfigEdgeChainer<OWNER, IN, OUT> chain(
            TypeSerializer<IN> inputSerializer, TypeSerializer<OUT> outputSerializer) {
        return new StreamConfigEdgeChainer<>(this, inputSerializer, outputSerializer);
    }

    public OWNER finish() {
        if (setTailNonChainedOutputs) {
            List<NonChainedOutput> nonChainedOutputs = new ArrayList<>();
            for (int i = 0; i < numberOfNonChainedOutputs; ++i) {
                NonChainedOutput streamOutput =
                        new NonChainedOutput(
                                true,
                                chainIndex,
                                1,
                                1,
                                100,
                                false,
                                new IntermediateDataSetID(),
                                null,
                                new BroadcastPartitioner<>(),
                                ResultPartitionType.PIPELINED_BOUNDED);
                nonChainedOutputs.add(streamOutput);
            }
            outEdgesInOrder.add(nonChainedOutputs);
            tailConfig.setNumberOfOutputs(numberOfNonChainedOutputs);
            tailConfig.setVertexNonChainedOutputs(nonChainedOutputs);
            tailConfig.setOperatorNonChainedOutputs(nonChainedOutputs);
        }

        Collections.reverse(outEdgesInOrder);
        List<NonChainedOutput> allOutEdgesInOrder =
                outEdgesInOrder.stream().flatMap(List::stream).collect(Collectors.toList());

        tailConfig.setChainEnd();
        chainedConfigs.values().forEach(StreamConfig::serializeAllConfigs);
        headConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfigs);
        headConfig.setVertexNonChainedOutputs(allOutEdgesInOrder);
        headConfig.serializeAllConfigs();

        return owner;
    }

    public <OUT> OWNER finishForSingletonOperatorChain(TypeSerializer<OUT> outputSerializer) {
        return finishForSingletonOperatorChain(outputSerializer, new BroadcastPartitioner<>());
    }

    public <OUT> OWNER finishForSingletonOperatorChain(
            TypeSerializer<OUT> outputSerializer, StreamPartitioner<?> partitioner) {

        checkState(chainIndex == 0, "Use finishForSingletonOperatorChain");
        checkState(headConfig == tailConfig);
        StreamOperator<OUT> dummyOperator =
                new AbstractStreamOperator<OUT>() {
                    private static final long serialVersionUID = 1L;
                };
        List<NonChainedOutput> streamOutputs = new LinkedList<>();

        StreamNode sourceVertexDummy =
                new StreamNode(
                        MAIN_NODE_ID,
                        "group",
                        null,
                        dummyOperator,
                        "source dummy",
                        SourceStreamTask.class);
        for (int i = 0; i < numberOfNonChainedOutputs; ++i) {
            streamOutputs.add(
                    new NonChainedOutput(
                            true,
                            sourceVertexDummy.getId(),
                            1,
                            1,
                            100,
                            false,
                            new IntermediateDataSetID(),
                            null,
                            partitioner,
                            ResultPartitionType.PIPELINED_BOUNDED));
        }

        headConfig.setVertexID(0);
        headConfig.setNumberOfOutputs(1);
        headConfig.setVertexNonChainedOutputs(streamOutputs);
        headConfig.setOperatorNonChainedOutputs(streamOutputs);
        chainedConfigs.values().forEach(StreamConfig::serializeAllConfigs);
        headConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfigs);
        headConfig.setVertexNonChainedOutputs(streamOutputs);
        headConfig.setTypeSerializerOut(outputSerializer);
        headConfig.serializeAllConfigs();

        return owner;
    }

    public StreamConfigChainer<OWNER> name(String name) {
        tailConfig.setOperatorName(name);
        return this;
    }

    public void setBufferTimeout(int bufferTimeout) {
        this.bufferTimeout = bufferTimeout;
    }

    /** Helper class to build operator node. */
    public static class StreamConfigEdgeChainer<OWNER, IN, OUT> {
        private final OutputTag<Void> placeHolderTag;
        private StreamConfigChainer<OWNER> parent;
        private OperatorID operatorID;

        private final TypeSerializer<IN> inputSerializer;
        private final TypeSerializer<OUT> outputSerializer;

        private StreamOperatorFactory<OUT> operatorFactory;

        private Map<OutputTag<?>, Integer> nonChainedOutPuts;
        private boolean createKeyedStateBackend;

        private StreamConfigEdgeChainer(
                StreamConfigChainer<OWNER> parent,
                TypeSerializer<IN> inputSerializer,
                TypeSerializer<OUT> outputSerializer) {
            this.parent = parent;
            this.parent.setTailNonChainedOutputs = true;

            this.inputSerializer = inputSerializer;
            this.outputSerializer = outputSerializer;
            this.placeHolderTag =
                    new OutputTag<>("FLINK_PLACEHOLDER", BasicTypeInfo.VOID_TYPE_INFO);
            this.nonChainedOutPuts = new HashMap<>(4);
        }

        public StreamConfigEdgeChainer<OWNER, IN, OUT> setOperatorID(OperatorID operatorID) {
            this.operatorID = operatorID;
            return this;
        }

        public StreamConfigEdgeChainer<OWNER, IN, OUT> setOperatorFactory(
                StreamOperatorFactory operatorFactory) {
            this.operatorFactory = operatorFactory;
            return this;
        }

        public StreamConfigEdgeChainer<OWNER, IN, OUT> addNonChainedOutputsCount(
                int nonChainedOutputsCount) {
            return addNonChainedOutputsCount(placeHolderTag, nonChainedOutputsCount);
        }

        public StreamConfigEdgeChainer<OWNER, IN, OUT> addNonChainedOutputsCount(
                OutputTag<?> outputTag, int nonChainedOutputsCount) {
            checkArgument(nonChainedOutputsCount >= 0 && outputTag != null);
            this.nonChainedOutPuts.put(outputTag, nonChainedOutputsCount);
            return this;
        }

        public StreamConfigEdgeChainer<OWNER, IN, OUT> setCreateKeyedStateBackend(
                boolean createKeyedStateBackend) {
            this.createKeyedStateBackend = createKeyedStateBackend;
            return this;
        }

        public StreamConfigChainer<OWNER> build() {
            parent.chainIndex++;

            StreamEdge streamEdge =
                    new StreamEdge(
                            new StreamNode(
                                    parent.tailConfig.getChainIndex(),
                                    null,
                                    null,
                                    (StreamOperator<?>) null,
                                    null,
                                    null),
                            new StreamNode(
                                    parent.chainIndex,
                                    null,
                                    null,
                                    (StreamOperator<?>) null,
                                    null,
                                    null),
                            0,
                            null,
                            null);
            streamEdge.setBufferTimeout(parent.bufferTimeout);
            parent.tailConfig.setChainedOutputs(Collections.singletonList(streamEdge));
            parent.tailConfig = new StreamConfig(new Configuration());
            parent.tailConfig.setStreamOperatorFactory(checkNotNull(operatorFactory));
            parent.tailConfig.setOperatorID(operatorID == null ? new OperatorID() : operatorID);
            parent.tailConfig.setupNetworkInputs(inputSerializer);
            parent.tailConfig.setTypeSerializerOut(outputSerializer);
            if (createKeyedStateBackend) {
                // used to test multiple stateful operators chained in a single task.
                parent.tailConfig.setStateKeySerializer(inputSerializer);
                parent.tailConfig.setStateBackendUsesManagedMemory(true);
                parent.tailConfig.setManagedMemoryFractionOperatorOfUseCase(
                        ManagedMemoryUseCase.STATE_BACKEND, 1.0);
            }
            if (!nonChainedOutPuts.isEmpty()) {
                List<NonChainedOutput> nonChainedOutputs =
                        createNonChainedOutputs(nonChainedOutPuts, streamEdge);

                parent.tailConfig.setVertexNonChainedOutputs(nonChainedOutputs);
                parent.tailConfig.setOperatorNonChainedOutputs(nonChainedOutputs);
                parent.chainedConfigs.values().forEach(StreamConfig::serializeAllConfigs);
                parent.tailConfig.setNumberOfOutputs(nonChainedOutputs.size());
                parent.outEdgesInOrder.add(nonChainedOutputs);
                parent.setTailNonChainedOutputs = false;
            }
            parent.tailConfig.setChainIndex(parent.chainIndex);
            parent.tailConfig.serializeAllConfigs();

            parent.chainedConfigs.put(parent.chainIndex, parent.tailConfig);
            return parent;
        }

        private List<NonChainedOutput> createNonChainedOutputs(
                Map<OutputTag<?>, Integer> nonChainedOutputsCount, StreamEdge streamEdge) {
            List<NonChainedOutput> nonChainedOutputs = new ArrayList<>();
            nonChainedOutputsCount.forEach(
                    (outputTag, value) -> {
                        for (int i = 0; i < value; i++) {
                            nonChainedOutputs.add(
                                    new NonChainedOutput(
                                            true,
                                            streamEdge.getTargetId(),
                                            1,
                                            1,
                                            100,
                                            false,
                                            new IntermediateDataSetID(),
                                            placeHolderTag.equals(outputTag) ? null : outputTag,
                                            new BroadcastPartitioner<>(),
                                            ResultPartitionType.PIPELINED_BOUNDED));
                            if (!placeHolderTag.equals(outputTag)) {
                                parent.tailConfig.setTypeSerializerSideOut(
                                        outputTag, outputSerializer);
                            }
                        }
                    });
            return nonChainedOutputs;
        }
    }
}
