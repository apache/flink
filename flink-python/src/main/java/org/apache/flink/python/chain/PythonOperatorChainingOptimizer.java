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

package org.apache.flink.python.chain;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.util.PythonConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.AbstractDataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.AbstractOneInputPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.PythonCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonKeyedProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonProcessOperator;
import org.apache.flink.streaming.api.transformations.AbstractBroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.ReduceTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.apache.commons.compress.utils.Lists;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A util class which attempts to chain all available Python functions.
 *
 * <p>An operator could be chained to it's predecessor if all of the following conditions are met:
 *
 * <ul>
 *   <li>Both of them are Python operators
 *   <li>The parallelism, the maximum parallelism and the slot sharing group are all the same
 *   <li>The chaining strategy is ChainingStrategy.ALWAYS and the chaining strategy of the
 *       predecessor isn't ChainingStrategy.NEVER
 *   <li>This partitioner between them is ForwardPartitioner
 * </ul>
 *
 * <p>The properties of the generated chained operator are as following:
 *
 * <ul>
 *   <li>The name is the concatenation of all the names of the chained operators
 *   <li>The parallelism, the maximum parallelism and the slot sharing group are from one of the
 *       chained operators as all of them are the same between the chained operators
 *   <li>The chaining strategy is the same as the head operator
 *   <li>The uid and the uidHash are the same as the head operator
 * </ul>
 */
public class PythonOperatorChainingOptimizer {

    private final List<Transformation<?>> transformations;

    private final boolean isChainingEnabled;

    // key-value pairs where the value is the output transformations of the key transformation
    private final transient Map<Transformation<?>, Set<Transformation<?>>> outputMap =
            new HashMap<>();

    public PythonOperatorChainingOptimizer(
            List<Transformation<?>> transformations, boolean isChainingEnabled) {
        this.transformations = checkNotNull(transformations);
        this.isChainingEnabled = isChainingEnabled;
        buildOutputMap();
    }

    private void buildOutputMap() {
        for (Transformation<?> transformation : transformations) {
            for (Transformation<?> input : transformation.getInputs()) {
                Set<Transformation<?>> outputs =
                        outputMap.computeIfAbsent(input, i -> Sets.newHashSet());
                outputs.add(transformation);
            }
        }
    }

    @VisibleForTesting
    public List<Transformation<?>> optimize() throws Exception {
        List<Transformation<?>> optimizedTransformations = Lists.newArrayList();
        Set<Transformation<?>> removedTransformations = Sets.newIdentityHashSet();
        for (Transformation<?> transformation : transformations) {
            if (!removedTransformations.contains(transformation)) {
                ChainInfo chainInfo = chainWithInputIfPossible(transformation);
                optimizedTransformations.add(chainInfo.newTransformation);
                optimizedTransformations.removeAll(chainInfo.oldTransformations);
                removedTransformations.addAll(chainInfo.oldTransformations);
            }
        }
        return optimizedTransformations;
    }

    private ChainInfo chainWithInputIfPossible(Transformation<?> transform) throws Exception {
        ChainInfo chainInfo = null;
        if (transform instanceof OneInputTransformation
                && PythonConfigUtil.isPythonDataStreamOperator(transform)) {
            Transformation<?> input = transform.getInputs().get(0);
            while (!PythonConfigUtil.isPythonDataStreamOperator(input)) {
                if (input instanceof PartitionTransformation
                        && ((PartitionTransformation<?>) input).getPartitioner()
                                instanceof ForwardPartitioner) {
                    input = input.getInputs().get(0);
                } else {
                    return ChainInfo.of(transform);
                }
            }

            if (isChainable(input, transform)) {
                Transformation<?> chainedTransformation =
                        createChainedTransformation(input, transform);
                Set<Transformation<?>> outputTransformations = outputMap.get(transform);
                if (outputTransformations != null) {
                    for (Transformation<?> output : outputTransformations) {
                        replaceInput(output, transform, chainedTransformation);
                    }
                }
                chainInfo = ChainInfo.of(chainedTransformation, Arrays.asList(input, transform));
            }
        }

        if (chainInfo == null) {
            chainInfo = ChainInfo.of(transform);
        }

        return chainInfo;
    }

    private Transformation<?> createChainedTransformation(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        AbstractDataStreamPythonFunctionOperator<?> upOperator =
                (AbstractDataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(upTransform)).getOperator();
        PythonProcessOperator<?, ?> downOperator =
                (PythonProcessOperator<?, ?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(downTransform))
                                .getOperator();

        DataStreamPythonFunctionInfo upPythonFunctionInfo = upOperator.getPythonFunctionInfo();
        DataStreamPythonFunctionInfo downPythonFunctionInfo = downOperator.getPythonFunctionInfo();

        DataStreamPythonFunctionInfo chainedPythonFunctionInfo =
                new DataStreamPythonFunctionInfo(
                        downPythonFunctionInfo.getPythonFunction(),
                        upPythonFunctionInfo,
                        downPythonFunctionInfo.getFunctionType());

        AbstractDataStreamPythonFunctionOperator<?> chainedOperator =
                upOperator.copy(chainedPythonFunctionInfo, downOperator.getProducedType());

        PhysicalTransformation<?> chainedTransformation;
        if (upOperator instanceof AbstractOneInputPythonFunctionOperator) {
            chainedTransformation =
                    new OneInputTransformation(
                            upTransform.getInputs().get(0),
                            upTransform.getName() + ", " + downTransform.getName(),
                            (OneInputStreamOperator<?, ?>) chainedOperator,
                            downTransform.getOutputType(),
                            upTransform.getParallelism());

            ((OneInputTransformation<?, ?>) chainedTransformation)
                    .setStateKeySelector(
                            ((OneInputTransformation) upTransform).getStateKeySelector());
            ((OneInputTransformation<?, ?>) chainedTransformation)
                    .setStateKeyType(
                            ((OneInputTransformation<?, ?>) upTransform).getStateKeyType());
        } else {
            chainedTransformation =
                    new TwoInputTransformation(
                            upTransform.getInputs().get(0),
                            upTransform.getInputs().get(1),
                            upTransform.getName() + ", " + downTransform.getName(),
                            (TwoInputStreamOperator<?, ?, ?>) chainedOperator,
                            downTransform.getOutputType(),
                            upTransform.getParallelism());

            ((TwoInputTransformation<?, ?, ?>) chainedTransformation)
                    .setStateKeySelectors(
                            ((TwoInputTransformation) upTransform).getStateKeySelector1(),
                            ((TwoInputTransformation) upTransform).getStateKeySelector2());
            ((TwoInputTransformation<?, ?, ?>) chainedTransformation)
                    .setStateKeyType(
                            ((TwoInputTransformation<?, ?, ?>) upTransform).getStateKeyType());
        }

        chainedTransformation.setUid(upTransform.getUid());
        if (upTransform.getUserProvidedNodeHash() != null) {
            chainedTransformation.setUidHash(upTransform.getUserProvidedNodeHash());
        }

        for (ManagedMemoryUseCase useCase : upTransform.getManagedMemorySlotScopeUseCases()) {
            chainedTransformation.declareManagedMemoryUseCaseAtSlotScope(useCase);
        }
        for (ManagedMemoryUseCase useCase : downTransform.getManagedMemorySlotScopeUseCases()) {
            chainedTransformation.declareManagedMemoryUseCaseAtSlotScope(useCase);
        }

        for (Map.Entry<ManagedMemoryUseCase, Integer> useCase :
                upTransform.getManagedMemoryOperatorScopeUseCaseWeights().entrySet()) {
            chainedTransformation.declareManagedMemoryUseCaseAtOperatorScope(
                    useCase.getKey(), useCase.getValue());
        }
        for (Map.Entry<ManagedMemoryUseCase, Integer> useCase :
                downTransform.getManagedMemoryOperatorScopeUseCaseWeights().entrySet()) {
            chainedTransformation.declareManagedMemoryUseCaseAtOperatorScope(
                    useCase.getKey(),
                    useCase.getValue()
                            + chainedTransformation
                                    .getManagedMemoryOperatorScopeUseCaseWeights()
                                    .getOrDefault(useCase.getKey(), 0));
        }

        chainedTransformation.setBufferTimeout(
                Math.min(upTransform.getBufferTimeout(), downTransform.getBufferTimeout()));

        if (upTransform.getMaxParallelism() > 0) {
            chainedTransformation.setMaxParallelism(upTransform.getMaxParallelism());
        }
        chainedTransformation.setChainingStrategy(
                getOperatorFactory(upTransform).getChainingStrategy());
        chainedTransformation.setCoLocationGroupKey(upTransform.getCoLocationGroupKey());
        chainedTransformation.setResources(
                upTransform.getMinResources().merge(downTransform.getMinResources()),
                upTransform.getPreferredResources().merge(downTransform.getPreferredResources()));

        if (upTransform.getSlotSharingGroup().isPresent()) {
            chainedTransformation.setSlotSharingGroup(upTransform.getSlotSharingGroup().get());
        }
        return chainedTransformation;
    }

    private boolean isChainable(Transformation<?> upTransform, Transformation<?> downTransform) {
        return upTransform.getParallelism() == downTransform.getParallelism()
                && upTransform.getMaxParallelism() == downTransform.getMaxParallelism()
                && upTransform.getSlotSharingGroup().equals(downTransform.getSlotSharingGroup())
                && areOperatorsChainable(upTransform, downTransform)
                && isChainingEnabled;
    }

    // ----------------------- Utility Methods -----------------------

    public static void apply(StreamExecutionEnvironment env) throws Exception {
        Field transformationsField =
                StreamExecutionEnvironment.class.getDeclaredField("transformations");
        transformationsField.setAccessible(true);
        List<Transformation<?>> transformations =
                (List<Transformation<?>>) transformationsField.get(env);
        PythonOperatorChainingOptimizer chainingOptimizer =
                new PythonOperatorChainingOptimizer(
                        transformations,
                        env.getConfiguration().get(PythonOptions.PYTHON_OPERATOR_CHAINING_ENABLED));
        transformationsField.set(env, chainingOptimizer.optimize());
    }

    private static boolean areOperatorsChainable(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        if (!areOperatorsChainableByChainingStrategy(upTransform, downTransform)) {
            return false;
        }

        AbstractDataStreamPythonFunctionOperator<?> upOperator =
                (AbstractDataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(upTransform)).getOperator();
        AbstractDataStreamPythonFunctionOperator<?> downOperator =
                (AbstractDataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(downTransform))
                                .getOperator();

        return downOperator instanceof PythonProcessOperator
                && (upOperator instanceof PythonKeyedProcessOperator
                        || upOperator instanceof PythonKeyedCoProcessOperator
                        || upOperator instanceof PythonProcessOperator
                        || upOperator instanceof PythonCoProcessOperator);
    }

    private static boolean areOperatorsChainableByChainingStrategy(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;

        StreamOperatorFactory<?> upStreamOperator = getOperatorFactory(upTransform);
        StreamOperatorFactory<?> downStreamOperator = getOperatorFactory(downTransform);

        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        return isChainable;
    }

    private static StreamOperatorFactory<?> getOperatorFactory(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            return ((OneInputTransformation<?, ?>) transform).getOperatorFactory();
        } else if (transform instanceof TwoInputTransformation) {
            return ((TwoInputTransformation<?, ?, ?>) transform).getOperatorFactory();
        } else if (transform instanceof AbstractMultipleInputTransformation) {
            return ((AbstractMultipleInputTransformation<?>) transform).getOperatorFactory();
        } else {
            return null;
        }
    }

    private static void replaceInput(
            Transformation<?> transformation,
            Transformation<?> oldInput,
            Transformation<?> newInput)
            throws Exception {
        if (transformation instanceof OneInputTransformation
                || transformation instanceof FeedbackTransformation
                || transformation instanceof SideOutputTransformation
                || transformation instanceof ReduceTransformation
                || transformation instanceof SinkTransformation
                || transformation instanceof LegacySinkTransformation
                || transformation instanceof TimestampsAndWatermarksTransformation) {
            Field inputField = transformation.getClass().getDeclaredField("input");
            inputField.setAccessible(true);
            inputField.set(transformation, newInput);
        } else if (transformation instanceof TwoInputTransformation) {
            Field inputField;
            if (((TwoInputTransformation<?, ?, ?>) transformation).getInput1() == oldInput) {
                inputField = transformation.getClass().getDeclaredField("input1");
            } else {
                inputField = transformation.getClass().getDeclaredField("input2");
            }
            inputField.setAccessible(true);
            inputField.set(transformation, newInput);
        } else if (transformation instanceof UnionTransformation
                || transformation instanceof AbstractMultipleInputTransformation) {
            Field inputsField = transformation.getClass().getDeclaredField("inputs");
            inputsField.setAccessible(true);
            List<Transformation<?>> newInputs = Lists.newArrayList();
            newInputs.addAll(transformation.getInputs());
            newInputs.remove(oldInput);
            newInputs.add(newInput);
            inputsField.set(transformation, newInputs);
        } else if (transformation instanceof AbstractBroadcastStateTransformation) {
            Field inputField;
            if (((AbstractBroadcastStateTransformation<?, ?, ?>) transformation).getRegularInput()
                    == oldInput) {
                inputField = transformation.getClass().getDeclaredField("regularInput");
            } else {
                inputField = transformation.getClass().getDeclaredField("broadcastInput");
            }
            inputField.setAccessible(true);
            inputField.set(transformation, newInput);
        } else {
            throw new RuntimeException("Unsupported transformation: " + transformation);
        }
    }

    // ----------------------- Utility Classes -----------------------

    private static class ChainInfo {
        /** The transformation which represents the chaining of the {@link #oldTransformations}. */
        public final Transformation<?> newTransformation;

        /** The transformations which will be chained together. */
        public final Collection<Transformation<?>> oldTransformations;

        private ChainInfo(
                Transformation<?> newTransformation,
                Collection<Transformation<?>> oldTransformations) {
            this.newTransformation = newTransformation;
            this.oldTransformations = oldTransformations;
        }

        /** No chaining happens. */
        public static ChainInfo of(Transformation<?> newTransformation) {
            return new ChainInfo(newTransformation, Collections.emptyList());
        }

        public static ChainInfo of(
                Transformation<?> newTransformation,
                Collection<Transformation<?>> oldTransformations) {
            return new ChainInfo(newTransformation, oldTransformations);
        }
    }
}
