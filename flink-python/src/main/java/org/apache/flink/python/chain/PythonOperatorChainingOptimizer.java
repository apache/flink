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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.util.PythonConfigUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.DataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.embedded.AbstractEmbeddedDataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.embedded.EmbeddedPythonCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.embedded.EmbeddedPythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.embedded.EmbeddedPythonKeyedProcessOperator;
import org.apache.flink.streaming.api.operators.python.embedded.EmbeddedPythonProcessOperator;
import org.apache.flink.streaming.api.operators.python.embedded.EmbeddedPythonWindowOperator;
import org.apache.flink.streaming.api.operators.python.process.AbstractExternalDataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonKeyedProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonProcessOperator;
import org.apache.flink.streaming.api.transformations.AbstractBroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
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
import org.apache.flink.streaming.api.transformations.python.PythonBroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.python.PythonKeyedBroadcastStateTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava32.com.google.common.collect.Queues;
import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.python.util.PythonConfigUtil.getOperatorFactory;

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

    /**
     * Perform chaining optimization. It will iterate the transformations defined in the given
     * StreamExecutionEnvironment and update them with the chained transformations.
     */
    @SuppressWarnings("unchecked")
    public static void apply(StreamExecutionEnvironment env) throws Exception {
        if (env.getConfiguration().get(PythonOptions.PYTHON_OPERATOR_CHAINING_ENABLED)) {
            final Field transformationsField =
                    StreamExecutionEnvironment.class.getDeclaredField("transformations");
            transformationsField.setAccessible(true);
            final List<Transformation<?>> transformations =
                    (List<Transformation<?>>) transformationsField.get(env);
            transformationsField.set(env, optimize(transformations));
        }
    }

    /**
     * Perform chaining optimization. It will iterate the transformations defined in the given
     * StreamExecutionEnvironment and update them with the chained transformations. Besides, it will
     * return the transformation after chaining optimization for the given transformation.
     */
    @SuppressWarnings("unchecked")
    public static Transformation<?> apply(
            StreamExecutionEnvironment env, Transformation<?> transformation) throws Exception {
        if (env.getConfiguration().get(PythonOptions.PYTHON_OPERATOR_CHAINING_ENABLED)) {
            final Field transformationsField =
                    StreamExecutionEnvironment.class.getDeclaredField("transformations");
            transformationsField.setAccessible(true);
            final List<Transformation<?>> transformations =
                    (List<Transformation<?>>) transformationsField.get(env);
            final Tuple2<List<Transformation<?>>, Transformation<?>> resultTuple =
                    optimize(transformations, transformation);
            transformationsField.set(env, resultTuple.f0);
            return resultTuple.f1;
        } else {
            return transformation;
        }
    }

    /**
     * Perform chaining optimization. It will return the chained transformations for the given
     * transformation list.
     */
    public static List<Transformation<?>> optimize(List<Transformation<?>> transformations) {
        final Map<Transformation<?>, Set<Transformation<?>>> outputMap =
                buildOutputMap(transformations);

        final LinkedHashSet<Transformation<?>> chainedTransformations = new LinkedHashSet<>();
        final Set<Transformation<?>> alreadyTransformed = Sets.newIdentityHashSet();
        final Queue<Transformation<?>> toTransformQueue = Queues.newArrayDeque(transformations);
        while (!toTransformQueue.isEmpty()) {
            final Transformation<?> transformation = toTransformQueue.poll();
            if (!alreadyTransformed.contains(transformation)) {
                alreadyTransformed.add(transformation);

                final ChainInfo chainInfo = chainWithInputIfPossible(transformation, outputMap);
                chainedTransformations.add(chainInfo.newTransformation);
                chainedTransformations.removeAll(chainInfo.oldTransformations);
                alreadyTransformed.addAll(chainInfo.oldTransformations);

                // Add the chained transformation and its inputs to the to-optimize list
                toTransformQueue.add(chainInfo.newTransformation);
                toTransformQueue.addAll(chainInfo.newTransformation.getInputs());
            }
        }
        return new ArrayList<>(chainedTransformations);
    }

    /**
     * Perform chaining optimization. It will returns the chained transformations and the
     * transformation after chaining optimization for the given transformation.
     */
    public static Tuple2<List<Transformation<?>>, Transformation<?>> optimize(
            List<Transformation<?>> transformations, Transformation<?> targetTransformation) {
        final Map<Transformation<?>, Set<Transformation<?>>> outputMap =
                buildOutputMap(transformations);

        final LinkedHashSet<Transformation<?>> chainedTransformations = new LinkedHashSet<>();
        final Set<Transformation<?>> alreadyTransformed = Sets.newIdentityHashSet();
        final Queue<Transformation<?>> toTransformQueue = Queues.newArrayDeque();
        toTransformQueue.add(targetTransformation);
        while (!toTransformQueue.isEmpty()) {
            final Transformation<?> toTransform = toTransformQueue.poll();
            if (!alreadyTransformed.contains(toTransform)) {
                alreadyTransformed.add(toTransform);

                final ChainInfo chainInfo = chainWithInputIfPossible(toTransform, outputMap);
                chainedTransformations.add(chainInfo.newTransformation);
                chainedTransformations.removeAll(chainInfo.oldTransformations);
                alreadyTransformed.addAll(chainInfo.oldTransformations);

                // Add the chained transformation and its inputs to the to-optimize list
                toTransformQueue.add(chainInfo.newTransformation);
                toTransformQueue.addAll(chainInfo.newTransformation.getInputs());

                if (toTransform == targetTransformation) {
                    targetTransformation = chainInfo.newTransformation;
                }
            }
        }
        return Tuple2.of(new ArrayList<>(chainedTransformations), targetTransformation);
    }

    /**
     * Construct the key-value pairs where the value is the output transformations of the key
     * transformation.
     */
    private static Map<Transformation<?>, Set<Transformation<?>>> buildOutputMap(
            List<Transformation<?>> transformations) {
        final Map<Transformation<?>, Set<Transformation<?>>> outputMap = new HashMap<>();
        final Queue<Transformation<?>> toTransformQueue = Queues.newArrayDeque(transformations);
        final Set<Transformation<?>> alreadyTransformed = Sets.newIdentityHashSet();
        while (!toTransformQueue.isEmpty()) {
            Transformation<?> transformation = toTransformQueue.poll();
            if (!alreadyTransformed.contains(transformation)) {
                alreadyTransformed.add(transformation);
                for (Transformation<?> input : transformation.getInputs()) {
                    Set<Transformation<?>> outputs =
                            outputMap.computeIfAbsent(input, i -> Sets.newHashSet());
                    outputs.add(transformation);
                }
                toTransformQueue.addAll(transformation.getInputs());
            }
        }
        return outputMap;
    }

    private static ChainInfo chainWithInputIfPossible(
            Transformation<?> transform, Map<Transformation<?>, Set<Transformation<?>>> outputMap) {
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

            if (isChainable(input, transform, outputMap)) {
                Transformation<?> chainedTransformation =
                        createChainedTransformation(input, transform);
                Set<Transformation<?>> outputTransformations = outputMap.get(transform);
                if (outputTransformations != null) {
                    for (Transformation<?> output : outputTransformations) {
                        replaceInput(output, transform, chainedTransformation);
                    }
                    outputMap.put(chainedTransformation, outputTransformations);
                }
                chainInfo = ChainInfo.of(chainedTransformation, Arrays.asList(input, transform));
            }
        }

        if (chainInfo == null) {
            chainInfo = ChainInfo.of(transform);
        }

        return chainInfo;
    }

    @SuppressWarnings("unchecked")
    private static Transformation<?> createChainedTransformation(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        DataStreamPythonFunctionOperator<?> upOperator =
                (DataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(upTransform)).getOperator();

        DataStreamPythonFunctionOperator<?> downOperator =
                (DataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(downTransform))
                                .getOperator();

        assert arePythonOperatorsInSameExecutionEnvironment(upOperator, downOperator);

        final DataStreamPythonFunctionInfo upPythonFunctionInfo =
                upOperator.getPythonFunctionInfo().copy();

        final DataStreamPythonFunctionInfo downPythonFunctionInfo =
                downOperator.getPythonFunctionInfo().copy();

        DataStreamPythonFunctionInfo headPythonFunctionInfoOfDownOperator = downPythonFunctionInfo;
        while (headPythonFunctionInfoOfDownOperator.getInputs().length != 0) {
            headPythonFunctionInfoOfDownOperator =
                    (DataStreamPythonFunctionInfo)
                            headPythonFunctionInfoOfDownOperator.getInputs()[0];
        }
        headPythonFunctionInfoOfDownOperator.setInputs(
                new DataStreamPythonFunctionInfo[] {upPythonFunctionInfo});

        final DataStreamPythonFunctionOperator chainedOperator =
                upOperator.copy(
                        downPythonFunctionInfo,
                        ((DataStreamPythonFunctionOperator) downOperator).getProducedType());
        chainedOperator.addSideOutputTags(downOperator.getSideOutputTags());

        PhysicalTransformation<?> chainedTransformation;
        if (upOperator instanceof OneInputStreamOperator) {
            chainedTransformation =
                    new OneInputTransformation(
                            upTransform.getInputs().get(0),
                            upTransform.getName() + ", " + downTransform.getName(),
                            (OneInputStreamOperator<?, ?>) chainedOperator,
                            downTransform.getOutputType(),
                            upTransform.getParallelism(),
                            false);

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
                            upTransform.getParallelism(),
                            false);

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

        if (upTransform.getDescription() != null && downTransform.getDescription() != null) {
            chainedTransformation.setDescription(
                    upTransform.getDescription() + ", " + downTransform.getDescription());
        } else if (upTransform.getDescription() != null) {
            chainedTransformation.setDescription(upTransform.getDescription());
        } else if (downTransform.getDescription() != null) {
            chainedTransformation.setDescription(downTransform.getDescription());
        }
        return chainedTransformation;
    }

    private static boolean isChainable(
            Transformation<?> upTransform,
            Transformation<?> downTransform,
            Map<Transformation<?>, Set<Transformation<?>>> outputMap) {
        return upTransform.getParallelism() == downTransform.getParallelism()
                && upTransform.getMaxParallelism() == downTransform.getMaxParallelism()
                && upTransform.getSlotSharingGroup().equals(downTransform.getSlotSharingGroup())
                && areOperatorsChainable(upTransform, downTransform)
                && outputMap.get(upTransform).size() == 1;
    }

    private static boolean areOperatorsChainable(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        if (!areOperatorsChainableByChainingStrategy(upTransform, downTransform)) {
            return false;
        }

        if (upTransform instanceof PythonBroadcastStateTransformation
                || upTransform instanceof PythonKeyedBroadcastStateTransformation) {
            return false;
        }

        DataStreamPythonFunctionOperator<?> upOperator =
                (DataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(upTransform)).getOperator();

        DataStreamPythonFunctionOperator<?> downOperator =
                (DataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(downTransform))
                                .getOperator();

        if (!arePythonOperatorsInSameExecutionEnvironment(upOperator, downOperator)) {
            return false;
        }

        return (downOperator instanceof ExternalPythonProcessOperator
                        && (upOperator instanceof ExternalPythonKeyedProcessOperator
                                || upOperator instanceof ExternalPythonKeyedCoProcessOperator
                                || upOperator instanceof ExternalPythonProcessOperator
                                || upOperator instanceof ExternalPythonCoProcessOperator))
                || (downOperator instanceof EmbeddedPythonProcessOperator
                        && (upOperator instanceof EmbeddedPythonKeyedProcessOperator
                                || upOperator instanceof EmbeddedPythonKeyedCoProcessOperator
                                || upOperator instanceof EmbeddedPythonProcessOperator
                                || upOperator instanceof EmbeddedPythonCoProcessOperator
                                || upOperator instanceof EmbeddedPythonWindowOperator));
    }

    private static boolean arePythonOperatorsInSameExecutionEnvironment(
            DataStreamPythonFunctionOperator<?> upOperator,
            DataStreamPythonFunctionOperator<?> downOperator) {
        return upOperator instanceof AbstractExternalDataStreamPythonFunctionOperator
                        && downOperator instanceof AbstractExternalDataStreamPythonFunctionOperator
                || upOperator instanceof AbstractEmbeddedDataStreamPythonFunctionOperator
                        && downOperator instanceof AbstractEmbeddedDataStreamPythonFunctionOperator;
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

    // ----------------------- Utility Methods -----------------------

    private static void replaceInput(
            Transformation<?> transformation,
            Transformation<?> oldInput,
            Transformation<?> newInput) {
        try {
            if (transformation instanceof OneInputTransformation
                    || transformation instanceof SideOutputTransformation
                    || transformation instanceof ReduceTransformation
                    || transformation instanceof LegacySinkTransformation
                    || transformation instanceof TimestampsAndWatermarksTransformation
                    || transformation instanceof PartitionTransformation) {
                final Field inputField = transformation.getClass().getDeclaredField("input");
                inputField.setAccessible(true);
                inputField.set(transformation, newInput);
            } else if (transformation instanceof SinkTransformation) {
                final Field inputField = transformation.getClass().getDeclaredField("input");
                inputField.setAccessible(true);
                inputField.set(transformation, newInput);

                final Field transformationField =
                        DataStream.class.getDeclaredField("transformation");
                transformationField.setAccessible(true);
                transformationField.set(
                        ((SinkTransformation<?, ?>) transformation).getInputStream(), newInput);
            } else if (transformation instanceof TwoInputTransformation) {
                final Field inputField;
                if (((TwoInputTransformation<?, ?, ?>) transformation).getInput1() == oldInput) {
                    inputField = transformation.getClass().getDeclaredField("input1");
                } else {
                    inputField = transformation.getClass().getDeclaredField("input2");
                }
                inputField.setAccessible(true);
                inputField.set(transformation, newInput);
            } else if (transformation instanceof UnionTransformation
                    || transformation instanceof AbstractMultipleInputTransformation) {
                final Field inputsField = transformation.getClass().getDeclaredField("inputs");
                inputsField.setAccessible(true);
                List<Transformation<?>> newInputs = Lists.newArrayList();
                newInputs.addAll(transformation.getInputs());
                newInputs.remove(oldInput);
                newInputs.add(newInput);
                inputsField.set(transformation, newInputs);
            } else if (transformation instanceof AbstractBroadcastStateTransformation) {
                final Field inputField;
                if (((AbstractBroadcastStateTransformation<?, ?, ?>) transformation)
                                .getRegularInput()
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
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // This should never happen
            throw new RuntimeException(e);
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
