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

package org.apache.flink.python.util;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.DataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.process.AbstractExternalOneInputPythonFunctionOperator;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.python.DelegateOperatorTransformation;
import org.apache.flink.streaming.api.transformations.python.PythonBroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.python.PythonKeyedBroadcastStateTransformation;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.translators.python.PythonBroadcastStateTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.python.PythonKeyedBroadcastStateTransformationTranslator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava33.com.google.common.collect.Queues;
import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/** A Util class to handle the configurations of Python jobs. */
public class PythonConfigUtil {

    public static final String KEYED_STREAM_VALUE_OPERATOR_NAME = "_keyed_stream_values_operator";
    public static final String STREAM_KEY_BY_MAP_OPERATOR_NAME = "_stream_key_by_map_operator";
    public static final String STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME =
            "_partition_custom_map_operator";

    public static Configuration getEnvironmentConfig(StreamExecutionEnvironment env) {
        return (Configuration) env.getConfiguration();
    }

    public static void configPythonOperator(StreamExecutionEnvironment env) throws Exception {
        final Configuration config = extractPythonConfiguration(env.getConfiguration());

        env.getConfiguration()
                .getOptional(PipelineOptions.CACHED_FILES)
                .ifPresent(
                        f -> {
                            env.getCachedFiles().clear();
                            env.getCachedFiles()
                                    .addAll(DistributedCache.parseCachedFilesFromString(f));
                        });

        if (!env.getConfig().hasAutoGeneratedUIDsEnabled()) {
            assignHelperOperatorUids(env.getTransformations());
        }

        for (Transformation<?> transformation : env.getTransformations()) {
            alignTransformation(transformation);

            if (isPythonOperator(transformation)) {
                // declare the use case of managed memory
                transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);

                AbstractPythonFunctionOperator<?> pythonFunctionOperator =
                        getPythonOperator(transformation);
                if (pythonFunctionOperator != null) {
                    pythonFunctionOperator.getConfiguration().addAll(config);
                }
            }
        }

        processSideOutput(env.getTransformations());
        registerPythonBroadcastTransformationTranslator();
    }

    /** Extract the configurations which is used in the Python operators. */
    public static Configuration extractPythonConfiguration(ReadableConfig config) {
        final Configuration pythonDependencyConfig =
                PythonDependencyUtils.configurePythonDependencies(config);
        final PythonConfig pythonConfig = new PythonConfig(config, pythonDependencyConfig);
        return pythonConfig.toConfiguration();
    }

    /**
     * Process {@link SideOutputTransformation}s, set the {@link OutputTag}s into the Python
     * corresponding operator to make it aware of the {@link OutputTag}s.
     */
    private static void processSideOutput(List<Transformation<?>> transformations) {
        final Set<Transformation<?>> visitedTransforms = Sets.newIdentityHashSet();
        final Queue<Transformation<?>> queue = Queues.newArrayDeque(transformations);

        while (!queue.isEmpty()) {
            Transformation<?> transform = queue.poll();
            visitedTransforms.add(transform);

            if (transform instanceof SideOutputTransformation) {
                final SideOutputTransformation<?> sideTransform =
                        (SideOutputTransformation<?>) transform;
                final Transformation<?> upTransform =
                        Iterables.getOnlyElement(sideTransform.getInputs());
                if (PythonConfigUtil.isPythonDataStreamOperator(upTransform)) {
                    final DataStreamPythonFunctionOperator<?> upOperator =
                            (DataStreamPythonFunctionOperator<?>)
                                    ((SimpleOperatorFactory<?>) getOperatorFactory(upTransform))
                                            .getOperator();
                    upOperator.addSideOutputTags(
                            Collections.singletonList(sideTransform.getOutputTag()));
                }
            }

            for (Transformation<?> upTransform : transform.getInputs()) {
                if (!visitedTransforms.contains(upTransform)) {
                    queue.add(upTransform);
                }
            }
        }
    }

    public static StreamOperatorFactory<?> getOperatorFactory(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            return ((OneInputTransformation<?, ?>) transform).getOperatorFactory();
        } else if (transform instanceof TwoInputTransformation) {
            return ((TwoInputTransformation<?, ?, ?>) transform).getOperatorFactory();
        } else if (transform instanceof AbstractMultipleInputTransformation) {
            return ((AbstractMultipleInputTransformation<?>) transform).getOperatorFactory();
        } else if (transform instanceof DelegateOperatorTransformation<?>) {
            return ((DelegateOperatorTransformation<?>) transform).getOperatorFactory();
        } else {
            return null;
        }
    }

    /**
     * Configure the {@link AbstractExternalOneInputPythonFunctionOperator} to be chained with the
     * upstream/downstream operator by setting their parallelism, slot sharing group, co-location
     * group to be the same, and applying a {@link ForwardPartitioner}. 1. operator with name
     * "_keyed_stream_values_operator" should align with its downstream operator. 2. operator with
     * name "_stream_key_by_map_operator" should align with its upstream operator.
     */
    private static void alignTransformation(Transformation<?> transformation)
            throws NoSuchFieldException, IllegalAccessException {
        String transformName = transformation.getName();
        if (transformation.getInputs().isEmpty()) {
            return;
        }
        Transformation<?> inputTransformation = transformation.getInputs().get(0);
        String inputTransformName = inputTransformation.getName();
        if (inputTransformName.equals(KEYED_STREAM_VALUE_OPERATOR_NAME)) {
            chainTransformation(inputTransformation, transformation);
            configForwardPartitioner(inputTransformation, transformation);
        }
        if (transformName.equals(STREAM_KEY_BY_MAP_OPERATOR_NAME)
                || transformName.equals(STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME)) {
            chainTransformation(transformation, inputTransformation);
            configForwardPartitioner(inputTransformation, transformation);
        }
    }

    /**
     * Gives the operators that the Python DataStream API adds for {@code key_by}, {@code
     * partition_custom} and for turning a {@code KeyedStream} back into a {@code DataStream} a uid
     * derived from their input's uid, or a uid hash derived from their input's uid hash if the
     * input only has a hash. Users can't reach most of these operators, and with auto-generated
     * UIDs disabled one without a uid or uid hash can prevent submission. The uids and uid hashes
     * already in the graph are collected first, and a derived one that is already taken gets a
     * numeric suffix. The helpers are then visited in creation order, so a helper's input has
     * already been given its uid or uid hash. Only called when auto-generated UIDs are disabled, so
     * the operator IDs of other jobs don't change. Operators that already have a uid or a uid hash,
     * or whose input has neither, are left as they are.
     */
    private static void assignHelperOperatorUids(List<Transformation<?>> transformations) {
        final Set<String> usedUids = new HashSet<>();
        final Set<String> usedUidHashes = new HashSet<>();
        final Set<Transformation<?>> visited = Sets.newIdentityHashSet();
        final Queue<Transformation<?>> queue = Queues.newArrayDeque(transformations);
        while (!queue.isEmpty()) {
            final Transformation<?> transformation = queue.poll();
            if (!visited.add(transformation)) {
                continue;
            }
            if (transformation.getUid() != null) {
                usedUids.add(transformation.getUid());
            }
            if (transformation.getUserProvidedNodeHash() != null) {
                // Hex digits in either case decode to the same operator ID.
                usedUidHashes.add(
                        transformation.getUserProvidedNodeHash().toLowerCase(Locale.ROOT));
            }
            queue.addAll(transformation.getInputs());
        }
        for (Transformation<?> transformation : transformations) {
            final String suffix = getHelperOperatorUidSuffix(transformation.getName());
            if (suffix == null
                    || transformation.getUid() != null
                    || transformation.getUserProvidedNodeHash() != null) {
                continue;
            }
            final Optional<Transformation<?>> input =
                    getNonPartitionTransformationInput(transformation);
            if (!input.isPresent()) {
                continue;
            }
            final String inputUid = input.get().getUid();
            final String inputUidHash = input.get().getUserProvidedNodeHash();
            if (inputUid != null) {
                final String baseUid = inputUid + "-" + suffix;
                String uid = baseUid;
                for (int index = 2; !usedUids.add(uid); index++) {
                    uid = baseUid + "-" + index;
                }
                transformation.setUid(uid);
            } else if (inputUidHash != null) {
                String uidHash = deriveUidHash(inputUidHash, suffix);
                for (int index = 2; !usedUidHashes.add(uidHash); index++) {
                    uidHash = deriveUidHash(inputUidHash, suffix + "-" + index);
                }
                transformation.setUidHash(uidHash);
            }
        }
    }

    /**
     * Derives a uid hash the same way as {@code _derive_uid_hash} in {@code
     * pyflink/datastream/data_stream.py}, which does it for the operators {@code
     * assign_timestamps_and_watermarks} hides. A uid hash must be 32 hex digits, so the suffix
     * cannot be appended to it as it is to a uid.
     */
    private static String deriveUidHash(String uidHash, String suffix) {
        try {
            final byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest((uidHash + "-" + suffix).getBytes(StandardCharsets.UTF_8));
            return StringUtils.byteToHexString(digest).substring(0, 32);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String getHelperOperatorUidSuffix(String transformationName) {
        if (STREAM_KEY_BY_MAP_OPERATOR_NAME.equals(transformationName)) {
            return "key-by";
        }
        if (STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME.equals(transformationName)) {
            return "partition-custom";
        }
        if (KEYED_STREAM_VALUE_OPERATOR_NAME.equals(transformationName)) {
            return "values";
        }
        return null;
    }

    private static void chainTransformation(
            Transformation<?> firstTransformation, Transformation<?> secondTransformation) {
        secondTransformation
                .getSlotSharingGroup()
                .ifPresent(firstTransformation::setSlotSharingGroup);
        firstTransformation.setCoLocationGroupKey(secondTransformation.getCoLocationGroupKey());
        firstTransformation.setParallelism(secondTransformation.getParallelism(), false);
    }

    private static void configForwardPartitioner(
            Transformation<?> upTransformation, Transformation<?> transformation)
            throws IllegalAccessException, NoSuchFieldException {
        // set ForwardPartitioner
        PartitionTransformation<?> partitionTransform =
                new PartitionTransformation<>(upTransformation, new ForwardPartitioner<>());
        Field inputTransformationField = transformation.getClass().getDeclaredField("input");
        inputTransformationField.setAccessible(true);
        inputTransformationField.set(transformation, partitionTransform);
    }

    private static AbstractPythonFunctionOperator<?> getPythonOperator(
            Transformation<?> transformation) {
        StreamOperatorFactory<?> operatorFactory = null;
        if (transformation instanceof OneInputTransformation) {
            operatorFactory = ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
        } else if (transformation instanceof TwoInputTransformation) {
            operatorFactory =
                    ((TwoInputTransformation<?, ?, ?>) transformation).getOperatorFactory();
        } else if (transformation instanceof AbstractMultipleInputTransformation) {
            operatorFactory =
                    ((AbstractMultipleInputTransformation<?>) transformation).getOperatorFactory();
        } else if (transformation instanceof DelegateOperatorTransformation) {
            operatorFactory =
                    ((DelegateOperatorTransformation<?>) transformation).getOperatorFactory();
        }

        if (operatorFactory instanceof SimpleOperatorFactory
                && ((SimpleOperatorFactory<?>) operatorFactory).getOperator()
                        instanceof AbstractPythonFunctionOperator) {
            return (AbstractPythonFunctionOperator<?>)
                    ((SimpleOperatorFactory<?>) operatorFactory).getOperator();
        }

        return null;
    }

    private static boolean isPythonOperator(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            return isPythonOperator(
                    ((OneInputTransformation<?, ?>) transform).getOperatorFactory());
        } else if (transform instanceof TwoInputTransformation) {
            return isPythonOperator(
                    ((TwoInputTransformation<?, ?, ?>) transform).getOperatorFactory());
        } else if (transform instanceof AbstractMultipleInputTransformation) {
            return isPythonOperator(
                    ((AbstractMultipleInputTransformation<?>) transform).getOperatorFactory());
        } else if (transform instanceof PythonBroadcastStateTransformation
                || transform instanceof PythonKeyedBroadcastStateTransformation) {
            return true;
        } else {
            return false;
        }
    }

    private static boolean isPythonOperator(StreamOperatorFactory<?> streamOperatorFactory) {
        if (streamOperatorFactory instanceof SimpleOperatorFactory) {
            return ((SimpleOperatorFactory<?>) streamOperatorFactory).getOperator()
                    instanceof AbstractPythonFunctionOperator;
        } else {
            return false;
        }
    }

    public static boolean isPythonDataStreamOperator(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            return isPythonDataStreamOperator(
                    ((OneInputTransformation<?, ?>) transform).getOperatorFactory());
        } else if (transform instanceof TwoInputTransformation) {
            return isPythonDataStreamOperator(
                    ((TwoInputTransformation<?, ?, ?>) transform).getOperatorFactory());
        } else if (transform instanceof PythonBroadcastStateTransformation
                || transform instanceof PythonKeyedBroadcastStateTransformation) {
            return true;
        } else {
            return false;
        }
    }

    private static boolean isPythonDataStreamOperator(
            StreamOperatorFactory<?> streamOperatorFactory) {
        if (streamOperatorFactory instanceof SimpleOperatorFactory) {
            return ((SimpleOperatorFactory<?>) streamOperatorFactory).getOperator()
                    instanceof DataStreamPythonFunctionOperator;
        } else {
            return false;
        }
    }

    public static void setPartitionCustomOperatorNumPartitions(
            List<Transformation<?>> transformations) {
        // Update the numPartitions of PartitionCustomOperator after aligned all operators.
        final Set<Transformation<?>> alreadyTransformed = Sets.newIdentityHashSet();
        final Queue<Transformation<?>> toTransformQueue = Queues.newArrayDeque(transformations);
        while (!toTransformQueue.isEmpty()) {
            final Transformation<?> transformation = toTransformQueue.poll();
            if (!alreadyTransformed.contains(transformation)
                    && !(transformation instanceof PartitionTransformation)) {
                alreadyTransformed.add(transformation);

                getNonPartitionTransformationInput(transformation)
                        .ifPresent(
                                input -> {
                                    AbstractPythonFunctionOperator<?> pythonFunctionOperator =
                                            getPythonOperator(input);
                                    if (pythonFunctionOperator
                                            instanceof DataStreamPythonFunctionOperator) {
                                        DataStreamPythonFunctionOperator
                                                pythonDataStreamFunctionOperator =
                                                        (DataStreamPythonFunctionOperator)
                                                                pythonFunctionOperator;
                                        pythonDataStreamFunctionOperator.setNumPartitions(
                                                transformation.getParallelism());
                                    }
                                });

                toTransformQueue.addAll(transformation.getInputs());
            }
        }
    }

    private static Optional<Transformation<?>> getNonPartitionTransformationInput(
            Transformation<?> transformation) {
        if (transformation.getInputs().size() != 1) {
            return Optional.empty();
        }

        final Transformation<?> inputTransformation = transformation.getInputs().get(0);
        if (inputTransformation instanceof PartitionTransformation) {
            return getNonPartitionTransformationInput(inputTransformation);
        } else {
            return Optional.of(inputTransformation);
        }
    }

    public static List<MapStateDescriptor<ByteArrayWrapper, byte[]>>
            convertStateNamesToStateDescriptors(String[] names) {
        List<MapStateDescriptor<ByteArrayWrapper, byte[]>> descriptors =
                new ArrayList<>(names.length);
        TypeSerializer<byte[]> byteArraySerializer =
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.createSerializer(
                        new SerializerConfigImpl());
        for (String name : names) {
            descriptors.add(
                    new MapStateDescriptor<>(
                            name, ByteArrayWrapperSerializer.INSTANCE, byteArraySerializer));
        }
        return descriptors;
    }

    @SuppressWarnings("rawtypes,unchecked")
    public static void registerPythonBroadcastTransformationTranslator() throws Exception {
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
                PythonBroadcastStateTransformation.class,
                new PythonBroadcastStateTransformationTranslator<>());
        underlyingMap.put(
                PythonKeyedBroadcastStateTransformation.class,
                new PythonKeyedBroadcastStateTransformationTranslator<>());
    }

    @SuppressWarnings("rawtypes")
    public static SingleOutputStreamOperator<?> createSingleOutputStreamOperator(
            StreamExecutionEnvironment env, Transformation<?> transformation) throws Exception {
        Constructor<SingleOutputStreamOperator> constructor =
                SingleOutputStreamOperator.class.getDeclaredConstructor(
                        StreamExecutionEnvironment.class, Transformation.class);
        constructor.setAccessible(true);
        return constructor.newInstance(env, transformation);
    }
}
