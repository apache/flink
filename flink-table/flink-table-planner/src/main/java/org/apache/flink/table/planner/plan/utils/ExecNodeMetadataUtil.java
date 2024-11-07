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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecAsyncCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIncrementalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLegacySink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/** Utility class for {@link ExecNodeMetadata} related functionality. */
@Internal
public final class ExecNodeMetadataUtil {

    private ExecNodeMetadataUtil() {
        // no instantiation
    }

    private static final Set<Class<? extends ExecNode<?>>> EXEC_NODES =
            new HashSet<Class<? extends ExecNode<?>>>() {
                {
                    add(StreamExecCalc.class);
                    add(StreamExecChangelogNormalize.class);
                    add(StreamExecCorrelate.class);
                    add(StreamExecDeduplicate.class);
                    add(StreamExecDropUpdateBefore.class);
                    add(StreamExecExchange.class);
                    add(StreamExecExpand.class);
                    add(StreamExecGlobalGroupAggregate.class);
                    add(StreamExecGlobalWindowAggregate.class);
                    add(StreamExecGroupAggregate.class);
                    add(StreamExecGroupWindowAggregate.class);
                    add(StreamExecIncrementalGroupAggregate.class);
                    add(StreamExecIntervalJoin.class);
                    add(StreamExecJoin.class);
                    add(StreamExecLimit.class);
                    add(StreamExecLocalGroupAggregate.class);
                    add(StreamExecLocalWindowAggregate.class);
                    add(StreamExecLookupJoin.class);
                    add(StreamExecMatch.class);
                    add(StreamExecMiniBatchAssigner.class);
                    add(StreamExecOverAggregate.class);
                    add(StreamExecRank.class);
                    add(StreamExecSink.class);
                    add(StreamExecSortLimit.class);
                    add(StreamExecSort.class);
                    add(StreamExecTableSourceScan.class);
                    add(StreamExecTemporalJoin.class);
                    add(StreamExecTemporalSort.class);
                    add(StreamExecUnion.class);
                    add(StreamExecValues.class);
                    add(StreamExecWatermarkAssigner.class);
                    add(StreamExecWindowAggregate.class);
                    add(StreamExecWindowDeduplicate.class);
                    add(StreamExecWindowJoin.class);
                    add(StreamExecWindowRank.class);
                    add(StreamExecWindowTableFunction.class);
                    add(StreamExecPythonCalc.class);
                    add(StreamExecAsyncCalc.class);
                    add(StreamExecPythonCorrelate.class);
                    add(StreamExecPythonGroupAggregate.class);
                    add(StreamExecPythonGroupWindowAggregate.class);
                    add(StreamExecPythonOverAggregate.class);
                    // Batch execution mode
                    add(BatchExecSink.class);
                    add(BatchExecTableSourceScan.class);
                    add(BatchExecCalc.class);
                    add(BatchExecExchange.class);
                    add(BatchExecSort.class);
                    add(BatchExecValues.class);
                    add(BatchExecCorrelate.class);
                    add(BatchExecHashJoin.class);
                    add(BatchExecNestedLoopJoin.class);
                    add(BatchExecLimit.class);
                    add(BatchExecUnion.class);
                    add(BatchExecHashAggregate.class);
                    add(BatchExecExpand.class);
                    add(BatchExecSortAggregate.class);
                    add(BatchExecSortLimit.class);
                    add(BatchExecMatch.class);
                }
            };

    private static final Map<ExecNodeNameVersion, Class<? extends ExecNode<?>>> LOOKUP_MAP =
            new HashMap<>();

    static {
        for (Class<? extends ExecNode<?>> execNodeClass : EXEC_NODES) {
            addToLookupMap(execNodeClass);
        }
    }

    @SuppressWarnings("rawtypes")
    static final Set<Class<? extends ExecNode>> UNSUPPORTED_JSON_SERDE_CLASSES =
            new HashSet<Class<? extends ExecNode>>() {
                {
                    add(StreamExecDataStreamScan.class);
                    add(StreamExecLegacyTableSourceScan.class);
                    add(StreamExecLegacySink.class);
                    add(StreamExecGroupTableAggregate.class);
                    add(StreamExecPythonGroupTableAggregate.class);
                    add(StreamExecMultipleInput.class);
                }
            };

    public static final Set<ConfigOption<?>> TABLE_CONFIG_OPTIONS;

    static {
        TABLE_CONFIG_OPTIONS = ConfigUtils.getAllConfigOptions(TableConfigOptions.class);
    }

    public static final Set<ConfigOption<?>> EXECUTION_CONFIG_OPTIONS;

    static {
        EXECUTION_CONFIG_OPTIONS = ConfigUtils.getAllConfigOptions(ExecutionConfigOptions.class);
    }

    public static Set<Class<? extends ExecNode<?>>> execNodes() {
        return EXEC_NODES;
    }

    public static Map<ExecNodeNameVersion, Class<? extends ExecNode<?>>> getVersionedExecNodes() {
        return LOOKUP_MAP;
    }

    public static Class<? extends ExecNode<?>> retrieveExecNode(String name, int version) {
        return LOOKUP_MAP.get(new ExecNodeNameVersion(name, version));
    }

    public static <T extends ExecNode<?>> boolean isUnsupported(Class<T> execNode) {
        boolean streamOrKnownExecNode =
                StreamExecNode.class.isAssignableFrom(execNode) || execNodes().contains(execNode);
        return !streamOrKnownExecNode || UNSUPPORTED_JSON_SERDE_CLASSES.contains(execNode);
    }

    public static void addTestNode(Class<? extends ExecNode<?>> execNodeClass) {
        addToLookupMap(execNodeClass);
    }

    public static <T extends ExecNode<?>> List<ExecNodeMetadata> extractMetadataFromAnnotation(
            Class<T> execNodeClass) {
        List<ExecNodeMetadata> metadata = new ArrayList<>();
        ExecNodeMetadata annotation = execNodeClass.getDeclaredAnnotation(ExecNodeMetadata.class);
        if (annotation != null) {
            metadata.add(annotation);
        }

        MultipleExecNodeMetadata annotations =
                execNodeClass.getDeclaredAnnotation(MultipleExecNodeMetadata.class);
        if (annotations != null) {
            if (metadata.isEmpty()) {
                for (ExecNodeMetadata annot : annotations.value()) {
                    if (annot != null) {
                        metadata.add(annot);
                    }
                }
            } else {
                throw new IllegalStateException(
                        String.format(
                                "ExecNode: %s is annotated both with %s and %s. Please use only "
                                        + "%s or multiple %s",
                                execNodeClass.getCanonicalName(),
                                ExecNodeMetadata.class,
                                MultipleExecNodeMetadata.class,
                                MultipleExecNodeMetadata.class,
                                ExecNodeMetadata.class));
            }
        }
        return metadata;
    }

    private static void addToLookupMap(Class<? extends ExecNode<?>> execNodeClass) {
        if (!hasJsonCreatorAnnotation(execNodeClass)) {
            throw new IllegalStateException(
                    String.format(
                            "ExecNode: %s does not implement @JsonCreator annotation on "
                                    + "constructor.",
                            execNodeClass.getCanonicalName()));
        }

        List<ExecNodeMetadata> metadata = extractMetadataFromAnnotation(execNodeClass);
        if (metadata.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "ExecNode: %s is missing %s annotation.",
                            execNodeClass.getCanonicalName(),
                            ExecNodeMetadata.class.getSimpleName()));
        }

        for (ExecNodeMetadata meta : metadata) {
            doAddToMap(new ExecNodeNameVersion(meta.name(), meta.version()), execNodeClass);
        }
    }

    private static void doAddToMap(
            ExecNodeNameVersion key, Class<? extends ExecNode<?>> execNodeClass) {
        if (LOOKUP_MAP.containsKey(key)) {
            throw new IllegalStateException(String.format("Found duplicate ExecNode: %s.", key));
        }
        LOOKUP_MAP.put(key, execNodeClass);
    }

    /**
     * Returns the {@link ExecNodeMetadata} annotation of the class with the highest (most recent)
     * {@link ExecNodeMetadata#version()}.
     */
    public static <T extends ExecNode<?>> ExecNodeMetadata latestAnnotation(
            Class<T> execNodeClass) {
        List<ExecNodeMetadata> sortedAnnotations = extractMetadataFromAnnotation(execNodeClass);
        if (sortedAnnotations.isEmpty()) {
            return null;
        }
        sortedAnnotations.sort(Comparator.comparingInt(ExecNodeMetadata::version));
        return sortedAnnotations.get(sortedAnnotations.size() - 1);
    }

    @Nullable
    public static <T extends ExecNode<?>> String[] consumedOptions(Class<T> execNodeClass) {
        ExecNodeMetadata metadata = latestAnnotation(execNodeClass);
        if (metadata == null) {
            return null;
        }
        return metadata.consumedOptions();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T extends ExecNode<?>> ReadableConfig newPersistedConfig(
            Class<T> execNodeClass,
            ReadableConfig tableConfig,
            Stream<ConfigOption<?>> configOptions) {
        final Map<String, ConfigOption<?>> availableConfigOptions = new HashMap<>();
        configOptions.forEach(
                co -> {
                    availableConfigOptions.put(co.key(), co);
                    co.fallbackKeys().forEach(k -> availableConfigOptions.put(k.getKey(), co));
                });

        final Configuration persistedConfig = new Configuration();
        final String[] consumedOptions = ExecNodeMetadataUtil.consumedOptions(execNodeClass);
        if (consumedOptions == null) {
            return persistedConfig;
        }

        final Map<ConfigOption, Object> nodeConfigOptions = new HashMap<>();
        for (final String consumedOption : consumedOptions) {
            ConfigOption configOption = availableConfigOptions.get(consumedOption);
            if (configOption == null) {
                throw new IllegalStateException(
                        String.format(
                                "ExecNode: %s, consumedOption: %s not listed in [%s].",
                                execNodeClass.getCanonicalName(),
                                consumedOption,
                                String.join(
                                        ", ",
                                        Arrays.asList(
                                                TableConfigOptions.class.getSimpleName(),
                                                ExecutionConfigOptions.class.getSimpleName()))));
            }
            if (nodeConfigOptions.containsKey(configOption)) {
                throw new IllegalStateException(
                        String.format(
                                "ExecNode: %s, consumedOption: %s is listed multiple times in "
                                        + "consumedOptions, potentially also with "
                                        + "fallback/deprecated key.",
                                execNodeClass.getCanonicalName(), consumedOption));
            } else {
                nodeConfigOptions.put(configOption, tableConfig.get(configOption));
            }
        }
        nodeConfigOptions.forEach(persistedConfig::set);
        return persistedConfig;
    }

    /** Helper Pojo used as a tuple for the {@link #LOOKUP_MAP}. */
    public static final class ExecNodeNameVersion {

        private final String name;
        private final int version;

        private ExecNodeNameVersion(String name, int version) {
            this.name = name;
            this.version = version;
        }

        @Override
        public String toString() {
            return String.format("name: %s, version: %s", name, version);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExecNodeNameVersion that = (ExecNodeNameVersion) o;
            return version == that.version && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version);
        }
    }

    /** Return true if the given class's constructors have @JsonCreator annotation, else false. */
    static boolean hasJsonCreatorAnnotation(Class<?> clazz) {
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            for (Annotation annotation : constructor.getAnnotations()) {
                if (annotation instanceof JsonCreator) {
                    return true;
                }
            }
        }
        return false;
    }
}
