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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeMiniBatchDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeMiniBatchDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeMiniBatchDeduplicateFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeMiniBatchLatestChangeDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} which deduplicate on keys and keeps only first row or last row. This node
 * is an optimization of {@link StreamExecRank} for some special cases. Compared to {@link
 * StreamExecRank}, this node could use mini-batch and access less state.
 */
@ExecNodeMetadata(
        name = "stream-exec-deduplicate",
        version = 1,
        consumedOptions = {
            "table.exec.mini-batch.enabled",
            "table.exec.mini-batch.size",
            "table.exec.deduplicate.insert-update-after-sensitive-enabled",
            "table.exec.deduplicate.mini-batch.compact-changes-enabled"
        },
        producedTransformations = StreamExecDeduplicate.DEDUPLICATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecDeduplicate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String DEDUPLICATE_TRANSFORMATION = "deduplicate";

    public static final String FIELD_NAME_UNIQUE_KEYS = "uniqueKeys";
    public static final String FIELD_NAME_IS_ROWTIME = "isRowtime";
    public static final String FIELD_NAME_KEEP_LAST_ROW = "keepLastRow";
    public static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";
    public static final String STATE_NAME = "deduplicateState";

    @JsonProperty(FIELD_NAME_UNIQUE_KEYS)
    private final int[] uniqueKeys;

    @JsonProperty(FIELD_NAME_IS_ROWTIME)
    private final boolean isRowtime;

    @JsonProperty(FIELD_NAME_KEEP_LAST_ROW)
    private final boolean keepLastRow;

    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecDeduplicate(
            ReadableConfig tableConfig,
            int[] uniqueKeys,
            boolean isRowtime,
            boolean keepLastRow,
            boolean generateUpdateBefore,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecDeduplicate.class),
                ExecNodeContext.newPersistedConfig(StreamExecDeduplicate.class, tableConfig),
                uniqueKeys,
                isRowtime,
                keepLastRow,
                generateUpdateBefore,
                StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecDeduplicate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_UNIQUE_KEYS) int[] uniqueKeys,
            @JsonProperty(FIELD_NAME_IS_ROWTIME) boolean isRowtime,
            @JsonProperty(FIELD_NAME_KEEP_LAST_ROW) boolean keepLastRow,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.uniqueKeys = checkNotNull(uniqueKeys);
        this.isRowtime = isRowtime;
        this.keepLastRow = keepLastRow;
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateMetadataList = stateMetadataList;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final InternalTypeInfo<RowData> rowTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        final TypeSerializer<RowData> rowSerializer =
                rowTypeInfo.createSerializer(planner.getExecEnv().getConfig());
        final OneInputStreamOperator<RowData, RowData> operator;

        long stateRetentionTime =
                StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList);
        if (isRowtime) {
            operator =
                    new RowtimeDeduplicateOperatorTranslator(
                                    config,
                                    rowTypeInfo,
                                    rowSerializer,
                                    inputRowType,
                                    keepLastRow,
                                    generateUpdateBefore,
                                    stateRetentionTime)
                            .createDeduplicateOperator();
        } else {
            operator =
                    new ProcTimeDeduplicateOperatorTranslator(
                                    config,
                                    planner.getFlinkContext().getClassLoader(),
                                    rowTypeInfo,
                                    rowSerializer,
                                    inputRowType,
                                    keepLastRow,
                                    generateUpdateBefore,
                                    stateRetentionTime)
                            .createDeduplicateOperator();
        }

        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(DEDUPLICATE_TRANSFORMATION, config),
                        operator,
                        rowTypeInfo,
                        inputTransform.getParallelism(),
                        false);

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(), uniqueKeys, rowTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        return transform;
    }

    /** Base translator to create deduplicate operator. */
    private abstract static class DeduplicateOperatorTranslator {
        private final ReadableConfig config;
        protected final InternalTypeInfo<RowData> rowTypeInfo;
        protected final TypeSerializer<RowData> typeSerializer;
        protected final boolean keepLastRow;
        protected final boolean generateUpdateBefore;
        protected final long stateRetentionTime;

        protected DeduplicateOperatorTranslator(
                ReadableConfig config,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                boolean keepLastRow,
                boolean generateUpdateBefore,
                long stateRetentionTime) {
            this.config = config;
            this.rowTypeInfo = rowTypeInfo;
            this.typeSerializer = typeSerializer;
            this.keepLastRow = keepLastRow;
            this.generateUpdateBefore = generateUpdateBefore;
            this.stateRetentionTime = stateRetentionTime;
        }

        protected boolean generateInsert() {
            return config.get(TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED);
        }

        protected boolean isMiniBatchEnabled() {
            return config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
        }

        protected boolean isCompactChanges() {
            return config.get(TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED);
        }

        protected long getMiniBatchSize() {
            if (isMiniBatchEnabled()) {
                long size = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
                checkArgument(
                        size > 0,
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE.key()
                                + " should be greater than 0.");
                return size;
            } else {
                return -1;
            }
        }

        abstract OneInputStreamOperator<RowData, RowData> createDeduplicateOperator();
    }

    /** Translator to create event time deduplicate operator. */
    private static class RowtimeDeduplicateOperatorTranslator
            extends DeduplicateOperatorTranslator {

        private final RowType inputRowType;

        protected RowtimeDeduplicateOperatorTranslator(
                ReadableConfig config,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                RowType inputRowType,
                boolean keepLastRow,
                boolean generateUpdateBefore,
                long stateRetentionTime) {
            super(
                    config,
                    rowTypeInfo,
                    typeSerializer,
                    keepLastRow,
                    generateUpdateBefore,
                    stateRetentionTime);
            this.inputRowType = inputRowType;
        }

        @Override
        OneInputStreamOperator<RowData, RowData> createDeduplicateOperator() {
            int rowtimeIndex = -1;
            for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
                if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                    rowtimeIndex = i;
                    break;
                }
            }
            checkArgument(rowtimeIndex >= 0);
            if (isMiniBatchEnabled()) {
                CountBundleTrigger<RowData> trigger = new CountBundleTrigger<>(getMiniBatchSize());
                if (isCompactChanges()) {
                    return new KeyedMapBundleOperator<>(
                            new RowTimeMiniBatchLatestChangeDeduplicateFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    stateRetentionTime,
                                    rowtimeIndex,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    keepLastRow),
                            trigger);
                } else {
                    return new KeyedMapBundleOperator<>(
                            new RowTimeMiniBatchDeduplicateFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    stateRetentionTime,
                                    rowtimeIndex,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    keepLastRow),
                            trigger);
                }
            } else {
                RowTimeDeduplicateFunction processFunction =
                        new RowTimeDeduplicateFunction(
                                rowTypeInfo,
                                stateRetentionTime,
                                rowtimeIndex,
                                generateUpdateBefore,
                                generateInsert(),
                                keepLastRow);
                return new KeyedProcessOperator<>(processFunction);
            }
        }
    }

    /** Translator to create process time deduplicate operator. */
    private static class ProcTimeDeduplicateOperatorTranslator
            extends DeduplicateOperatorTranslator {
        private final GeneratedRecordEqualiser generatedEqualiser;

        protected ProcTimeDeduplicateOperatorTranslator(
                ReadableConfig config,
                ClassLoader classLoader,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                RowType inputRowType,
                boolean keepLastRow,
                boolean generateUpdateBefore,
                long stateRetentionTime) {
            super(
                    config,
                    rowTypeInfo,
                    typeSerializer,
                    keepLastRow,
                    generateUpdateBefore,
                    stateRetentionTime);
            generatedEqualiser =
                    new EqualiserCodeGenerator(inputRowType, classLoader)
                            .generateRecordEqualiser("DeduplicateRowEqualiser");
        }

        @Override
        OneInputStreamOperator<RowData, RowData> createDeduplicateOperator() {
            if (isMiniBatchEnabled()) {
                CountBundleTrigger<RowData> trigger = new CountBundleTrigger<>(getMiniBatchSize());
                if (keepLastRow) {
                    ProcTimeMiniBatchDeduplicateKeepLastRowFunction processFunction =
                            new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    stateRetentionTime,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    true,
                                    generatedEqualiser);
                    return new KeyedMapBundleOperator<>(processFunction, trigger);
                } else {
                    ProcTimeMiniBatchDeduplicateKeepFirstRowFunction processFunction =
                            new ProcTimeMiniBatchDeduplicateKeepFirstRowFunction(
                                    typeSerializer, stateRetentionTime);
                    return new KeyedMapBundleOperator<>(processFunction, trigger);
                }
            } else {
                if (keepLastRow) {
                    ProcTimeDeduplicateKeepLastRowFunction processFunction =
                            new ProcTimeDeduplicateKeepLastRowFunction(
                                    rowTypeInfo,
                                    stateRetentionTime,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    true,
                                    generatedEqualiser);
                    return new KeyedProcessOperator<>(processFunction);
                } else {
                    ProcTimeDeduplicateKeepFirstRowFunction processFunction =
                            new ProcTimeDeduplicateKeepFirstRowFunction(stateRetentionTime);
                    return new KeyedProcessOperator<>(processFunction);
                }
            }
        }
    }
}
