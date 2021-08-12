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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} which deduplicate on keys and keeps only first row or last row. This node
 * is an optimization of {@link StreamExecRank} for some special cases. Compared to {@link
 * StreamExecRank}, this node could use mini-batch and access less state.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecDeduplicate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_UNIQUE_KEYS = "uniqueKeys";
    public static final String FIELD_NAME_IS_ROWTIME = "isRowtime";
    public static final String FIELD_NAME_KEEP_LAST_ROW = "keepLastRow";
    public static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";

    @Experimental
    public static final ConfigOption<Boolean> TABLE_EXEC_INSERT_AND_UPDATE_AFTER_SENSITIVE =
            ConfigOptions.key("table.exec.insert-and-updateafter-sensitive")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Set whether the job (especially the sinks) is sensitive to "
                                    + "INSERT messages and UPDATE_AFTER messages. "
                                    + "If false, Flink may send UPDATE_AFTER instead of INSERT for the first row "
                                    + "at some times (e.g. deduplication for last row). "
                                    + "If true, Flink will guarantee to send INSERT for the first row, "
                                    + "but there will be additional overhead."
                                    + "Default is true.");

    @Experimental
    public static final ConfigOption<Boolean> TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES =
            ConfigOptions.key("table.exec.deduplicate.mini-batch.compact-changes")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Set whether to compact the changes sent downstream in row-time mini-batch. "
                                    + "If true, Flink will compact changes, only send the latest change to downstream. "
                                    + "Notes: If the downstream needs the details of versioned data, this optimization cannot be opened. "
                                    + "If false, Flink will send all changes to downstream just like when the mini-batch is not on.");

    @JsonProperty(FIELD_NAME_UNIQUE_KEYS)
    private final int[] uniqueKeys;

    @JsonProperty(FIELD_NAME_IS_ROWTIME)
    private final boolean isRowtime;

    @JsonProperty(FIELD_NAME_KEEP_LAST_ROW)
    private final boolean keepLastRow;

    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    public StreamExecDeduplicate(
            int[] uniqueKeys,
            boolean isRowtime,
            boolean keepLastRow,
            boolean generateUpdateBefore,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                uniqueKeys,
                isRowtime,
                keepLastRow,
                generateUpdateBefore,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecDeduplicate(
            @JsonProperty(FIELD_NAME_UNIQUE_KEYS) int[] uniqueKeys,
            @JsonProperty(FIELD_NAME_IS_ROWTIME) boolean isRowtime,
            @JsonProperty(FIELD_NAME_KEEP_LAST_ROW) boolean keepLastRow,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.uniqueKeys = checkNotNull(uniqueKeys);
        this.isRowtime = isRowtime;
        this.keepLastRow = keepLastRow;
        this.generateUpdateBefore = generateUpdateBefore;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final InternalTypeInfo<RowData> rowTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        final TypeSerializer<RowData> rowSerializer =
                rowTypeInfo.createSerializer(planner.getExecEnv().getConfig());
        final OneInputStreamOperator<RowData, RowData> operator;

        if (isRowtime) {
            operator =
                    new RowtimeDeduplicateOperatorTranslator(
                                    planner.getTableConfig(),
                                    rowTypeInfo,
                                    rowSerializer,
                                    inputRowType,
                                    keepLastRow,
                                    generateUpdateBefore)
                            .createDeduplicateOperator();
        } else {
            operator =
                    new ProcTimeDeduplicateOperatorTranslator(
                                    planner.getTableConfig(),
                                    rowTypeInfo,
                                    rowSerializer,
                                    inputRowType,
                                    keepLastRow,
                                    generateUpdateBefore)
                            .createDeduplicateOperator();
        }

        final OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDescription(),
                        operator,
                        rowTypeInfo,
                        inputTransform.getParallelism());

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(uniqueKeys, rowTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        return transform;
    }

    /** Base translator to create deduplicate operator. */
    private abstract static class DeduplicateOperatorTranslator {
        private final TableConfig tableConfig;
        protected final InternalTypeInfo<RowData> rowTypeInfo;
        protected final TypeSerializer<RowData> typeSerializer;
        protected final boolean keepLastRow;
        protected final boolean generateUpdateBefore;

        protected DeduplicateOperatorTranslator(
                TableConfig tableConfig,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                boolean keepLastRow,
                boolean generateUpdateBefore) {
            this.tableConfig = tableConfig;
            this.rowTypeInfo = rowTypeInfo;
            this.typeSerializer = typeSerializer;
            this.keepLastRow = keepLastRow;
            this.generateUpdateBefore = generateUpdateBefore;
        }

        protected boolean generateInsert() {
            return tableConfig
                    .getConfiguration()
                    .getBoolean(TABLE_EXEC_INSERT_AND_UPDATE_AFTER_SENSITIVE);
        }

        protected boolean isMiniBatchEnabled() {
            return tableConfig
                    .getConfiguration()
                    .getBoolean(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
        }

        protected boolean isCompactChanges() {
            return tableConfig
                    .getConfiguration()
                    .getBoolean(TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES);
        }

        protected long getMinRetentionTime() {
            return tableConfig.getMinIdleStateRetentionTime();
        }

        protected long getMiniBatchSize() {
            if (isMiniBatchEnabled()) {
                long size =
                        tableConfig
                                .getConfiguration()
                                .getLong(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
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
                TableConfig tableConfig,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                RowType inputRowType,
                boolean keepLastRow,
                boolean generateUpdateBefore) {
            super(tableConfig, rowTypeInfo, typeSerializer, keepLastRow, generateUpdateBefore);
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
                MapBundleFunction processFunction;
                if (isCompactChanges()) {
                    processFunction =
                            new RowTimeMiniBatchLatestChangeDeduplicateFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    getMinRetentionTime(),
                                    rowtimeIndex,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    keepLastRow);
                } else {
                    processFunction =
                            new RowTimeMiniBatchDeduplicateFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    getMinRetentionTime(),
                                    rowtimeIndex,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    keepLastRow);
                }
                return new KeyedMapBundleOperator<>(processFunction, trigger);
            } else {
                RowTimeDeduplicateFunction processFunction =
                        new RowTimeDeduplicateFunction(
                                rowTypeInfo,
                                getMinRetentionTime(),
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
                TableConfig tableConfig,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                RowType inputRowType,
                boolean keepLastRow,
                boolean generateUpdateBefore) {
            super(tableConfig, rowTypeInfo, typeSerializer, keepLastRow, generateUpdateBefore);
            generatedEqualiser =
                    new EqualiserCodeGenerator(inputRowType)
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
                                    getMinRetentionTime(),
                                    generateUpdateBefore,
                                    generateInsert(),
                                    true,
                                    generatedEqualiser);
                    return new KeyedMapBundleOperator<>(processFunction, trigger);
                } else {
                    ProcTimeMiniBatchDeduplicateKeepFirstRowFunction processFunction =
                            new ProcTimeMiniBatchDeduplicateKeepFirstRowFunction(
                                    typeSerializer, getMinRetentionTime());
                    return new KeyedMapBundleOperator<>(processFunction, trigger);
                }
            } else {
                if (keepLastRow) {
                    ProcTimeDeduplicateKeepLastRowFunction processFunction =
                            new ProcTimeDeduplicateKeepLastRowFunction(
                                    rowTypeInfo,
                                    getMinRetentionTime(),
                                    generateUpdateBefore,
                                    generateInsert(),
                                    true,
                                    generatedEqualiser);
                    return new KeyedProcessOperator<>(processFunction);
                } else {
                    ProcTimeDeduplicateKeepFirstRowFunction processFunction =
                            new ProcTimeDeduplicateKeepFirstRowFunction(getMinRetentionTime());
                    return new KeyedProcessOperator<>(processFunction);
                }
            }
        }
    }
}
