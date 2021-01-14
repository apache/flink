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
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeMiniBatchDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeMiniBatchDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeMiniBatchDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;

/**
 * Stream {@link ExecNode} which deduplicate on keys and keeps only first row or last row. This node
 * is an optimization of {@link StreamExecRank} for some special cases. Compared to {@link
 * StreamExecRank}, this node could use mini-batch and access less state.
 */
public class StreamExecDeduplicate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

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

    private final int[] uniqueKeys;
    private final boolean isRowtime;
    private final boolean keepLastRow;
    private final boolean generateUpdateBefore;

    public StreamExecDeduplicate(
            int[] uniqueKeys,
            boolean isRowtime,
            boolean keepLastRow,
            boolean generateUpdateBefore,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.uniqueKeys = uniqueKeys;
        this.isRowtime = isRowtime;
        this.keepLastRow = keepLastRow;
        this.generateUpdateBefore = generateUpdateBefore;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

        final RowType inputRowType = (RowType) inputNode.getOutputType();
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
                                    keepLastRow,
                                    generateUpdateBefore)
                            .createDeduplicateOperator();
        }

        final OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDesc(),
                        operator,
                        rowTypeInfo,
                        inputTransform.getParallelism());

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(uniqueKeys, rowTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

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

        protected long getMinRetentionTime() {
            return tableConfig.getMinIdleStateRetentionTime();
        }

        protected long getMiniBatchSize() {
            if (isMiniBatchEnabled()) {
                long size =
                        tableConfig
                                .getConfiguration()
                                .getLong(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
                Preconditions.checkArgument(
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

    /** Translator to create process time deduplicate operator. */
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
            Preconditions.checkArgument(rowtimeIndex >= 0);
            if (isMiniBatchEnabled()) {
                CountBundleTrigger<RowData> trigger = new CountBundleTrigger<>(getMiniBatchSize());
                RowTimeMiniBatchDeduplicateFunction processFunction =
                        new RowTimeMiniBatchDeduplicateFunction(
                                rowTypeInfo,
                                typeSerializer,
                                getMinRetentionTime(),
                                rowtimeIndex,
                                generateUpdateBefore,
                                generateInsert(),
                                keepLastRow);
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

        protected ProcTimeDeduplicateOperatorTranslator(
                TableConfig tableConfig,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                boolean keepLastRow,
                boolean generateUpdateBefore) {
            super(tableConfig, rowTypeInfo, typeSerializer, keepLastRow, generateUpdateBefore);
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
                                    true);
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
                                    true);
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
