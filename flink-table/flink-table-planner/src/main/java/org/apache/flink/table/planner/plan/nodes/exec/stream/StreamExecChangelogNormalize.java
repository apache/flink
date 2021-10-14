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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
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
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeMiniBatchDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Stream {@link ExecNode} which normalizes a changelog stream which maybe an upsert stream or a
 * changelog stream containing duplicate events. This node normalize such stream into a regular
 * changelog stream that contains INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE records without
 * duplication.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecChangelogNormalize extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_UNIQUE_KEYS = "uniqueKeys";
    public static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";

    @JsonProperty(FIELD_NAME_UNIQUE_KEYS)
    private final int[] uniqueKeys;

    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    public StreamExecChangelogNormalize(
            int[] uniqueKeys,
            boolean generateUpdateBefore,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                uniqueKeys,
                generateUpdateBefore,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecChangelogNormalize(
            @JsonProperty(FIELD_NAME_UNIQUE_KEYS) int[] uniqueKeys,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        this.uniqueKeys = uniqueKeys;
        this.generateUpdateBefore = generateUpdateBefore;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final InternalTypeInfo<RowData> rowTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();

        final OneInputStreamOperator<RowData, RowData> operator;
        final TableConfig tableConfig = planner.getTableConfig();
        final long stateIdleTime = tableConfig.getIdleStateRetention().toMillis();
        final boolean isMiniBatchEnabled =
                tableConfig
                        .getConfiguration()
                        .getBoolean(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);

        GeneratedRecordEqualiser generatedEqualiser =
                new EqualiserCodeGenerator(rowTypeInfo.toRowType())
                        .generateRecordEqualiser("DeduplicateRowEqualiser");

        if (isMiniBatchEnabled) {
            TypeSerializer<RowData> rowSerializer =
                    rowTypeInfo.createSerializer(planner.getExecEnv().getConfig());
            ProcTimeMiniBatchDeduplicateKeepLastRowFunction processFunction =
                    new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
                            rowTypeInfo,
                            rowSerializer,
                            stateIdleTime,
                            generateUpdateBefore,
                            true, // generateInsert
                            false, // inputInsertOnly
                            generatedEqualiser);
            CountBundleTrigger<RowData> trigger = AggregateUtil.createMiniBatchTrigger(tableConfig);
            operator = new KeyedMapBundleOperator<>(processFunction, trigger);
        } else {
            ProcTimeDeduplicateKeepLastRowFunction processFunction =
                    new ProcTimeDeduplicateKeepLastRowFunction(
                            rowTypeInfo,
                            stateIdleTime,
                            generateUpdateBefore,
                            true, // generateInsert
                            false, // inputInsertOnly
                            generatedEqualiser);
            operator = new KeyedProcessOperator<>(processFunction);
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
}
