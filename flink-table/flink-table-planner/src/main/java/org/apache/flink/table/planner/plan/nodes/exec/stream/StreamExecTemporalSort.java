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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.operators.sort.ProcTimeSortOperator;
import org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link StreamExecNode} for time-ascending-order Sort without `limit`. */
@ExecNodeMetadata(
        name = "stream-exec-temporal-sort",
        version = 1,
        producedTransformations = StreamExecTemporalSort.TEMPORAL_SORT_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecTemporalSort extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, MultipleTransformationTranslator<RowData> {

    public static final String TEMPORAL_SORT_TRANSFORMATION = "temporal-sort";

    public static final String FIELD_NAME_SORT_SPEC = "orderBy";

    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec sortSpec;

    public StreamExecTemporalSort(
            ReadableConfig tableConfig,
            SortSpec sortSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecTemporalSort.class),
                ExecNodeContext.newPersistedConfig(StreamExecTemporalSort.class, tableConfig),
                sortSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecTemporalSort(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.sortSpec = checkNotNull(sortSpec);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // time ordering needs to be ascending
        if (sortSpec.getFieldSize() == 0 || !sortSpec.getFieldSpec(0).getIsAscendingOrder()) {
            throw new TableException(
                    "Sort: Primary sort order of a streaming table must be ascending on time.\n"
                            + "please re-check sort statement according to the description above");
        }

        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();
        LogicalType timeType = inputType.getTypeAt(sortSpec.getFieldSpec(0).getFieldIndex());
        if (isRowtimeAttribute(timeType)) {
            return createSortRowTime(
                    inputType, inputTransform, config, planner.getFlinkContext().getClassLoader());
        } else if (isProctimeAttribute(timeType)) {
            return createSortProcTime(
                    inputType, inputTransform, config, planner.getFlinkContext().getClassLoader());
        } else {
            throw new TableException(
                    String.format(
                            "Sort: Internal Error\n"
                                    + "First field in temporal sort is not a time attribute, %s is given.",
                            timeType));
        }
    }

    /** Create Sort logic based on processing time. */
    private Transformation<RowData> createSortProcTime(
            RowType inputType,
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        // if the order has secondary sorting fields in addition to the proctime
        if (sortSpec.getFieldSize() > 1) {
            // skip the first field which is the proctime field and would be ordered by timer.
            SortSpec specExcludeTime = sortSpec.createSubSortSpec(1);

            GeneratedRecordComparator rowComparator =
                    ComparatorCodeGenerator.gen(
                            config,
                            classLoader,
                            "ProcTimeSortComparator",
                            inputType,
                            specExcludeTime);
            ProcTimeSortOperator sortOperator =
                    new ProcTimeSortOperator(InternalTypeInfo.of(inputType), rowComparator);

            OneInputTransformation<RowData, RowData> transform =
                    ExecNodeUtil.createOneInputTransformation(
                            inputTransform,
                            createTransformationMeta(TEMPORAL_SORT_TRANSFORMATION, config),
                            sortOperator,
                            InternalTypeInfo.of(inputType),
                            inputTransform.getParallelism(),
                            false);

            // as input node is singleton exchange, its parallelism is 1.
            if (inputsContainSingleton()) {
                transform.setParallelism(1);
                transform.setMaxParallelism(1);
            }

            EmptyRowDataKeySelector selector = EmptyRowDataKeySelector.INSTANCE;
            transform.setStateKeySelector(selector);
            transform.setStateKeyType(selector.getProducedType());
            return transform;
        } else {
            // if the order is done only on proctime we only need to forward the elements
            return inputTransform;
        }
    }

    /** Create Sort logic based on row time. */
    private Transformation<RowData> createSortRowTime(
            RowType inputType,
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        GeneratedRecordComparator rowComparator = null;
        if (sortSpec.getFieldSize() > 1) {
            // skip the first field which is the rowtime field and would be ordered by timer.
            SortSpec specExcludeTime = sortSpec.createSubSortSpec(1);
            rowComparator =
                    ComparatorCodeGenerator.gen(
                            config,
                            classLoader,
                            "RowTimeSortComparator",
                            inputType,
                            specExcludeTime);
        }
        RowTimeSortOperator sortOperator =
                new RowTimeSortOperator(
                        InternalTypeInfo.of(inputType),
                        sortSpec.getFieldSpec(0).getFieldIndex(),
                        rowComparator);

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(TEMPORAL_SORT_TRANSFORMATION, config),
                        sortOperator,
                        InternalTypeInfo.of(inputType),
                        inputTransform.getParallelism(),
                        false);

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        EmptyRowDataKeySelector selector = EmptyRowDataKeySelector.INSTANCE;
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }
}
