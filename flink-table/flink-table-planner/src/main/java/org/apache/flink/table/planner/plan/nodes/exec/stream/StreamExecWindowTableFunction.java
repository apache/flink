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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.operator.UnalignedWindowTableFunctionOperator;
import org.apache.flink.table.runtime.operators.window.tvf.operator.WindowTableFunctionOperatorBase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.plan.utils.WindowTableFunctionUtil.createWindowAssigner;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Stream {@link ExecNode} which acts as a table-valued function to assign a window for each row of
 * the input relation. The return value of the new relation includes all the original columns as
 * well additional 3 columns named {@code window_start}, {@code window_end}, {@code window_time} to
 * indicate the assigned window.
 */
@ExecNodeMetadata(
        name = "stream-exec-window-table-function",
        version = 1,
        consumedOptions = "table.local-time-zone",
        producedTransformations = CommonExecWindowTableFunction.WINDOW_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecWindowTableFunction extends CommonExecWindowTableFunction
        implements StreamExecNode<RowData> {

    public StreamExecWindowTableFunction(
            ReadableConfig tableConfig,
            TimeAttributeWindowingStrategy windowingStrategy,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecWindowTableFunction.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecWindowTableFunction.class, tableConfig),
                windowingStrategy,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecWindowTableFunction(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_WINDOWING) TimeAttributeWindowingStrategy windowingStrategy,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                windowingStrategy,
                inputProperties,
                outputType,
                description);
    }

    protected Transformation<RowData> translateWithUnalignedWindow(
            PlannerBase planner,
            ExecNodeConfig config,
            RowType inputRowType,
            Transformation<RowData> inputTransform) {
        final WindowTableFunctionOperatorBase windowTableFunctionOperator =
                createUnalignedWindowTableFunctionOperator(config, inputRowType);
        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(WINDOW_TRANSFORMATION, config),
                        windowTableFunctionOperator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        final int[] partitionKeys = extractPartitionKeys(windowingStrategy.getWindow());
        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partitionKeys,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    private int[] extractPartitionKeys(WindowSpec window) {
        checkState(
                window instanceof SessionWindowSpec,
                "Only support unaligned window with session window now.");

        return ((SessionWindowSpec) window).getPartitionKeyIndices();
    }

    private WindowTableFunctionOperatorBase createUnalignedWindowTableFunctionOperator(
            ExecNodeConfig config, RowType inputRowType) {
        GroupWindowAssigner<TimeWindow> windowAssigner = createWindowAssigner(windowingStrategy);
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowingStrategy.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config));

        return new UnalignedWindowTableFunctionOperator(
                windowAssigner,
                windowAssigner.getWindowSerializer(new ExecutionConfig()),
                new RowDataSerializer(inputRowType),
                windowingStrategy.getTimeAttributeIndex(),
                shiftTimeZone);
    }
}
