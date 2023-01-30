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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.deduplicate.window.RowTimeWindowDeduplicateOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for Window Deduplicate. */
@ExecNodeMetadata(
        name = "stream-exec-window-deduplicate",
        version = 1,
        consumedOptions = "table.local-time-zone",
        producedTransformations = StreamExecWindowDeduplicate.WINDOW_DEDUPLICATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecWindowDeduplicate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String WINDOW_DEDUPLICATE_TRANSFORMATION = "window-deduplicate";

    private static final long WINDOW_RANK_MEMORY_RATIO = 100;

    public static final String FIELD_NAME_PARTITION_KEYS = "partitionKeys";
    public static final String FIELD_NAME_ORDER_KEY = "orderKey";
    public static final String FIELD_NAME_KEEP_LAST_ROW = "keepLastRow";
    public static final String FIELD_NAME_WINDOWING = "windowing";

    @JsonProperty(FIELD_NAME_PARTITION_KEYS)
    private final int[] partitionKeys;

    @JsonProperty(FIELD_NAME_ORDER_KEY)
    private final int orderKey;

    @JsonProperty(FIELD_NAME_KEEP_LAST_ROW)
    private final boolean keepLastRow;

    @JsonProperty(FIELD_NAME_WINDOWING)
    private final WindowingStrategy windowing;

    public StreamExecWindowDeduplicate(
            ReadableConfig tableConfig,
            int[] partitionKeys,
            int orderKey,
            boolean keepLastRow,
            WindowingStrategy windowing,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecWindowDeduplicate.class),
                ExecNodeContext.newPersistedConfig(StreamExecWindowDeduplicate.class, tableConfig),
                partitionKeys,
                orderKey,
                keepLastRow,
                windowing,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecWindowDeduplicate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PARTITION_KEYS) int[] partitionKeys,
            @JsonProperty(FIELD_NAME_ORDER_KEY) int orderKey,
            @JsonProperty(FIELD_NAME_KEEP_LAST_ROW) boolean keepLastRow,
            @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.partitionKeys = checkNotNull(partitionKeys);
        this.orderKey = orderKey;
        this.keepLastRow = keepLastRow;
        this.windowing = checkNotNull(windowing);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // validate window strategy
        if (!windowing.isRowtime()) {
            throw new TableException("Processing time Window Deduplication is not supported yet.");
        }
        int windowEndIndex;
        if (windowing instanceof WindowAttachedWindowingStrategy) {
            windowEndIndex = ((WindowAttachedWindowingStrategy) windowing).getWindowEnd();
        } else {
            throw new UnsupportedOperationException(
                    windowing.getClass().getName() + " is not supported yet.");
        }

        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowing.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config));

        RowType inputType = (RowType) inputEdge.getOutputType();
        RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partitionKeys,
                        InternalTypeInfo.of(inputType));

        OneInputStreamOperator<RowData, RowData> operator =
                RowTimeWindowDeduplicateOperatorBuilder.builder()
                        .inputSerializer(new RowDataSerializer(inputType))
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(
                                (PagedTypeSerializer<RowData>)
                                        selector.getProducedType().toSerializer())
                        .keepLastRow(keepLastRow)
                        .rowtimeIndex(orderKey)
                        .windowEndIndex(windowEndIndex)
                        .build();

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(WINDOW_DEDUPLICATE_TRANSFORMATION, config),
                        SimpleOperatorFactory.of(operator),
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        WINDOW_RANK_MEMORY_RATIO,
                        false);

        // set KeyType and Selector for state
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }
}
