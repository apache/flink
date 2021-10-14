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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator;
import org.apache.flink.table.runtime.operators.window.assigners.CumulativeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} which acts as a table-valued function to assign a window for each row of
 * the input relation. The return value of the new relation includes all the original columns as
 * well additional 3 columns named {@code window_start}, {@code window_end}, {@code window_time} to
 * indicate the assigned window.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecWindowTableFunction extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_WINDOWING = "windowing";
    public static final String FIELD_NAME_EMIT_PER_RECORD = "emitPerRecord";

    @JsonProperty(FIELD_NAME_WINDOWING)
    private final TimeAttributeWindowingStrategy windowingStrategy;

    @JsonProperty(FIELD_NAME_EMIT_PER_RECORD)
    private final Boolean emitPerRecord;

    public StreamExecWindowTableFunction(
            TimeAttributeWindowingStrategy windowingStrategy,
            Boolean emitPerRecord,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                windowingStrategy,
                emitPerRecord,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecWindowTableFunction(
            @JsonProperty(FIELD_NAME_WINDOWING) TimeAttributeWindowingStrategy windowingStrategy,
            @JsonProperty(FIELD_NAME_EMIT_PER_RECORD) Boolean emitPerRecord,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.windowingStrategy = checkNotNull(windowingStrategy);
        this.emitPerRecord = checkNotNull(emitPerRecord);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        String[] inputFieldNames = inputRowType.getFieldNames().toArray(new String[0]);
        String windowSummary = windowingStrategy.toSummaryString(inputFieldNames);

        if (!emitPerRecord) {
            throw new TableException(
                    String.format(
                            "Currently Flink doesn't support individual window table-valued function %s.\n "
                                    + "Please use window table-valued function with the following computations:\n"
                                    + "1. aggregate using window_start and window_end as group keys.\n"
                                    + "2. topN using window_start and window_end as partition key.\n"
                                    + "3. join with join condition contains window starts equality of input tables "
                                    + "and window ends equality of input tables.\n",
                            windowSummary));
        } else if (!windowingStrategy.isRowtime()) {
            throw new TableException("Processing time Window TableFunction is not supported yet.");
        }
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        WindowSpec windowSpec = windowingStrategy.getWindow();
        WindowAssigner<TimeWindow> windowAssigner = createWindowAssigner(windowSpec);
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowingStrategy.getTimeAttributeType(), planner.getTableConfig());
        WindowTableFunctionOperator windowTableFunctionOperator =
                new WindowTableFunctionOperator(
                        windowAssigner, windowingStrategy.getTimeAttributeIndex(), shiftTimeZone);
        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                windowTableFunctionOperator,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism());
    }

    private WindowAssigner<TimeWindow> createWindowAssigner(WindowSpec windowSpec) {
        if (windowSpec instanceof TumblingWindowSpec) {
            TumblingWindowSpec tumblingWindowSpec = (TumblingWindowSpec) windowSpec;
            TumblingWindowAssigner windowAssigner =
                    TumblingWindowAssigner.of(tumblingWindowSpec.getSize());
            if (tumblingWindowSpec.getOffset() != null) {
                windowAssigner = windowAssigner.withOffset(tumblingWindowSpec.getOffset());
            }
            return windowAssigner;
        } else if (windowSpec instanceof HoppingWindowSpec) {
            HoppingWindowSpec hoppingWindowSpec = (HoppingWindowSpec) windowSpec;
            SlidingWindowAssigner windowAssigner =
                    SlidingWindowAssigner.of(
                            hoppingWindowSpec.getSize(), hoppingWindowSpec.getSlide());
            if (hoppingWindowSpec.getOffset() != null) {
                windowAssigner = windowAssigner.withOffset(hoppingWindowSpec.getOffset());
            }
            return windowAssigner;
        } else if (windowSpec instanceof CumulativeWindowSpec) {
            CumulativeWindowSpec cumulativeWindowSpec = (CumulativeWindowSpec) windowSpec;
            CumulativeWindowAssigner windowAssigner =
                    CumulativeWindowAssigner.of(
                            cumulativeWindowSpec.getMaxSize(), cumulativeWindowSpec.getStep());
            if (cumulativeWindowSpec.getOffset() != null) {
                windowAssigner = windowAssigner.withOffset(cumulativeWindowSpec.getOffset());
            }
            return windowAssigner;
        } else {
            throw new TableException(
                    String.format(
                            "Unknown window spec: %s", windowSpec.getClass().getSimpleName()));
        }
    }
}
