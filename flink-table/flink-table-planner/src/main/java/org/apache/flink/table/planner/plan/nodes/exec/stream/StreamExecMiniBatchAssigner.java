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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
import org.apache.flink.table.planner.plan.trait.MiniBatchMode;
import org.apache.flink.table.runtime.operators.wmassigners.ProcTimeMiniBatchAssignerOperator;
import org.apache.flink.table.runtime.operators.wmassigners.RowTimeMiniBatchAssginerOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} which injects a mini-batch event in the streaming data. The mini-batch
 * event will be recognized as a boundary between two mini-batches. The following operators will
 * keep track of the mini-batch events and trigger mini-batch once the mini-batch id is advanced.
 *
 * <p>NOTE: currently, we leverage the runtime watermark mechanism to achieve the mini-batch,
 * because runtime doesn't support customized events and the watermark mechanism fully meets
 * mini-batch needs.
 */
public class StreamExecMiniBatchAssigner extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_MINI_BATCH_INTERVAL = "miniBatchInterval";

    @JsonProperty(FIELD_NAME_MINI_BATCH_INTERVAL)
    private final MiniBatchInterval miniBatchInterval;

    public StreamExecMiniBatchAssigner(
            MiniBatchInterval miniBatchInterval,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                miniBatchInterval,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecMiniBatchAssigner(
            @JsonProperty(FIELD_NAME_MINI_BATCH_INTERVAL) MiniBatchInterval miniBatchInterval,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        this.miniBatchInterval = checkNotNull(miniBatchInterval);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);

        final OneInputStreamOperator<RowData, RowData> operator;
        if (miniBatchInterval.getMode() == MiniBatchMode.ProcTime) {
            operator = new ProcTimeMiniBatchAssignerOperator(miniBatchInterval.getInterval());
        } else if (miniBatchInterval.getMode() == MiniBatchMode.RowTime) {
            operator = new RowTimeMiniBatchAssginerOperator(miniBatchInterval.getInterval());
        } else {
            throw new TableException(
                    String.format(
                            "MiniBatchAssigner shouldn't be in %s mode this is a bug, please file an issue.",
                            miniBatchInterval.getMode()));
        }

        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                operator,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism());
    }
}
