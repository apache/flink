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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec.GroupSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Batch {@link ExecNode} base class for sort-based over window aggregate. */
public abstract class BatchExecOverAggregateBase extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    protected final OverSpec overSpec;

    public BatchExecOverAggregateBase(
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.overSpec = overSpec;
    }

    protected RowType getInputTypeWithConstants() {
        final RowType inputRowType = (RowType) getInputEdges().get(0).getOutputType();
        final List<LogicalType> inputTypesWithConstants =
                new ArrayList<>(inputRowType.getChildren());
        final List<String> inputTypeNamesWithConstants =
                new ArrayList<>(inputRowType.getFieldNames());
        for (int i = 0; i < overSpec.getConstants().size(); ++i) {
            inputTypesWithConstants.add(
                    FlinkTypeFactory.toLogicalType(overSpec.getConstants().get(i).getType()));
            inputTypeNamesWithConstants.add("TMP" + i);
        }
        return RowType.of(
                inputTypesWithConstants.toArray(new LogicalType[0]),
                inputTypeNamesWithConstants.toArray(new String[0]));
    }

    protected boolean isUnboundedWindow(GroupSpec group) {
        return group.getLowerBound().isUnbounded() && group.getUpperBound().isUnbounded();
    }

    protected boolean isUnboundedPrecedingWindow(GroupSpec group) {
        return group.getLowerBound().isUnbounded() && !group.getUpperBound().isUnbounded();
    }

    protected boolean isUnboundedFollowingWindow(GroupSpec group) {
        return !group.getLowerBound().isUnbounded() && group.getUpperBound().isUnbounded();
    }

    protected boolean isSlidingWindow(GroupSpec group) {
        return !group.getLowerBound().isUnbounded() && !group.getUpperBound().isUnbounded();
    }

    protected List<RexLiteral> getConstants() {
        return overSpec.getConstants();
    }

    /** Infer the over window mode based on given group info. */
    protected OverWindowMode inferGroupMode(GroupSpec group) {
        AggregateCall aggCall = group.getAggCalls().get(0);
        if (aggCall.getAggregation().allowsFraming()) {
            if (group.isRows()) {
                return OverWindowMode.ROW;
            } else {
                return OverWindowMode.RANGE;
            }
        } else {
            if (aggCall.getAggregation() instanceof SqlLeadLagAggFunction) {
                return OverWindowMode.OFFSET;
            } else {
                return OverWindowMode.INSENSITIVE;
            }
        }
    }

    /** OverWindowMode describes the mode of a group in over window. */
    public enum OverWindowMode {
        /** The ROW mode allows window framing with ROW clause. */
        ROW,
        /** The RANGE mode allows window framing with RANGE clause. */
        RANGE,
        /**
         * The OFFSET mode does not care the window framing with LEAD/LAG agg function, see {@link
         * SqlLeadLagAggFunction}.
         */
        OFFSET,
        /**
         * The INSENSITIVE mode does not care the window framing without LEAD/LAG agg function, for
         * example RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST/ROW_NUMBER.
         */
        INSENSITIVE
    }
}
