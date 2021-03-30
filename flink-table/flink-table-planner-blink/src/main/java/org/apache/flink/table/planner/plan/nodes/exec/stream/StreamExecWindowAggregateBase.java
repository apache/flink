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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The base class for window aggregate {@link ExecNode}. */
public abstract class StreamExecWindowAggregateBase extends StreamExecAggregateBase {

    protected StreamExecWindowAggregateBase(
            int id,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    protected SliceAssigner createSliceAssigner(WindowingStrategy windowingStrategy) {
        WindowSpec windowSpec = windowingStrategy.getWindow();
        if (windowingStrategy instanceof WindowAttachedWindowingStrategy) {
            int windowEndIndex =
                    ((WindowAttachedWindowingStrategy) windowingStrategy).getWindowEnd();
            // we don't need time attribute to assign windows, use a magic value in this case
            SliceAssigner innerAssigner = createSliceAssigner(windowSpec, Integer.MAX_VALUE);
            return SliceAssigners.windowed(windowEndIndex, innerAssigner);

        } else if (windowingStrategy instanceof TimeAttributeWindowingStrategy) {
            final int timeAttributeIndex;
            if (windowingStrategy.isRowtime()) {
                timeAttributeIndex =
                        ((TimeAttributeWindowingStrategy) windowingStrategy)
                                .getTimeAttributeIndex();
            } else {
                timeAttributeIndex = -1;
            }
            return createSliceAssigner(windowSpec, timeAttributeIndex);

        } else {
            throw new UnsupportedOperationException(windowingStrategy + " is not supported yet.");
        }
    }

    protected SliceAssigner createSliceAssigner(WindowSpec windowSpec, int timeAttributeIndex) {
        if (windowSpec instanceof TumblingWindowSpec) {
            Duration size = ((TumblingWindowSpec) windowSpec).getSize();
            return SliceAssigners.tumbling(timeAttributeIndex, size);

        } else if (windowSpec instanceof HoppingWindowSpec) {
            Duration size = ((HoppingWindowSpec) windowSpec).getSize();
            Duration slide = ((HoppingWindowSpec) windowSpec).getSlide();
            if (size.toMillis() % slide.toMillis() != 0) {
                throw new TableException(
                        String.format(
                                "HOP table function based aggregate requires size must be an "
                                        + "integral multiple of slide, but got size %s ms and slide %s ms",
                                size.toMillis(), slide.toMillis()));
            }
            return SliceAssigners.hopping(timeAttributeIndex, size, slide);

        } else if (windowSpec instanceof CumulativeWindowSpec) {
            Duration maxSize = ((CumulativeWindowSpec) windowSpec).getMaxSize();
            Duration step = ((CumulativeWindowSpec) windowSpec).getStep();
            if (maxSize.toMillis() % step.toMillis() != 0) {
                throw new TableException(
                        String.format(
                                "CUMULATE table function based aggregate requires maxSize must be an "
                                        + "integral multiple of step, but got maxSize %s ms and step %s ms",
                                maxSize.toMillis(), step.toMillis()));
            }
            return SliceAssigners.cumulative(timeAttributeIndex, maxSize, step);

        } else {
            throw new UnsupportedOperationException(windowSpec + " is not supported yet.");
        }
    }
}
