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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.assigners.CumulativeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;

/** Utilities for Window Table Function. */
@Internal
public final class WindowTableFunctionUtil {

    /**
     * Creates window assigner based on input window specification.
     *
     * @param windowSpec input window specification
     * @return new created window assigner
     */
    public static WindowAssigner<TimeWindow> createWindowAssigner(WindowSpec windowSpec) {
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
