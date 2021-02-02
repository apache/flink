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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

/**
 * IntervalJoinSpec describes how two tables will be joined in interval join.
 *
 * <p>This class corresponds to {@link org.apache.calcite.rel.core.Join} rel node. the join
 * condition is splitted into two part: WindowBounds and JoinSpec: 1. WindowBounds contains the time
 * range condition. 2. JoinSpec contains rest of the join condition except windowBounds.
 */
public class IntervalJoinSpec {
    private final WindowBounds windowBounds;
    private final JoinSpec joinSpec;

    public IntervalJoinSpec(JoinSpec joinSpec, WindowBounds windowBounds) {
        this.windowBounds = windowBounds;
        this.joinSpec = joinSpec;
    }

    public WindowBounds getWindowBounds() {
        return windowBounds;
    }

    public JoinSpec getJoinSpec() {
        return joinSpec;
    }

    /** WindowBounds describes the time range condition of a Interval Join. */
    public static class WindowBounds {
        private final boolean isEventTime;
        private final long leftLowerBound;
        private final long leftUpperBound;
        private final int leftTimeIdx;
        private final int rightTimeIdx;

        public WindowBounds(
                boolean isEventTime,
                long leftLowerBound,
                long leftUpperBound,
                int leftTimeIdx,
                int rightTimeIdx) {
            this.isEventTime = isEventTime;
            this.leftLowerBound = leftLowerBound;
            this.leftUpperBound = leftUpperBound;
            this.leftTimeIdx = leftTimeIdx;
            this.rightTimeIdx = rightTimeIdx;
        }

        public boolean isEventTime() {
            return isEventTime;
        }

        public long getLeftLowerBound() {
            return leftLowerBound;
        }

        public long getLeftUpperBound() {
            return leftUpperBound;
        }

        public int getLeftTimeIdx() {
            return leftTimeIdx;
        }

        public int getRightTimeIdx() {
            return rightTimeIdx;
        }
    }
}
