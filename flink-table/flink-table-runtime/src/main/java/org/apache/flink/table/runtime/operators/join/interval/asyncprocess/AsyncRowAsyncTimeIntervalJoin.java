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

package org.apache.flink.table.runtime.operators.join.interval.asyncprocess;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/** The function to execute row(event) time interval stream inner-join. */
public final class AsyncRowAsyncTimeIntervalJoin extends AsyncTimeIntervalJoin {

    private static final long serialVersionUID = 1L;

    private final int leftTimeIdx;
    private final int rightTimeIdx;

    public AsyncRowAsyncTimeIntervalJoin(
            FlinkJoinType joinType,
            long leftLowerBound,
            long leftUpperBound,
            long allowedLateness,
            long minCleanUpInterval,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            IntervalJoinFunction joinFunc,
            int leftTimeIdx,
            int rightTimeIdx) {
        super(
                joinType,
                leftLowerBound,
                leftUpperBound,
                allowedLateness,
                minCleanUpInterval,
                leftType,
                rightType,
                joinFunc);
        this.leftTimeIdx = leftTimeIdx;
        this.rightTimeIdx = rightTimeIdx;
    }

    /**
     * Get the maximum interval between receiving a row and emitting it (as part of a joined
     * result). This is the time interval by which watermarks need to be held back.
     *
     * @return the maximum delay for the outputs
     */
    public long getMaxOutputDelay() {
        return Math.max(leftRelativeSize, rightRelativeSize) + allowedLateness;
    }

    @Override
    public void updateOperatorTime(Context ctx) {
        leftOperatorTime.set(
                ctx.timerService().currentWatermark() > 0
                        ? ctx.timerService().currentWatermark()
                        : 0L);
        // We may set different operator times in the future.
        rightOperatorTime.set(leftOperatorTime.get());
    }

    @Override
    public long getTimeForLeftStream(Context ctx, RowData row) {
        return row.getLong(leftTimeIdx);
    }

    @Override
    public long getTimeForRightStream(Context ctx, RowData row) {
        return row.getLong(rightTimeIdx);
    }

    @Override
    public void registerTimer(Context ctx, long cleanupTime) {
        // Maybe we can register timers for different streams in the future.
        ctx.timerService().registerEventTimeTimer(cleanupTime);
    }
}
