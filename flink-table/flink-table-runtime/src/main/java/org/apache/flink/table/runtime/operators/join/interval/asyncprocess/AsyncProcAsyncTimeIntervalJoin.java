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

/** The function to execute processing time interval stream inner-join. */
public final class AsyncProcAsyncTimeIntervalJoin extends AsyncTimeIntervalJoin {

    private static final long serialVersionUID = 1L;

    private long rawLeftOperatorTime = 0;

    public AsyncProcAsyncTimeIntervalJoin(
            FlinkJoinType joinType,
            long leftLowerBound,
            long leftUpperBound,
            long minCleanUpInterval,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            IntervalJoinFunction genJoinFunc) {
        super(
                joinType,
                leftLowerBound,
                leftUpperBound,
                0L,
                minCleanUpInterval,
                leftType,
                rightType,
                genJoinFunc);
    }

    @Override
    public void updateOperatorTime(Context ctx) {
        rawLeftOperatorTime = ctx.timerService().currentProcessingTime();
        leftOperatorTime.set(rawLeftOperatorTime);
        rightOperatorTime.set(rawLeftOperatorTime);
    }

    @Override
    public long getTimeForLeftStream(Context ctx, RowData row) {
        return leftOperatorTime.get();
    }

    @Override
    public long getTimeForRightStream(Context ctx, RowData row) {

        return rightOperatorTime.get();
    }

    @Override
    public void registerTimer(Context ctx, long cleanupTime) {
        ctx.timerService().registerProcessingTimeTimer(cleanupTime);
    }
}
