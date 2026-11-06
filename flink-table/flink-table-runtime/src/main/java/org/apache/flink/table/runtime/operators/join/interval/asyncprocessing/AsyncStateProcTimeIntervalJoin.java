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

package org.apache.flink.table.runtime.operators.join.interval.asyncprocessing;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/** The function to execute processing time interval stream join with async state. */
public final class AsyncStateProcTimeIntervalJoin extends AsyncStateTimeIntervalJoin {

    private static final long serialVersionUID = 1L;

    public AsyncStateProcTimeIntervalJoin(
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
    long getOperatorTime(Context ctx) {
        return ctx.timerService().currentProcessingTime();
    }

    @Override
    long getTimeForLeftStream(Context ctx, RowData row, long operatorTime) {
        return operatorTime;
    }

    @Override
    long getTimeForRightStream(Context ctx, RowData row, long operatorTime) {
        return operatorTime;
    }

    @Override
    void registerTimer(Context ctx, long cleanupTime) {
        ctx.timerService().registerProcessingTimeTimer(cleanupTime);
    }
}
