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

package org.apache.flink.table.runtime.operators.join.snapshot;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Stub for the stream operator implementing the {@code LATERAL SNAPSHOT} processing-time temporal
 * table join. The implementation is under separate review (FLINK-39781); this stub only preserves
 * the constructor and interface surface that the planner translation depends on.
 */
@Internal
public class LateralSnapshotJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                Triggerable<RowData, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    public LateralSnapshotJoinOperator(
            boolean isLeftOuterJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            int buildRowtimeIndex,
            GeneratedJoinCondition generatedJoinCondition,
            boolean[] filterNullKeys,
            Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        Preconditions.checkNotNull(leftType);
        Preconditions.checkNotNull(rightType);
        Preconditions.checkNotNull(generatedJoinCondition);
        Preconditions.checkNotNull(filterNullKeys);
        Preconditions.checkNotNull(loadCompletedTime);
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
