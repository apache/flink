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

package org.apache.flink.table.runtime.operators.rank.async;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyFirstNFunction;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.utils.AppendOnlyFirstNHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A variant of {@link AppendOnlyFirstNFunction} to handle first-n case.
 *
 * <p>The input stream should only contain INSERT messages.
 */
public class AsyncStateAppendOnlyFirstNFunction extends AbstractAsyncStateTopNFunction {

    private static final long serialVersionUID = -889227691088906247L;

    // state stores a counter to record the occurrence of key.
    private ValueState<Integer> state;

    private transient AsyncStateAppendOnlyFirstNHelper helper;

    public AsyncStateAppendOnlyFirstNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            GeneratedRecordComparator sortKeyGeneratedRecordComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber) {
        super(
                ttlConfig,
                inputRowType,
                sortKeyGeneratedRecordComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        ValueStateDescriptor<Integer> stateDesc =
                new ValueStateDescriptor<>("counterState", Types.INT);
        if (ttlConfig.isEnabled()) {
            stateDesc.enableTimeToLive(ttlConfig);
        }
        state = ((StreamingRuntimeContext) getRuntimeContext()).getValueState(stateDesc);

        helper = new AsyncStateAppendOnlyFirstNHelper();
    }

    @Override
    public void processElement(RowData input, Context context, Collector<RowData> out)
            throws Exception {
        StateFuture<Long> rankEndFuture = initRankEnd(input);


        // Ensure the message is an insert-only operation.
        Preconditions.checkArgument(input.getRowKind() == RowKind.INSERT);
        AtomicLong currentRank = new AtomicLong(getCurrentRank());
        // Ignore record if it does not belong to the first-n rows
        rankEndFuture.thenCompose(
                rankEnd -> {
                    if (currentRank.get() >= rankEnd) {
                        return null;
                    }
                    currentRank.set(currentRank.get() + 1);
                    StateFuture<Void> updateFuture = state.asyncUpdate((int) currentRank.get());

                    return updateFuture.thenAccept(
                            VOID -> {
                                helper.sendData(
                                        hasOffset(), out, input, currentRank.get(), rankEnd);
                            });
                });
    }

    private int getCurrentRank() throws IOException {
        Integer value = state.value();
        return value == null ? 0 : value;
    }

    private class AsyncStateAppendOnlyFirstNHelper extends AppendOnlyFirstNHelper {
        public AsyncStateAppendOnlyFirstNHelper() {
            super(AsyncStateAppendOnlyFirstNFunction.this);
        }
    }
}
