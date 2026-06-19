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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.util.Objects;

/** Base class for TopN Function with sync state api. */
public abstract class AbstractSyncStateTopNFunction extends AbstractTopNFunction {

    private ValueState<Long> rankEndState;

    protected long rankEnd;

    public AbstractSyncStateTopNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            GeneratedRecordComparator generatedSortKeyComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber) {
        super(
                ttlConfig,
                inputRowType,
                generatedSortKeyComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        if (!isConstantRankEnd) {
            ValueStateDescriptor<Long> rankStateDesc =
                    new ValueStateDescriptor<>("rankEnd", Types.LONG);
            if (ttlConfig.isEnabled()) {
                rankStateDesc.enableTimeToLive(ttlConfig);
            }
            rankEndState = getRuntimeContext().getState(rankStateDesc);
        }
    }

    /**
     * Initialize rank end.
     *
     * @param row input record
     * @return rank end
     * @throws Exception
     */
    protected long initRankEnd(RowData row) throws Exception {
        if (isConstantRankEnd) {
            rankEnd = Objects.requireNonNull(constantRankEnd);
            return rankEnd;
        } else {
            Long rankEndValue = rankEndState.value();
            long curRankEnd = rankEndFetcher.apply(row);
            if (rankEndValue == null) {
                rankEnd = curRankEnd;
                rankEndState.update(rankEnd);
                return rankEnd;
            } else {
                rankEnd = rankEndValue;
                if (rankEnd != curRankEnd) {
                    // increment the invalid counter when the current rank end not equal to previous
                    // rank end
                    invalidCounter.inc();
                }
                return rankEnd;
            }
        }
    }

    // ====== utility methods that omit the specified rank end ======

    protected boolean isInRankEnd(long rank) {
        return rank <= rankEnd;
    }

    protected boolean isInRankRange(long rank) {
        return rank <= rankEnd && rank >= rankStart;
    }

    protected void collectInsert(Collector<RowData> out, RowData inputRow, long rank) {
        collectInsert(out, inputRow, rank, rankEnd);
    }

    protected void collectDelete(Collector<RowData> out, RowData inputRow, long rank) {
        collectDelete(out, inputRow, rank, rankEnd);
    }

    protected void collectUpdateAfter(Collector<RowData> out, RowData inputRow, long rank) {
        collectUpdateAfter(out, inputRow, rank, rankEnd);
    }

    protected void collectUpdateBefore(Collector<RowData> out, RowData inputRow, long rank) {
        collectUpdateBefore(out, inputRow, rank, rankEnd);
    }
}
