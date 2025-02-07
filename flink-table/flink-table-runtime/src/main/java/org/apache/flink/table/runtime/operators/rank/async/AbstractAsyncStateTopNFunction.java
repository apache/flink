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
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/** Base class for TopN Function with async state api. */
public abstract class AbstractAsyncStateTopNFunction extends AbstractTopNFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncStateTopNFunction.class);

    private ValueState<Long> rankEndState;

    public AbstractAsyncStateTopNFunction(
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

        LOG.info("Top-N is using async state");

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
     */
    protected StateFuture<Long> initRankEnd(RowData row) {
        if (isConstantRankEnd) {
            return StateFutureUtils.completedFuture(Objects.requireNonNull(constantRankEnd));
        } else {
            return rankEndState
                    .asyncValue()
                    .thenApply(
                            rankEndInState -> {
                                long curRankEnd = rankEndFetcher.apply(row);
                                if (rankEndInState == null) {
                                    // no need to wait this future
                                    rankEndState.asyncUpdate(curRankEnd);
                                    return curRankEnd;
                                } else {
                                    if (rankEndInState != curRankEnd) {
                                        // increment the invalid counter when the current rank end
                                        // not equal to previous rank end
                                        invalidCounter.inc();
                                    }
                                    return rankEndInState;
                                }
                            });
        }
    }
}
