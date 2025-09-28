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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.deltajoin.AsyncDeltaJoinRunner;
import org.apache.flink.table.runtime.operators.join.deltajoin.StreamingDeltaJoinOperator;
import org.apache.flink.table.types.logical.RowType;

/** The factory of {@link StreamingDeltaJoinOperator}. */
public class StreamingDeltaJoinOperatorFactory extends AbstractStreamOperatorFactory<RowData>
        implements TwoInputStreamOperatorFactory<RowData, RowData, RowData>,
                YieldingOperatorFactory<RowData> {

    private final AsyncDeltaJoinRunner rightLookupTableAsyncFunction;
    private final AsyncDeltaJoinRunner leftLookupTableAsyncFunction;

    private final RowDataKeySelector leftJoinKeySelector;
    private final RowDataKeySelector rightJoinKeySelector;

    private final long timeout;
    private final int capacity;

    private final long leftSideCacheSize;
    private final long rightSideCacheSize;

    private final RowType leftStreamType;
    private final RowType rightStreamType;

    public StreamingDeltaJoinOperatorFactory(
            AsyncDeltaJoinRunner rightLookupTableAsyncFunction,
            AsyncDeltaJoinRunner leftLookupTableAsyncFunction,
            RowDataKeySelector leftJoinKeySelector,
            RowDataKeySelector rightJoinKeySelector,
            long timeout,
            int capacity,
            long leftSideCacheSize,
            long rightSideCacheSize,
            RowType leftStreamType,
            RowType rightStreamType) {
        this.rightLookupTableAsyncFunction = rightLookupTableAsyncFunction;
        this.leftLookupTableAsyncFunction = leftLookupTableAsyncFunction;
        this.leftJoinKeySelector = leftJoinKeySelector;
        this.rightJoinKeySelector = rightJoinKeySelector;
        this.timeout = timeout;
        this.capacity = capacity;
        this.leftSideCacheSize = leftSideCacheSize;
        this.rightSideCacheSize = rightSideCacheSize;
        this.leftStreamType = leftStreamType;
        this.rightStreamType = rightStreamType;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        MailboxExecutor mailboxExecutor = getMailboxExecutor();
        StreamingDeltaJoinOperator deltaJoinOperator =
                new StreamingDeltaJoinOperator(
                        rightLookupTableAsyncFunction,
                        leftLookupTableAsyncFunction,
                        leftJoinKeySelector,
                        rightJoinKeySelector,
                        timeout,
                        capacity,
                        processingTimeService,
                        mailboxExecutor,
                        leftSideCacheSize,
                        rightSideCacheSize,
                        leftStreamType,
                        rightStreamType);
        deltaJoinOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        return (T) deltaJoinOperator;
    }

    @Override
    public Class<? extends StreamOperator<?>> getStreamOperatorClass(ClassLoader classLoader) {
        return StreamingDeltaJoinOperator.class;
    }
}
