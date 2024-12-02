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

package org.apache.flink.table.runtime.operators.rank.asyncprocessing;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/**
 * Base class for TopN Function with async state api.
 *
 * <p>Currently, only constant rank end is supported in async state rank.
 */
public abstract class AbstractAsyncSyncStateTopNFunction extends AbstractTopNFunction {

    public AbstractAsyncSyncStateTopNFunction(
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
        if (!isConstantRankEnd) {
            throw new UnsupportedOperationException(
                    "Variable rank end is not supported in rank with async state api.");
        }
    }
}
