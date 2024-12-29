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

package org.apache.flink.table.runtime.operators.rank.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyFirstNFunction;
import org.apache.flink.table.runtime.operators.rank.asyncprocessing.AsyncStateAppendOnlyFirstNFunction;
import org.apache.flink.util.Collector;

/**
 * A helper to help do the logic 'Top-1' in {@link AppendOnlyFirstNFunction} and {@link
 * AsyncStateAppendOnlyFirstNFunction}.
 */
public class AppendOnlyFirstNHelper extends AbstractTopNFunction.AbstractTopNHelper {
    public AppendOnlyFirstNHelper(AbstractTopNFunction topNFunction) {
        super(topNFunction);
    }

    public void sendData(
            boolean hasOffset, Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
        if (outputRankNumber || hasOffset) {
            collectInsert(out, inputRow, rank, rankEnd);
        } else {
            collectInsert(out, inputRow);
        }
    }
}
