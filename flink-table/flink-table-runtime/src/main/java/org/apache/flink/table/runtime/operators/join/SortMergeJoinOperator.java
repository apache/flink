/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

/**
 * An implementation that realizes the joining through a sort-merge join strategy. 1.In most cases,
 * its performance is weaker than HashJoin. 2.It is more stable than HashJoin, and most of the data
 * can be sorted stably. 3.SortMergeJoin should be the best choice if sort can be omitted in the
 * case of multi-level join cascade with the same key.
 *
 * <p>NOTE: SEMI and ANTI join output input1 instead of input2. (Contrary to {@link
 * HashJoinOperator}).
 */
public class SortMergeJoinOperator extends TableStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>, BoundedMultiInput {

    private final SortMergeJoinFunction sortMergeJoinFunction;

    public SortMergeJoinOperator(SortMergeJoinFunction sortMergeJoinFunction) {
        this.sortMergeJoinFunction = sortMergeJoinFunction;
    }

    @Override
    public void open() throws Exception {
        super.open();

        // initialize sort merge join function
        this.sortMergeJoinFunction.open(
                false,
                this.getContainingTask(),
                this.getOperatorConfig(),
                new StreamRecordCollector(output),
                this.computeMemorySize(),
                this.getRuntimeContext(),
                this.getMetricGroup());
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        this.sortMergeJoinFunction.processElement1(element.getValue());
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        this.sortMergeJoinFunction.processElement2(element.getValue());
    }

    @Override
    public void endInput(int inputId) throws Exception {
        this.sortMergeJoinFunction.endInput(inputId);
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.sortMergeJoinFunction.close();
    }
}
