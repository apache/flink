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

package org.apache.flink.table.runtime.operators.runtimefilter;

import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils.OVER_MAX_ROW_COUNT;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Global runtime filter builder operator. */
public class GlobalRuntimeFilterBuilderOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    /**
     * The maximum number of rows to build the bloom filter. Once the actual number of rows received
     * is greater than this value, we will give up building the bloom filter and directly output an
     * empty filter.
     */
    private final int maxRowCount;

    private transient byte[] serializedGlobalFilter;
    private transient Collector<RowData> collector;
    private transient int globalRowCount;

    public GlobalRuntimeFilterBuilderOperator(int maxRowCount) {
        checkArgument(maxRowCount > 0);
        this.maxRowCount = maxRowCount;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.serializedGlobalFilter = null;
        this.collector = new StreamRecordCollector<>(output);
        this.globalRowCount = 0;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData rowData = element.getValue();
        int localRowCount = rowData.getInt(0);

        if (globalRowCount == OVER_MAX_ROW_COUNT) {
            // Current global filter is already over-max-row-count, do nothing.
        } else if (localRowCount == OVER_MAX_ROW_COUNT
                || globalRowCount + localRowCount > maxRowCount) {
            // The received local filter is over-max-row-count, mark the global filter as
            // over-max-row-count.
            globalRowCount = OVER_MAX_ROW_COUNT;
            serializedGlobalFilter = null;
        } else {
            // merge the local filter
            byte[] serializedLocalFilter = rowData.getBinary(1);
            if (serializedGlobalFilter == null) {
                serializedGlobalFilter = serializedLocalFilter.clone();
            } else {
                BloomFilter.mergeSerializedBloomFilters(
                        serializedGlobalFilter, serializedLocalFilter);
            }
            globalRowCount += localRowCount;
        }
    }

    @Override
    public void endInput() throws Exception {
        collector.collect(
                RuntimeFilterUtils.convertBloomFilterToRowData(
                        globalRowCount, serializedGlobalFilter));
    }
}
