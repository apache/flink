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
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.utils.AppendOnlyTopNHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A TopN function could handle insert-only stream.
 *
 * <p>The input stream should only contain INSERT messages.
 */
public class AppendOnlyTopNFunction extends AbstractSyncStateTopNFunction {

    private static final long serialVersionUID = -4708453213104128011L;

    private final InternalTypeInfo<RowData> sortKeyType;
    private final TypeSerializer<RowData> inputRowSer;
    private final long cacheSize;

    // a map state stores mapping from sort key to records list which is in topN
    private transient MapState<RowData, List<RowData>> dataState;

    // the buffer stores mapping from sort key to records list, a heap mirror to dataState
    private transient TopNBuffer buffer;

    private transient SyncStateAppendOnlyTopNHelper helper;

    public AppendOnlyTopNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            GeneratedRecordComparator sortKeyGeneratedRecordComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            long cacheSize) {
        super(
                ttlConfig,
                inputRowType,
                sortKeyGeneratedRecordComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
        this.sortKeyType = sortKeySelector.getProducedType();
        this.inputRowSer = inputRowType.createSerializer(new SerializerConfigImpl());
        this.cacheSize = cacheSize;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        MapStateDescriptor<RowData, List<RowData>> mapStateDescriptor =
                new MapStateDescriptor<>("data-state-with-append", sortKeyType, valueTypeInfo);
        if (ttlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(ttlConfig);
        }
        dataState = getRuntimeContext().getMapState(mapStateDescriptor);

        helper = new SyncStateAppendOnlyTopNHelper();

        helper.registerMetric();
    }

    @Override
    public void processElement(RowData input, Context context, Collector<RowData> out)
            throws Exception {
        initHeapStates();
        initRankEnd(input);

        RowData sortKey = sortKeySelector.getKey(input);
        // check whether the sortKey is in the topN range
        if (checkSortKeyInBufferRange(sortKey, buffer)) {
            // insert sort key into buffer
            buffer.put(sortKey, inputRowSer.copy(input));
            Collection<RowData> inputs = buffer.get(sortKey);
            // update data state
            // copy a new collection to avoid mutating state values, see CopyOnWriteStateMap,
            // otherwise, the result might be corrupt.
            // don't need to perform a deep copy, because RowData elements will not be updated
            dataState.put(sortKey, new ArrayList<>(inputs));
            if (outputRankNumber || hasOffset()) {
                // the without-number-algorithm can't handle topN with offset,
                // so use the with-number-algorithm to handle offset
                helper.processElementWithRowNumber(buffer, sortKey, input, rankEnd, out);
            } else {
                helper.processElementWithoutRowNumber(buffer, input, rankEnd, out);
            }
        }
    }

    private void initHeapStates() throws Exception {
        helper.accRequestCount();
        RowData currentKey = (RowData) keyContext.getCurrentKey();
        buffer = helper.getTopNBufferFromCache(currentKey);
        if (buffer == null) {
            buffer = new TopNBuffer(sortKeyComparator, ArrayList::new);
            helper.saveTopNBufferToCache(currentKey, buffer);
            // restore buffer
            Iterator<Map.Entry<RowData, List<RowData>>> iter = dataState.iterator();
            if (iter != null) {
                while (iter.hasNext()) {
                    Map.Entry<RowData, List<RowData>> entry = iter.next();
                    RowData sortKey = entry.getKey();
                    List<RowData> values = entry.getValue();
                    // the order is preserved
                    buffer.putAll(sortKey, values);
                }
            }
        } else {
            helper.accHitCount();
        }
    }

    private class SyncStateAppendOnlyTopNHelper extends AppendOnlyTopNHelper {

        public SyncStateAppendOnlyTopNHelper() {
            super(
                    AppendOnlyTopNFunction.this,
                    cacheSize,
                    AppendOnlyTopNFunction.this.getDefaultTopNSize());
        }

        @Override
        protected void removeFromState(RowData key) throws Exception {
            dataState.remove(key);
        }

        @Override
        protected void updateState(RowData key, List<RowData> value) throws Exception {
            dataState.put(key, value);
        }
    }
}
