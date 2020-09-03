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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.LRUMap;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AppendOnlyTopNFunction extends AbstractTopNFunction {

	private static final long serialVersionUID = -4708453213104128010L;

	private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyTopNFunction.class);

	private final InternalTypeInfo<RowData> sortKeyType;
	private final TypeSerializer<RowData> inputRowSer;
	private final long cacheSize;

	// a map state stores mapping from sort key to records list which is in topN
	private transient MapState<RowData, List<RowData>> dataState;

	// the buffer stores mapping from sort key to records list, a heap mirror to dataState
	private transient TopNBuffer buffer;

	// the kvSortedMap stores mapping from partition key to it's buffer
	private transient Map<RowData, TopNBuffer> kvSortedMap;

	public AppendOnlyTopNFunction(
			long minRetentionTime,
			long maxRetentionTime,
			InternalTypeInfo<RowData> inputRowType,
			GeneratedRecordComparator sortKeyGeneratedRecordComparator,
			RowDataKeySelector sortKeySelector,
			RankType rankType,
			RankRange rankRange,
			boolean generateUpdateBefore,
			boolean outputRankNumber,
			long cacheSize) {
		super(minRetentionTime, maxRetentionTime, inputRowType, sortKeyGeneratedRecordComparator, sortKeySelector,
				rankType, rankRange, generateUpdateBefore, outputRankNumber);
		this.sortKeyType = sortKeySelector.getProducedType();
		this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
		this.cacheSize = cacheSize;
	}

	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		int lruCacheSize = Math.max(1, (int) (cacheSize / getDefaultTopNSize()));
		kvSortedMap = new LRUMap<>(lruCacheSize);
		LOG.info("Top{} operator is using LRU caches key-size: {}", getDefaultTopNSize(), lruCacheSize);

		ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
		MapStateDescriptor<RowData, List<RowData>> mapStateDescriptor = new MapStateDescriptor<>(
				"data-state-with-append", sortKeyType, valueTypeInfo);
		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		// metrics
		registerMetric(kvSortedMap.size() * getDefaultTopNSize());
	}

	@Override
	public void processElement(RowData input, Context context, Collector<RowData> out) throws Exception {
		long currentTime = context.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(context, currentTime);

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
				processElementWithRowNumber(sortKey, input, out);
			} else {
				processElementWithoutRowNumber(input, out);
			}
		}
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<RowData> out) throws Exception {
		if (stateCleaningEnabled) {
			// cleanup cache
			kvSortedMap.remove(keyContext.getCurrentKey());
			cleanupState(dataState);
		}
	}

	private void initHeapStates() throws Exception {
		requestCount += 1;
		RowData currentKey = (RowData) keyContext.getCurrentKey();
		buffer = kvSortedMap.get(currentKey);
		if (buffer == null) {
			buffer = new TopNBuffer(sortKeyComparator, ArrayList::new);
			kvSortedMap.put(currentKey, buffer);
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
			hitCount += 1;
		}
	}

	private void processElementWithRowNumber(RowData sortKey, RowData input, Collector<RowData> out) throws Exception {
		Iterator<Map.Entry<RowData, Collection<RowData>>> iterator = buffer.entrySet().iterator();
		long currentRank = 0L;
		boolean findsSortKey = false;
		RowData currentRow = null;
		while (iterator.hasNext() && isInRankEnd(currentRank)) {
			Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
			Collection<RowData> records = entry.getValue();
			// meet its own sort key
			if (!findsSortKey && entry.getKey().equals(sortKey)) {
				currentRank += records.size();
				currentRow = input;
				findsSortKey = true;
			} else if (findsSortKey) {
				Iterator<RowData> recordsIter = records.iterator();
				while (recordsIter.hasNext() && isInRankEnd(currentRank)) {
					RowData prevRow = recordsIter.next();
					collectUpdateBefore(out, prevRow, currentRank);
					collectUpdateAfter(out, currentRow, currentRank);
					currentRow = prevRow;
					currentRank += 1;
				}
			} else {
				currentRank += records.size();
			}
		}
		if (isInRankEnd(currentRank)) {
			// there is no enough elements in Top-N, emit INSERT message for the new record.
			collectInsert(out, currentRow, currentRank);
		}

		// remove the records associated to the sort key which is out of topN
		List<RowData> toDeleteSortKeys = new ArrayList<>();
		while (iterator.hasNext()) {
			Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
			RowData key = entry.getKey();
			dataState.remove(key);
			toDeleteSortKeys.add(key);
		}
		for (RowData toDeleteKey : toDeleteSortKeys) {
			buffer.removeAll(toDeleteKey);
		}
	}

	private void processElementWithoutRowNumber(RowData input, Collector<RowData> out) throws Exception {
		// remove retired element
		if (buffer.getCurrentTopNum() > rankEnd) {
			Map.Entry<RowData, Collection<RowData>> lastEntry = buffer.lastEntry();
			RowData lastKey = lastEntry.getKey();
			Collection<RowData> lastList = lastEntry.getValue();
			RowData lastElement = buffer.lastElement();
			int size = lastList.size();
			// remove last one
			if (size <= 1) {
				buffer.removeAll(lastKey);
				dataState.remove(lastKey);
			} else {
				buffer.removeLast();
				// last element has been removed from lastList, we have to copy a new collection
				// for lastList to avoid mutating state values, see CopyOnWriteStateMap,
				// otherwise, the result might be corrupt.
				// don't need to perform a deep copy, because RowData elements will not be updated
				dataState.put(lastKey, new ArrayList<>(lastList));
			}
			if (size == 0 || input.equals(lastElement)) {
				return;
			} else {
				// lastElement shouldn't be null
				collectDelete(out, lastElement);
			}
		}
		// it first appears in the TopN, send INSERT message
		collectInsert(out, input);
	}

}
