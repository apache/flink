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

package org.apache.flink.table.runtime.rank;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.util.LRUMap;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * RankFunction in Append Stream mode.
 */
public class AppendRankFunction extends AbstractRankFunction {

	private static final Logger LOG = LoggerFactory.getLogger(AppendRankFunction.class);

	protected final BaseRowTypeInfo sortKeyType;
	private final TypeSerializer<BaseRow> inputRowSer;
	private final long cacheSize;

	// a map state stores mapping from sort key to records list which is in topN
	private transient MapState<BaseRow, List<BaseRow>> dataState;

	// a sorted map stores mapping from sort key to records list, a heap mirror to dataState
	private transient SortedMap<BaseRow> sortedMap;
	private transient Map<BaseRow, SortedMap<BaseRow>> kvSortedMap;
	private Comparator<BaseRow> sortKeyComparator;

	public AppendRankFunction(
			long minRetentionTime, long maxRetentionTime, BaseRowTypeInfo inputRowType, BaseRowTypeInfo outputRowType,
			BaseRowTypeInfo sortKeyType, GeneratedRecordComparator generatedRecordComparator,
			KeySelector<BaseRow, BaseRow> sortKeySelector, RankType rankType, RankRange rankRange,
			GeneratedRecordEqualiser generatedEqualiser, boolean generateRetraction, long cacheSize) {
		super(minRetentionTime, maxRetentionTime, inputRowType, outputRowType,
			generatedRecordComparator, sortKeySelector, rankType, rankRange, generatedEqualiser, generateRetraction);
		this.sortKeyType = sortKeyType;
		this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
		this.cacheSize = cacheSize;
	}

	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		int lruCacheSize = Math.max(1, (int) (cacheSize / getDefaultTopSize()));
		kvSortedMap = new LRUMap<>(lruCacheSize);
		LOG.info("Top{} operator is using LRU caches key-size: {}", getDefaultTopSize(), lruCacheSize);

		ListTypeInfo<BaseRow> valueTypeInfo = new ListTypeInfo<>(inputRowType);
		MapStateDescriptor<BaseRow, List<BaseRow>> mapStateDescriptor = new MapStateDescriptor(
				"data-state-with-append", sortKeyType, valueTypeInfo);
		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		sortKeyComparator = generatedRecordComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());

		// metrics
		registerMetric(kvSortedMap.size() * getDefaultTopSize());
	}

	@Override
	public void processElement(
			BaseRow input, Context context, Collector<BaseRow> out) throws Exception {
		long currentTime = context.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(context, currentTime);

		initHeapStates();
		initRankEnd(input);

		BaseRow sortKey = sortKeySelector.getKey(input);
		// check whether the sortKey is in the topN range
		if (checkSortKeyInBufferRange(sortKey, sortedMap, sortKeyComparator)) {
			// insert sort key into sortedMap
			sortedMap.put(sortKey, inputRowSer.copy(input));
			Collection<BaseRow> inputs = sortedMap.get(sortKey);
			// update data state
			dataState.put(sortKey, (List<BaseRow>) inputs);
			if (isRowNumberAppend || hasOffset()) {
				// the without-number-algorithm can't handle topn with offset,
				// so use the with-number-algorithm to handle offset
				emitRecordsWithRowNumber(sortKey, input, out);
			} else {
				processElementWithoutRowNumber(input, out);
			}
		}
	}

	private void initHeapStates() throws Exception {
		requestCount += 1;
		BaseRow currentKey = (BaseRow) keyContext.getCurrentKey();
		sortedMap = kvSortedMap.get(currentKey);
		if (sortedMap == null) {
			sortedMap = new SortedMap(sortKeyComparator, new Supplier<Collection<BaseRow>>() {

				@Override
				public Collection<BaseRow> get() {
					return new ArrayList<>();
				}
			});
			kvSortedMap.put(currentKey, sortedMap);
			// restore sorted map
			Iterator<Map.Entry<BaseRow, List<BaseRow>>> iter = dataState.iterator();
			if (iter != null) {
				while (iter.hasNext()) {
					Map.Entry<BaseRow, List<BaseRow>> entry = iter.next();
					BaseRow sortKey = entry.getKey();
					List<BaseRow> values = entry.getValue();
					// the order is preserved
					sortedMap.putAll(sortKey, values);
				}
			}
		} else {
			hitCount += 1;
		}
	}

	private void emitRecordsWithRowNumber(BaseRow sortKey, BaseRow input, Collector<BaseRow> out) throws Exception {
		Iterator<Map.Entry<BaseRow, Collection<BaseRow>>> iterator = sortedMap.entrySet().iterator();
		long curRank = 0L;
		boolean findSortKey = false;
		while (iterator.hasNext() && isInRankEnd(curRank)) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			Collection<BaseRow> records = entry.getValue();
			// meet its own sort key
			if (!findSortKey && entry.getKey().equals(sortKey)) {
				curRank += records.size();
				collect(out, input, curRank);
				findSortKey = true;
			} else if (findSortKey) {
				Iterator<BaseRow> recordsIter = records.iterator();
				while (recordsIter.hasNext() && isInRankEnd(curRank)) {
					curRank += 1;
					BaseRow prevRow = recordsIter.next();
					retract(out, prevRow, curRank - 1);
					collect(out, prevRow, curRank);
				}
			} else {
				curRank += records.size();
			}
		}

		List<BaseRow> toDeleteKeys = new ArrayList<>();
		// remove the records associated to the sort key which is out of topN
		while (iterator.hasNext()) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			BaseRow key = entry.getKey();
			dataState.remove(key);
			toDeleteKeys.add(key);
		}
		for (BaseRow toDeleteKey : toDeleteKeys) {
			sortedMap.removeAll(toDeleteKey);
		}
	}

	private void processElementWithoutRowNumber(BaseRow input, Collector<BaseRow> out) throws Exception {
		// remove retired element
		if (sortedMap.getCurrentTopNum() > rankEnd) {
			Map.Entry<BaseRow, Collection<BaseRow>> lastEntry = sortedMap.lastEntry();
			BaseRow lastKey = lastEntry.getKey();
			List<BaseRow> lastList = (List<BaseRow>) lastEntry.getValue();
			// remove last one
			BaseRow lastElement = lastList.remove(lastList.size() - 1);
			if (lastList.isEmpty()) {
				sortedMap.removeAll(lastKey);
				dataState.remove(lastKey);
			} else {
				dataState.put(lastKey, lastList);
			}
			// lastElement shouldn't be null
			delete(out, lastElement);
		}
		collect(out, input);
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<BaseRow> out) throws Exception {
		if (needToCleanupState(timestamp)) {
			// cleanup cache
			kvSortedMap.remove(keyContext.getCurrentKey());
			cleanupState(dataState);
		}
	}

	@Override
	protected long getMaxSortMapSize() {
		return getDefaultTopSize();
	}

}
