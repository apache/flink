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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.BaseRowKeySelector;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
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
 * The function could and only could handle append input stream.
 */
public class AppendOnlyTopNFunction extends AbstractTopNFunction {

	private static final long serialVersionUID = -4708453213104128010L;

	private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyTopNFunction.class);

	private final BaseRowTypeInfo sortKeyType;
	private final TypeSerializer<BaseRow> inputRowSer;
	private final long cacheSize;

	// a map state stores mapping from sort key to records list which is in topN
	private transient MapState<BaseRow, List<BaseRow>> dataState;

	// the buffer stores mapping from sort key to records list, a heap mirror to dataState
	private transient TopNBuffer buffer;

	// the kvSortedMap stores mapping from partition key to it's buffer
	private transient Map<BaseRow, TopNBuffer> kvSortedMap;

	public AppendOnlyTopNFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo inputRowType,
			GeneratedRecordComparator sortKeyGeneratedRecordComparator,
			BaseRowKeySelector sortKeySelector,
			RankType rankType,
			RankRange rankRange,
			boolean generateRetraction,
			boolean outputRankNumber,
			long cacheSize) {
		super(minRetentionTime, maxRetentionTime, inputRowType, sortKeyGeneratedRecordComparator, sortKeySelector,
				rankType, rankRange, generateRetraction, outputRankNumber);
		this.sortKeyType = sortKeySelector.getProducedType();
		this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
		this.cacheSize = cacheSize;
	}

	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		int lruCacheSize = Math.max(1, (int) (cacheSize / getDefaultTopNSize()));
		kvSortedMap = new LRUMap<>(lruCacheSize);
		LOG.info("Top{} operator is using LRU caches key-size: {}", getDefaultTopNSize(), lruCacheSize);

		ListTypeInfo<BaseRow> valueTypeInfo = new ListTypeInfo<>(inputRowType);
		MapStateDescriptor<BaseRow, List<BaseRow>> mapStateDescriptor = new MapStateDescriptor<>(
				"data-state-with-append", sortKeyType, valueTypeInfo);
		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		// metrics
		registerMetric(kvSortedMap.size() * getDefaultTopNSize());
	}

	@Override
	public void processElement(BaseRow input, Context context, Collector<BaseRow> out) throws Exception {
		long currentTime = context.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(context, currentTime);

		initHeapStates();
		initRankEnd(input);

		BaseRow sortKey = sortKeySelector.getKey(input);
		// check whether the sortKey is in the topN range
		if (checkSortKeyInBufferRange(sortKey, buffer)) {
			// insert sort key into buffer
			buffer.put(sortKey, inputRowSer.copy(input));
			Collection<BaseRow> inputs = buffer.get(sortKey);
			// update data state
			dataState.put(sortKey, (List<BaseRow>) inputs);
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
			Collector<BaseRow> out) throws Exception {
		if (stateCleaningEnabled) {
			// cleanup cache
			kvSortedMap.remove(keyContext.getCurrentKey());
			cleanupState(dataState);
		}
	}

	private void initHeapStates() throws Exception {
		requestCount += 1;
		BaseRow currentKey = (BaseRow) keyContext.getCurrentKey();
		buffer = kvSortedMap.get(currentKey);
		if (buffer == null) {
			buffer = new TopNBuffer(sortKeyComparator, ArrayList::new);
			kvSortedMap.put(currentKey, buffer);
			// restore buffer
			Iterator<Map.Entry<BaseRow, List<BaseRow>>> iter = dataState.iterator();
			if (iter != null) {
				while (iter.hasNext()) {
					Map.Entry<BaseRow, List<BaseRow>> entry = iter.next();
					BaseRow sortKey = entry.getKey();
					List<BaseRow> values = entry.getValue();
					// the order is preserved
					buffer.putAll(sortKey, values);
				}
			}
		} else {
			hitCount += 1;
		}
	}

	private void processElementWithRowNumber(BaseRow sortKey, BaseRow input, Collector<BaseRow> out) throws Exception {
		Iterator<Map.Entry<BaseRow, Collection<BaseRow>>> iterator = buffer.entrySet().iterator();
		long curRank = 0L;
		boolean findsSortKey = false;
		while (iterator.hasNext() && isInRankEnd(curRank)) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			Collection<BaseRow> records = entry.getValue();
			// meet its own sort key
			if (!findsSortKey && entry.getKey().equals(sortKey)) {
				curRank += records.size();
				collect(out, input, curRank);
				findsSortKey = true;
			} else if (findsSortKey) {
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

		// remove the records associated to the sort key which is out of topN
		List<BaseRow> toDeleteSortKeys = new ArrayList<>();
		while (iterator.hasNext()) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			BaseRow key = entry.getKey();
			dataState.remove(key);
			toDeleteSortKeys.add(key);
		}
		for (BaseRow toDeleteKey : toDeleteSortKeys) {
			buffer.removeAll(toDeleteKey);
		}
	}

	private void processElementWithoutRowNumber(BaseRow input, Collector<BaseRow> out) throws Exception {
		// remove retired element
		if (buffer.getCurrentTopNum() > rankEnd) {
			Map.Entry<BaseRow, Collection<BaseRow>> lastEntry = buffer.lastEntry();
			BaseRow lastKey = lastEntry.getKey();
			List<BaseRow> lastList = (List<BaseRow>) lastEntry.getValue();
			// remove last one
			BaseRow lastElement = lastList.remove(lastList.size() - 1);
			if (lastList.isEmpty()) {
				buffer.removeAll(lastKey);
				dataState.remove(lastKey);
			} else {
				dataState.put(lastKey, lastList);
			}
			if (input.equals(lastElement)) {
				return;
			} else {
				// lastElement shouldn't be null
				delete(out, lastElement);
			}
		}
		collect(out, input);
	}

}
