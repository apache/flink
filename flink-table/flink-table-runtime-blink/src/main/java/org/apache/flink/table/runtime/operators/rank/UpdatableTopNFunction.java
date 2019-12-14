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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The function could handle update input stream. It is a fast version of {@link RetractableTopNFunction} which only hold
 * top n data in state, and keep sorted map in heap.
 * However, the function only works in some special scenarios:
 * 1. sort field collation is ascending and its mono is decreasing, or sort field collation is descending and its mono
 * is increasing
 * 2. input data has unique keys
 * 3. input stream could not contain delete record or retract record
 */
public class UpdatableTopNFunction extends AbstractTopNFunction implements CheckpointedFunction {

	private static final long serialVersionUID = 6786508184355952780L;

	private static final Logger LOG = LoggerFactory.getLogger(UpdatableTopNFunction.class);

	private final BaseRowTypeInfo rowKeyType;
	private final long cacheSize;

	// a map state stores mapping from row key to record which is in topN
	// in tuple2, f0 is the record row, f1 is the index in the list of the same sort_key
	// the f1 is used to preserve the record order in the same sort_key
	private transient MapState<BaseRow, Tuple2<BaseRow, Integer>> dataState;

	// a buffer stores mapping from sort key to rowKey list
	private transient TopNBuffer buffer;

	// the kvSortedMap stores mapping from partition key to it's buffer
	private transient Map<BaseRow, TopNBuffer> kvSortedMap;

	// a HashMap stores mapping from rowKey to record, a heap mirror to dataState
	private transient Map<BaseRow, RankRow> rowKeyMap;

	// the kvRowKeyMap store mapping from partitionKey to its rowKeyMap.
	private transient LRUMap<BaseRow, Map<BaseRow, RankRow>> kvRowKeyMap;

	private final TypeSerializer<BaseRow> inputRowSer;
	private final KeySelector<BaseRow, BaseRow> rowKeySelector;

	public UpdatableTopNFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo inputRowType,
			BaseRowKeySelector rowKeySelector,
			GeneratedRecordComparator generatedRecordComparator,
			BaseRowKeySelector sortKeySelector,
			RankType rankType,
			RankRange rankRange,
			boolean generateRetraction,
			boolean outputRankNumber,
			long cacheSize) {
		super(minRetentionTime, maxRetentionTime, inputRowType, generatedRecordComparator, sortKeySelector, rankType,
				rankRange, generateRetraction, outputRankNumber);
		this.rowKeyType = rowKeySelector.getProducedType();
		this.cacheSize = cacheSize;
		this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
		this.rowKeySelector = rowKeySelector;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		int lruCacheSize = Math.max(1, (int) (cacheSize / getDefaultTopNSize()));
		// make sure the cached map is in a fixed size, avoid OOM
		kvSortedMap = new HashMap<>(lruCacheSize);
		kvRowKeyMap = new LRUMap<>(lruCacheSize, new CacheRemovalListener());

		LOG.info("Top{} operator is using LRU caches key-size: {}", getDefaultTopNSize(), lruCacheSize);

		TupleTypeInfo<Tuple2<BaseRow, Integer>> valueTypeInfo = new TupleTypeInfo<>(inputRowType, Types.INT);
		MapStateDescriptor<BaseRow, Tuple2<BaseRow, Integer>> mapStateDescriptor = new MapStateDescriptor<>(
				"data-state-with-update", rowKeyType, valueTypeInfo);
		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		// metrics
		registerMetric(kvSortedMap.size() * getDefaultTopNSize());
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<BaseRow> out) throws Exception {
		if (stateCleaningEnabled) {
			BaseRow partitionKey = (BaseRow) keyContext.getCurrentKey();
			// cleanup cache
			kvRowKeyMap.remove(partitionKey);
			kvSortedMap.remove(partitionKey);
			cleanupState(dataState);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	@Override
	public void processElement(
			BaseRow input, Context context, Collector<BaseRow> out) throws Exception {
		long currentTime = context.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(context, currentTime);

		initHeapStates();
		initRankEnd(input);
		if (outputRankNumber || hasOffset()) {
			// the without-number-algorithm can't handle topN with offset,
			// so use the with-number-algorithm to handle offset
			processElementWithRowNumber(input, out);
		} else {
			processElementWithoutRowNumber(input, out);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Iterator<Map.Entry<BaseRow, Map<BaseRow, RankRow>>> iter = kvRowKeyMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<BaseRow, Map<BaseRow, RankRow>> entry = iter.next();
			BaseRow partitionKey = entry.getKey();
			Map<BaseRow, RankRow> currentRowKeyMap = entry.getValue();
			keyContext.setCurrentKey(partitionKey);
			flushBufferToState(currentRowKeyMap);
		}
	}

	private void initHeapStates() throws Exception {
		requestCount += 1;
		BaseRow partitionKey = (BaseRow) keyContext.getCurrentKey();
		buffer = kvSortedMap.get(partitionKey);
		rowKeyMap = kvRowKeyMap.get(partitionKey);
		if (buffer == null) {
			buffer = new TopNBuffer(sortKeyComparator, LinkedHashSet::new);
			rowKeyMap = new HashMap<>();
			kvSortedMap.put(partitionKey, buffer);
			kvRowKeyMap.put(partitionKey, rowKeyMap);

			// restore sorted map
			Iterator<Map.Entry<BaseRow, Tuple2<BaseRow, Integer>>> iter = dataState.iterator();
			if (iter != null) {
				// a temp map associate sort key to tuple2<index, record>
				Map<BaseRow, TreeMap<Integer, BaseRow>> tempSortedMap = new HashMap<>();
				while (iter.hasNext()) {
					Map.Entry<BaseRow, Tuple2<BaseRow, Integer>> entry = iter.next();
					BaseRow rowKey = entry.getKey();
					Tuple2<BaseRow, Integer> recordAndInnerRank = entry.getValue();
					BaseRow record = recordAndInnerRank.f0;
					Integer innerRank = recordAndInnerRank.f1;
					rowKeyMap.put(rowKey, new RankRow(record, innerRank, false));

					// insert into temp sort map to preserve the record order in the same sort key
					BaseRow sortKey = sortKeySelector.getKey(record);
					TreeMap<Integer, BaseRow> treeMap = tempSortedMap.get(sortKey);
					if (treeMap == null) {
						treeMap = new TreeMap<>();
						tempSortedMap.put(sortKey, treeMap);
					}
					treeMap.put(innerRank, rowKey);
				}

				// build sorted map from the temp map
				Iterator<Map.Entry<BaseRow, TreeMap<Integer, BaseRow>>> tempIter = tempSortedMap.entrySet().iterator();
				while (tempIter.hasNext()) {
					Map.Entry<BaseRow, TreeMap<Integer, BaseRow>> entry = tempIter.next();
					BaseRow sortKey = entry.getKey();
					TreeMap<Integer, BaseRow> treeMap = entry.getValue();
					Iterator<Map.Entry<Integer, BaseRow>> treeMapIter = treeMap.entrySet().iterator();
					while (treeMapIter.hasNext()) {
						Map.Entry<Integer, BaseRow> treeMapEntry = treeMapIter.next();
						Integer innerRank = treeMapEntry.getKey();
						BaseRow recordRowKey = treeMapEntry.getValue();
						int size = buffer.put(sortKey, recordRowKey);
						if (innerRank != size) {
							LOG.warn("Failed to build sorted map from state, this may result in wrong result. " +
											"The sort key is {}, partition key is {}, " +
											"treeMap is {}. The expected inner rank is {}, but current size is {}.",
									sortKey, partitionKey, treeMap, innerRank, size);
						}
					}
				}
			}
		} else {
			hitCount += 1;
		}
	}

	private void processElementWithRowNumber(BaseRow inputRow, Collector<BaseRow> out) throws Exception {
		BaseRow sortKey = sortKeySelector.getKey(inputRow);
		BaseRow rowKey = rowKeySelector.getKey(inputRow);
		if (rowKeyMap.containsKey(rowKey)) {
			// it is an updated record which is in the topN, in this scenario,
			// the new sort key must be higher than old sort key, this is guaranteed by rules
			RankRow oldRow = rowKeyMap.get(rowKey);
			BaseRow oldSortKey = sortKeySelector.getKey(oldRow.row);
			if (oldSortKey.equals(sortKey)) {
				// sort key is not changed, so the rank is the same, only output the row
				Tuple2<Integer, Integer> rankAndInnerRank = rowNumber(sortKey, rowKey, buffer);
				int rank = rankAndInnerRank.f0;
				int innerRank = rankAndInnerRank.f1;
				rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), innerRank, true));
				retract(out, oldRow.row, rank); // retract old record
				collect(out, inputRow, rank);
				return;
			}

			Tuple2<Integer, Integer> oldRankAndInnerRank = rowNumber(oldSortKey, rowKey, buffer);
			int oldRank = oldRankAndInnerRank.f0;
			// remove old sort key
			buffer.remove(oldSortKey, rowKey);
			// add new sort key
			int size = buffer.put(sortKey, rowKey);
			rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
			// update inner rank of records under the old sort key
			updateInnerRank(oldSortKey);

			// emit records
			emitRecordsWithRowNumber(sortKey, inputRow, out, oldSortKey, oldRow, oldRank);
		} else if (checkSortKeyInBufferRange(sortKey, buffer)) {
			// it is an unique record but is in the topN, insert sort key into buffer
			int size = buffer.put(sortKey, rowKey);
			rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));

			// emit records
			emitRecordsWithRowNumber(sortKey, inputRow, out);
		}
	}

	private Tuple2<Integer, Integer> rowNumber(BaseRow sortKey, BaseRow rowKey, TopNBuffer buffer) {
		Iterator<Map.Entry<BaseRow, Collection<BaseRow>>> iterator = buffer.entrySet().iterator();
		int curRank = 1;
		while (iterator.hasNext()) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			BaseRow curKey = entry.getKey();
			Collection<BaseRow> rowKeys = entry.getValue();
			if (curKey.equals(sortKey)) {
				Iterator<BaseRow> rowKeysIter = rowKeys.iterator();
				int innerRank = 1;
				while (rowKeysIter.hasNext()) {
					if (rowKey.equals(rowKeysIter.next())) {
						return Tuple2.of(curRank, innerRank);
					} else {
						innerRank += 1;
						curRank += 1;
					}
				}
			} else {
				curRank += rowKeys.size();
			}
		}
		LOG.error("Failed to find the sortKey: {}, rowkey: {} in the buffer. This should never happen", sortKey,
				rowKey);
		throw new RuntimeException("Failed to find the sortKey, rowkey in the buffer. This should never happen");
	}

	private void emitRecordsWithRowNumber(BaseRow sortKey, BaseRow inputRow, Collector<BaseRow> out) throws Exception {
		emitRecordsWithRowNumber(sortKey, inputRow, out, null, null, -1);
	}

	private void emitRecordsWithRowNumber(BaseRow sortKey, BaseRow inputRow, Collector<BaseRow> out, BaseRow oldSortKey,
			RankRow oldRow, int oldRank) throws Exception {

		int oldInnerRank = oldRow == null ? -1 : oldRow.innerRank;
		Iterator<Map.Entry<BaseRow, Collection<BaseRow>>> iterator = buffer.entrySet().iterator();
		int curRank = 0;
		// whether we have found the sort key in the buffer
		boolean findsSortKey = false;
		while (iterator.hasNext() && isInRankEnd(curRank)) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			BaseRow curSortKey = entry.getKey();
			Collection<BaseRow> rowKeys = entry.getValue();
			// meet its own sort key
			if (!findsSortKey && curSortKey.equals(sortKey)) {
				curRank += rowKeys.size();
				if (oldRow != null) {
					retract(out, oldRow.row, oldRank);
				}
				collect(out, inputRow, curRank);
				findsSortKey = true;
			} else if (findsSortKey) {
				if (oldSortKey == null) {
					// this is a new row, emit updates for all rows in the topn
					Iterator<BaseRow> rowKeyIter = rowKeys.iterator();
					while (rowKeyIter.hasNext() && isInRankEnd(curRank)) {
						curRank += 1;
						BaseRow rowKey = rowKeyIter.next();
						RankRow prevRow = rowKeyMap.get(rowKey);
						retract(out, prevRow.row, curRank - 1);
						collect(out, prevRow.row, curRank);
					}
				} else {
					// current sort key is higher than old sort key,
					// the rank of current record is changed, need to update the following rank
					int compare = sortKeyComparator.compare(curSortKey, oldSortKey);
					if (compare <= 0) {
						Iterator<BaseRow> rowKeyIter = rowKeys.iterator();
						int curInnerRank = 0;
						while (rowKeyIter.hasNext() && isInRankEnd(curRank)) {
							curRank += 1;
							curInnerRank += 1;
							if (compare == 0 && curInnerRank >= oldInnerRank) {
								// match to the previous position
								return;
							}

							BaseRow rowKey = rowKeyIter.next();
							RankRow prevRow = rowKeyMap.get(rowKey);
							retract(out, prevRow.row, curRank - 1);
							collect(out, prevRow.row, curRank);
						}
					} else {
						// current sort key is smaller than old sort key, the rank is not changed, so skip
						return;
					}
				}
			} else {
				curRank += rowKeys.size();
			}
		}

		// remove the records associated to the sort key which is out of topN
		List<BaseRow> toDeleteSortKeys = new ArrayList<>();
		while (iterator.hasNext()) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			Collection<BaseRow> rowKeys = entry.getValue();
			Iterator<BaseRow> rowKeyIter = rowKeys.iterator();
			while (rowKeyIter.hasNext()) {
				BaseRow rowKey = rowKeyIter.next();
				rowKeyMap.remove(rowKey);
				dataState.remove(rowKey);
			}
			toDeleteSortKeys.add(entry.getKey());
		}
		for (BaseRow toDeleteKey : toDeleteSortKeys) {
			buffer.removeAll(toDeleteKey);
		}
	}

	private void processElementWithoutRowNumber(BaseRow inputRow, Collector<BaseRow> out) throws Exception {
		BaseRow sortKey = sortKeySelector.getKey(inputRow);
		BaseRow rowKey = rowKeySelector.getKey(inputRow);
		if (rowKeyMap.containsKey(rowKey)) {
			// it is an updated record which is in the topN, in this scenario,
			// the new sort key must be higher than old sort key, this is guaranteed by rules
			RankRow oldRow = rowKeyMap.get(rowKey);
			BaseRow oldSortKey = sortKeySelector.getKey(oldRow.row);
			if (!oldSortKey.equals(sortKey)) {
				// remove old sort key
				buffer.remove(oldSortKey, rowKey);
				// add new sort key
				int size = buffer.put(sortKey, rowKey);
				rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
				// update inner rank of records under the old sort key
				updateInnerRank(oldSortKey);
			} else {
				// row content may change, so we need to update row in map
				rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), oldRow.innerRank, true));
			}
			// row content may change, so a retract is needed
			retract(out, oldRow.row, oldRow.innerRank);
			collect(out, inputRow);
		} else if (checkSortKeyInBufferRange(sortKey, buffer)) {
			// it is an unique record but is in the topN, insert sort key into buffer
			int size = buffer.put(sortKey, rowKey);
			rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
			collect(out, inputRow);
			// remove retired element
			if (buffer.getCurrentTopNum() > rankEnd) {
				BaseRow lastRowKey = buffer.removeLast();
				if (lastRowKey != null) {
					RankRow lastRow = rowKeyMap.remove(lastRowKey);
					dataState.remove(lastRowKey);
					// always send a retraction message
					delete(out, lastRow.row);
				}
			}
		}
	}

	private void flushBufferToState(Map<BaseRow, RankRow> curRowKeyMap) throws Exception {
		Iterator<Map.Entry<BaseRow, RankRow>> iter = curRowKeyMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<BaseRow, RankRow> entry = iter.next();
			BaseRow key = entry.getKey();
			RankRow rankRow = entry.getValue();
			if (rankRow.dirty) {
				// should update state
				dataState.put(key, Tuple2.of(rankRow.row, rankRow.innerRank));
				rankRow.dirty = false;
			}
		}
	}

	private void updateInnerRank(BaseRow oldSortKey) {
		Collection<BaseRow> list = buffer.get(oldSortKey);
		if (list != null) {
			Iterator<BaseRow> iter = list.iterator();
			int innerRank = 1;
			while (iter.hasNext()) {
				BaseRow rowKey = iter.next();
				RankRow row = rowKeyMap.get(rowKey);
				if (row.innerRank != innerRank) {
					row.innerRank = innerRank;
					row.dirty = true;
				}
				innerRank += 1;
			}
		}
	}

	private class CacheRemovalListener implements LRUMap.RemovalListener<BaseRow, Map<BaseRow, RankRow>> {

		@Override
		public void onRemoval(Map.Entry<BaseRow, Map<BaseRow, RankRow>> eldest) {
			BaseRow previousKey = (BaseRow) keyContext.getCurrentKey();
			BaseRow partitionKey = eldest.getKey();
			Map<BaseRow, RankRow> currentRowKeyMap = eldest.getValue();
			keyContext.setCurrentKey(partitionKey);
			kvSortedMap.remove(partitionKey);
			try {
				flushBufferToState(currentRowKeyMap);
			} catch (Throwable e) {
				LOG.error("Fail to synchronize state!", e);
				throw new RuntimeException(e);
			} finally {
				keyContext.setCurrentKey(previousKey);
			}
		}
	}

	private class RankRow {
		private final BaseRow row;
		private int innerRank;
		private boolean dirty;

		private RankRow(BaseRow row, int innerRank, boolean dirty) {
			this.row = row;
			this.innerRank = innerRank;
			this.dirty = dirty;
		}
	}

}
