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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.util.LRUMap;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Base class for Update Rank Function.
 */
abstract class AbstractUpdateRankFunction extends AbstractRankFunction
		implements CheckpointedFunction {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractUpdateRankFunction.class);

	private final BaseRowTypeInfo rowKeyType;
	private final long cacheSize;

	// a map state stores mapping from row key to record which is in topN
	// in tuple2, f0 is the record row, f1 is the index in the list of the same sort_key
	// the f1 is used to preserve the record order in the same sort_key
	protected transient MapState<BaseRow, Tuple2<BaseRow, Integer>> dataState;

	// a sorted map stores mapping from sort key to rowkey list
	protected transient SortedMap<BaseRow> sortedMap;

	protected transient Map<BaseRow, SortedMap<BaseRow>> kvSortedMap;

	// a HashMap stores mapping from rowkey to record, a heap mirror to dataState
	protected transient Map<BaseRow, RankRow> rowKeyMap;

	protected transient LRUMap<BaseRow, Map<BaseRow, RankRow>> kvRowKeyMap;

	protected Comparator<BaseRow> sortKeyComparator;

	public AbstractUpdateRankFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo inputRowType,
			BaseRowTypeInfo outputRowType,
			BaseRowTypeInfo rowKeyType,
			GeneratedRecordComparator generatedRecordComparator,
			KeySelector<BaseRow, BaseRow> sortKeySelector,
			RankType rankType,
			RankRange rankRange,
			GeneratedRecordEqualiser generatedEqualiser,
			boolean generateRetraction,
			long cacheSize) {
		super(minRetentionTime,
			maxRetentionTime,
			inputRowType,
			outputRowType,
			generatedRecordComparator,
			sortKeySelector,
			rankType,
			rankRange,
			generatedEqualiser,
			generateRetraction);
		this.rowKeyType = rowKeyType;
		this.cacheSize = cacheSize;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		int lruCacheSize = Math.max(1, (int) (cacheSize / getMaxSortMapSize()));
		// make sure the cached map is in a fixed size, avoid OOM
		kvSortedMap = new HashMap<>(lruCacheSize);
		kvRowKeyMap = new LRUMap<>(lruCacheSize, new CacheRemovalListener());

		LOG.info("Top{} operator is using LRU caches key-size: {}", getMaxSortMapSize(), lruCacheSize);

		TupleTypeInfo<Tuple2<BaseRow, Integer>> valueTypeInfo = new TupleTypeInfo<>(inputRowType, Types.INT);
		MapStateDescriptor<BaseRow, Tuple2<BaseRow, Integer>> mapStateDescriptor = new MapStateDescriptor(
				"data-state-with-update", rowKeyType, valueTypeInfo);
		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		// metrics
		registerMetric(kvSortedMap.size() * getMaxSortMapSize());

		sortKeyComparator = generatedRecordComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<BaseRow> out) throws Exception {
		if (needToCleanupState(timestamp)) {
			BaseRow partitionKey = (BaseRow) keyContext.getCurrentKey();
			// cleanup cache
			kvRowKeyMap.remove(partitionKey);
			kvSortedMap.remove(partitionKey);
			cleanupState(dataState);
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
			synchronizeState(currentRowKeyMap);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	protected void initHeapStates() throws Exception {
		requestCount += 1;
		BaseRow partitionKey = (BaseRow) keyContext.getCurrentKey();
		sortedMap = kvSortedMap.get(partitionKey);
		rowKeyMap = kvRowKeyMap.get(partitionKey);
		if (sortedMap == null) {
			sortedMap = new SortedMap(
					sortKeyComparator,
					new Supplier<Collection<BaseRow>>() {

						@Override
						public Collection<BaseRow> get() {
							return new LinkedHashSet<>();
						}
					});
			rowKeyMap = new HashMap<>();
			kvSortedMap.put(partitionKey, sortedMap);
			kvRowKeyMap.put(partitionKey, rowKeyMap);

			// restore sorted map
			Iterator<Map.Entry<BaseRow, Tuple2<BaseRow, Integer>>> iter = dataState.iterator();
			if (iter != null) {
				// a temp map associate sort key to tuple2<index, record>
				Map<BaseRow, TreeMap<Integer, BaseRow>> tempSortedMap = new HashMap<>();
				while (iter.hasNext()) {
					Map.Entry<BaseRow, Tuple2<BaseRow, Integer>> entry = iter.next();
					BaseRow rowkey = entry.getKey();
					Tuple2<BaseRow, Integer> recordAndInnerRank = entry.getValue();
					BaseRow record = recordAndInnerRank.f0;
					Integer innerRank = recordAndInnerRank.f1;
					rowKeyMap.put(rowkey, new RankRow(record, innerRank, false));

					// insert into temp sort map to preserve the record order in the same sort key
					BaseRow sortKey = sortKeySelector.getKey(record);
					TreeMap<Integer, BaseRow> treeMap = tempSortedMap.get(sortKey);
					if (treeMap == null) {
						treeMap = new TreeMap<>();
						tempSortedMap.put(sortKey, treeMap);
					}
					treeMap.put(innerRank, rowkey);
				}

				// build sorted map from the temp map
				Iterator<Map.Entry<BaseRow, TreeMap<Integer, BaseRow>>> tempIter =
						tempSortedMap.entrySet().iterator();
				while (tempIter.hasNext()) {
					Map.Entry<BaseRow, TreeMap<Integer, BaseRow>> entry = tempIter.next();
					BaseRow sortKey = entry.getKey();
					TreeMap<Integer, BaseRow> treeMap = entry.getValue();
					Iterator<Map.Entry<Integer, BaseRow>> treeMapIter = treeMap.entrySet().iterator();
					while (treeMapIter.hasNext()) {
						Map.Entry<Integer, BaseRow> treeMapEntry = treeMapIter.next();
						Integer innerRank = treeMapEntry.getKey();
						BaseRow recordRowKey = treeMapEntry.getValue();
						int size = sortedMap.put(sortKey, recordRowKey);
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

	private void synchronizeState(Map<BaseRow, RankRow> curRowKeyMap) throws Exception {
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

	class CacheRemovalListener implements LRUMap.RemovalListener<BaseRow, Map<BaseRow, RankRow>> {

		@Override
		public void onRemoval(Map.Entry<BaseRow, Map<BaseRow, RankRow>> eldest) {
			BaseRow previousKey = (BaseRow) keyContext.getCurrentKey();
			BaseRow partitionKey = eldest.getKey();
			Map<BaseRow, RankRow> currentRowKeyMap = eldest.getValue();
			keyContext.setCurrentKey(partitionKey);
			kvSortedMap.remove(partitionKey);
			try {
				synchronizeState(currentRowKeyMap);
			} catch (Throwable e) {
				LOG.error("Fail to synchronize state!");
				throw new RuntimeException(e);
			}
			keyContext.setCurrentKey(previousKey);
		}
	}

	protected class RankRow {
		private final BaseRow row;
		private int innerRank;
		private boolean dirty;

		protected RankRow(BaseRow row, int innerRank, boolean dirty) {
			this.row = row;
			this.innerRank = innerRank;
			this.dirty = dirty;
		}

		protected BaseRow getRow() {
			return row;
		}

		protected int getInnerRank() {
			return innerRank;
		}

		protected boolean isDirty() {
			return dirty;
		}

		public void setInnerRank(int innerRank) {
			this.innerRank = innerRank;
		}

		public void setDirty(boolean dirty) {
			this.dirty = dirty;
		}
	}
}



