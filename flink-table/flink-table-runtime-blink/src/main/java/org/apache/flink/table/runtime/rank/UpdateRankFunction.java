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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * A fast version of rank process function which only hold top n data in state,
 * and keep sorted map in heap. This only works in some special scenarios, such
 * as, rank a count(*) stream
 */
public class UpdateRankFunction extends AbstractUpdateRankFunction {

	private final TypeSerializer<BaseRow> inputRowSer;
	private final KeySelector<BaseRow, BaseRow> rowKeySelector;

	public UpdateRankFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo inputRowType,
			BaseRowTypeInfo outputRowType,
			BaseRowTypeInfo rowKeyType,
			KeySelector<BaseRow, BaseRow> rowKeySelector,
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
			rowKeyType,
			generatedRecordComparator,
			sortKeySelector,
			rankType,
			rankRange,
			generatedEqualiser,
			generateRetraction,
			cacheSize);
		this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
		this.rowKeySelector = rowKeySelector;
	}

	@Override
	public void processElement(
			BaseRow input, Context context, Collector<BaseRow> out) throws Exception {
		long currentTime = context.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(context, currentTime);

		initHeapStates();
		initRankEnd(input);
		if (isRowNumberAppend || hasOffset()) {
			// the without-number-algorithm can't handle topn with offset,
			// so use the with-number-algorithm to handle offset
			processElementWithRowNumber(input, out);
		} else {
			processElementWithoutRowNumber(input, out);
		}
	}

	@Override
	protected long getMaxSortMapSize() {
		return getDefaultTopSize();
	}

	private void processElementWithRowNumber(BaseRow inputRow, Collector<BaseRow> out) throws Exception {
		BaseRow sortKey = sortKeySelector.getKey(inputRow);
		BaseRow rowKey = rowKeySelector.getKey(inputRow);
		if (rowKeyMap.containsKey(rowKey)) {
			// it is an updated record which is in the topN, in this scenario,
			// the new sort key must be higher than old sort key, this is guaranteed by rules
			RankRow oldRow = rowKeyMap.get(rowKey);
			BaseRow oldSortKey = sortKeySelector.getKey(oldRow.getRow());
			if (oldSortKey.equals(sortKey)) {
				// sort key is not changed, so the rank is the same, only output the row
				Tuple2<Integer, Integer> rankAndInnerRank = rowNumber(sortKey, rowKey, sortedMap);
				int rank = rankAndInnerRank.f0;
				int innerRank = rankAndInnerRank.f1;
				rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), innerRank, true));
				retract(out, oldRow.getRow(), rank); // retract old record
				collect(out, inputRow, rank);
				return;
			}

			Tuple2<Integer, Integer> oldRankAndInnerRank = rowNumber(oldSortKey, rowKey, sortedMap);
			int oldRank = oldRankAndInnerRank.f0;
			// remove old sort key
			sortedMap.remove(oldSortKey, rowKey);
			// add new sort key
			int size = sortedMap.put(sortKey, rowKey);
			rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
			// update inner rank of records under the old sort key
			updateInnerRank(oldSortKey);

			// emit records
			emitRecordsWithRowNumber(sortKey, inputRow, out, oldSortKey, oldRow, oldRank);
		} else {
			// out of topN
		}
	}

	private void updateInnerRank(BaseRow oldSortKey) {
		Collection<BaseRow> list = sortedMap.get(oldSortKey);
		if (list != null) {
			Iterator<BaseRow> iter = list.iterator();
			int innerRank = 1;
			while (iter.hasNext()) {
				BaseRow rowkey = iter.next();
				RankRow row = rowKeyMap.get(rowkey);
				if (row.getInnerRank() != innerRank) {
					row.setInnerRank(innerRank);
					row.setDirty(true);
				}
				innerRank += 1;
			}
		}
	}

	private void emitRecordsWithRowNumber(
			BaseRow sortKey, BaseRow inputRow, Collector<BaseRow> out, BaseRow oldSortKey, RankRow oldRow, int oldRank) {

		int oldInnerRank = oldRow == null ? -1 : oldRow.getInnerRank();
		Iterator<Map.Entry<BaseRow, Collection<BaseRow>>> iterator = sortedMap.entrySet().iterator();
		int curRank = 0;
		// whether we have found the sort key in the sorted tree
		boolean findSortKey = false;
		while (iterator.hasNext() && isInRankEnd(curRank + 1)) {
			Map.Entry<BaseRow, Collection<BaseRow>> entry = iterator.next();
			BaseRow curKey = entry.getKey();
			Collection<BaseRow> rowKeys = entry.getValue();
			// meet its own sort key
			if (!findSortKey && curKey.equals(sortKey)) {
				curRank += rowKeys.size();
				if (oldRow != null) {
					retract(out, oldRow.getRow(), oldRank);
				}
				collect(out, inputRow, curRank);
				findSortKey = true;
			} else if (findSortKey) {
				if (oldSortKey == null) {
					// this is a new row, emit updates for all rows in the topn
					Iterator<BaseRow> rowKeyIter = rowKeys.iterator();
					while (rowKeyIter.hasNext() && isInRankEnd(curRank + 1)) {
						curRank += 1;
						BaseRow rowKey = rowKeyIter.next();
						RankRow prevRow = rowKeyMap.get(rowKey);
						retract(out, prevRow.getRow(), curRank - 1);
						collect(out, prevRow.getRow(), curRank);
					}
				} else {
					// current sort key is higher than old sort key,
					// the rank of current record is changed, need to update the following rank
					int compare = sortKeyComparator.compare(curKey, oldSortKey);
					if (compare <= 0) {
						Iterator<BaseRow> rowKeyIter = rowKeys.iterator();
						int curInnerRank = 0;
						while (rowKeyIter.hasNext() && isInRankEnd(curRank + 1)) {
							curRank += 1;
							curInnerRank += 1;
							if (compare == 0 && curInnerRank >= oldInnerRank) {
								// match to the previous position
								return;
							}

							BaseRow rowKey = rowKeyIter.next();
							RankRow prevRow = rowKeyMap.get(rowKey);
							retract(out, prevRow.getRow(), curRank - 1);
							collect(out, prevRow.getRow(), curRank);
						}
					} else {
						// current sort key is smaller than old sort key,
						// the rank is not changed, so skip
						return;
					}
				}
			} else {
				curRank += rowKeys.size();
			}
		}
	}

	private void processElementWithoutRowNumber(BaseRow inputRow, Collector<BaseRow> out) throws Exception {
		BaseRow sortKey = sortKeySelector.getKey(inputRow);
		BaseRow rowKey = rowKeySelector.getKey(inputRow);
		if (rowKeyMap.containsKey(rowKey)) {
			// it is an updated record which is in the topN, in this scenario,
			// the new sort key must be higher than old sort key, this is guaranteed by rules
			RankRow oldRow = rowKeyMap.get(rowKey);
			BaseRow oldSortKey = sortKeySelector.getKey(oldRow.getRow());
			if (!oldSortKey.equals(sortKey)) {
				// remove old sort key
				sortedMap.remove(oldSortKey, rowKey);
				// add new sort key
				int size = sortedMap.put(sortKey, rowKey);
				rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
				// update inner rank of records under the old sort key
				updateInnerRank(oldSortKey);
			} else {
				// row content may change, so we need to update row in map
				rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), oldRow.getInnerRank(), true));
			}
			// row content may change, so a retract is needed
			retract(out, oldRow.getRow(), oldRow.getInnerRank());
			collect(out, inputRow);
		} else if (checkSortKeyInBufferRange(sortKey, sortedMap, sortKeyComparator)) {
			// it is an unique record but is in the topN
			// insert sort key into sortedMap
			int size = sortedMap.put(sortKey, rowKey);
			rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
			collect(out, inputRow);
			// remove retired element
			if (sortedMap.getCurrentTopNum() > rankEnd) {
				BaseRow lastRowKey = sortedMap.removeLast();
				if (lastRowKey != null) {
					RankRow lastRow = rowKeyMap.remove(lastRowKey);
					dataState.remove(lastRowKey);
					// always send a retraction message
					delete(out, lastRow.getRow());
				}
			}
		} else {
			// out of topN
		}
	}

}
