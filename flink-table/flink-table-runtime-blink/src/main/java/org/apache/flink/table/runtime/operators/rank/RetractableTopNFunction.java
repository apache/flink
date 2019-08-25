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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.BaseRowKeySelector;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The function could handle retract stream. Input stream could only contain acc, delete or retract record.
 */
public class RetractableTopNFunction extends AbstractTopNFunction {

	private static final long serialVersionUID = 1365312180599454479L;

	private static final Logger LOG = LoggerFactory.getLogger(RetractableTopNFunction.class);

	// Message to indicate the state is cleared because of ttl restriction. The message could be used to output to log.
	private static final String STATE_CLEARED_WARN_MSG = "The state is cleared because of state ttl. " +
			"This will result in incorrect result. You can increase the state ttl to avoid this.";

	private final BaseRowTypeInfo sortKeyType;

	// flag to skip records with non-exist error instead to fail, true by default.
	private final boolean lenient = true;

	// a map state stores mapping from sort key to records list
	private transient MapState<BaseRow, List<BaseRow>> dataState;

	// a sorted map stores mapping from sort key to records count
	private transient ValueState<SortedMap<BaseRow, Long>> treeMap;

	// The util to compare two BaseRow equals to each other.
	private GeneratedRecordEqualiser generatedEqualiser;
	private RecordEqualiser equaliser;

	private Comparator<BaseRow> serializableComparator;

	public RetractableTopNFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo inputRowType,
			GeneratedRecordComparator generatedRecordComparator,
			BaseRowKeySelector sortKeySelector,
			RankType rankType,
			RankRange rankRange,
			GeneratedRecordEqualiser generatedEqualiser,
			boolean generateRetraction,
			boolean outputRankNumber) {
		super(minRetentionTime, maxRetentionTime, inputRowType, generatedRecordComparator, sortKeySelector, rankType,
				rankRange, generateRetraction, outputRankNumber);
		this.sortKeyType = sortKeySelector.getProducedType();
		this.serializableComparator = new ComparatorWrapper(generatedRecordComparator);
		this.generatedEqualiser = generatedEqualiser;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		// compile equaliser
		equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
		generatedEqualiser = null;

		ListTypeInfo<BaseRow> valueTypeInfo = new ListTypeInfo<>(inputRowType);
		MapStateDescriptor<BaseRow, List<BaseRow>> mapStateDescriptor = new MapStateDescriptor<>(
				"data-state", sortKeyType, valueTypeInfo);
		dataState = getRuntimeContext().getMapState(mapStateDescriptor);

		ValueStateDescriptor<SortedMap<BaseRow, Long>> valueStateDescriptor = new ValueStateDescriptor<>(
				"sorted-map",
				new SortedMapTypeInfo<>(sortKeyType, BasicTypeInfo.LONG_TYPE_INFO, serializableComparator));
		treeMap = getRuntimeContext().getState(valueStateDescriptor);
	}

	@Override
	public void processElement(BaseRow input, Context ctx, Collector<BaseRow> out) throws Exception {
		initRankEnd(input);
		SortedMap<BaseRow, Long> sortedMap = treeMap.value();
		if (sortedMap == null) {
			sortedMap = new TreeMap<>(sortKeyComparator);
		}
		BaseRow sortKey = sortKeySelector.getKey(input);
		if (BaseRowUtil.isAccumulateMsg(input)) {
			// update sortedMap
			if (sortedMap.containsKey(sortKey)) {
				sortedMap.put(sortKey, sortedMap.get(sortKey) + 1);
			} else {
				sortedMap.put(sortKey, 1L);
			}

			// emit
			emitRecordsWithRowNumber(sortedMap, sortKey, input, out);

			// update data state
			List<BaseRow> inputs = dataState.get(sortKey);
			if (inputs == null) {
				// the sort key is never seen
				inputs = new ArrayList<>();
			}
			inputs.add(input);
			dataState.put(sortKey, inputs);
		} else {
			// emit updates first
			retractRecordWithRowNumber(sortedMap, sortKey, input, out);

			// and then update sortedMap
			if (sortedMap.containsKey(sortKey)) {
				long count = sortedMap.get(sortKey) - 1;
				if (count == 0) {
					sortedMap.remove(sortKey);
				} else {
					sortedMap.put(sortKey, count);
				}
			} else {
				if (sortedMap.isEmpty()) {
					if (lenient) {
						LOG.warn(STATE_CLEARED_WARN_MSG);
					} else {
						throw new RuntimeException(STATE_CLEARED_WARN_MSG);
					}
				} else {
					throw new RuntimeException("Can not retract a non-existent record: ${inputBaseRow.toString}. " +
							"This should never happen.");
				}
			}

		}
		treeMap.update(sortedMap);
	}

	// ------------- ROW_NUMBER-------------------------------

	private void retractRecordWithRowNumber(
			SortedMap<BaseRow, Long> sortedMap, BaseRow sortKey, BaseRow inputRow, Collector<BaseRow> out)
			throws Exception {
		Iterator<Map.Entry<BaseRow, Long>> iterator = sortedMap.entrySet().iterator();
		long curRank = 0L;
		boolean findsSortKey = false;
		while (iterator.hasNext() && isInRankEnd(curRank)) {
			Map.Entry<BaseRow, Long> entry = iterator.next();
			BaseRow key = entry.getKey();
			if (!findsSortKey && key.equals(sortKey)) {
				List<BaseRow> inputs = dataState.get(key);
				if (inputs == null) {
					// Skip the data if it's state is cleared because of state ttl.
					if (lenient) {
						LOG.warn(STATE_CLEARED_WARN_MSG);
					} else {
						throw new RuntimeException(STATE_CLEARED_WARN_MSG);
					}
				} else {
					Iterator<BaseRow> inputIter = inputs.iterator();
					while (inputIter.hasNext() && isInRankEnd(curRank)) {
						curRank += 1;
						BaseRow prevRow = inputIter.next();
						if (!findsSortKey && equaliser.equalsWithoutHeader(prevRow, inputRow)) {
							delete(out, prevRow, curRank);
							curRank -= 1;
							findsSortKey = true;
							inputIter.remove();
						} else if (findsSortKey) {
							retract(out, prevRow, curRank + 1);
							collect(out, prevRow, curRank);
						}
					}
					if (inputs.isEmpty()) {
						dataState.remove(key);
					} else {
						dataState.put(key, inputs);
					}
				}
			} else if (findsSortKey) {
				List<BaseRow> inputs = dataState.get(key);
				int i = 0;
				while (i < inputs.size() && isInRankEnd(curRank)) {
					curRank += 1;
					BaseRow prevRow = inputs.get(i);
					retract(out, prevRow, curRank + 1);
					collect(out, prevRow, curRank);
					i++;
				}
			} else {
				curRank += entry.getValue();
			}
		}
	}

	private void emitRecordsWithRowNumber(
			SortedMap<BaseRow, Long> sortedMap, BaseRow sortKey, BaseRow inputRow, Collector<BaseRow> out)
			throws Exception {
		Iterator<Map.Entry<BaseRow, Long>> iterator = sortedMap.entrySet().iterator();
		long curRank = 0L;
		boolean findsSortKey = false;
		while (iterator.hasNext() && isInRankEnd(curRank)) {
			Map.Entry<BaseRow, Long> entry = iterator.next();
			BaseRow key = entry.getKey();
			if (!findsSortKey && key.equals(sortKey)) {
				curRank += entry.getValue();
				collect(out, inputRow, curRank);
				findsSortKey = true;
			} else if (findsSortKey) {
				List<BaseRow> inputs = dataState.get(key);
				if (inputs == null) {
					// Skip the data if it's state is cleared because of state ttl.
					if (lenient) {
						LOG.warn(STATE_CLEARED_WARN_MSG);
					} else {
						throw new RuntimeException(STATE_CLEARED_WARN_MSG);
					}
				} else {
					int i = 0;
					while (i < inputs.size() && isInRankEnd(curRank)) {
						curRank += 1;
						BaseRow prevRow = inputs.get(i);
						retract(out, prevRow, curRank - 1);
						collect(out, prevRow, curRank);
						i++;
					}
				}
			} else {
				curRank += entry.getValue();
			}
		}
	}

	/**
	 * Note: Because it's impossible to restore a RecordComparator instance generated by GeneratedRecordComparator from
	 * snapshot, We introduce ComparatorWrapper class to wrap the GeneratedRecordComparator, a ComparatorWrapper
	 * instance is serializable, and a RecordComparator instance could be restored based on the deserialized
	 * ComparatorWrapper instance.
	 */
	private static class ComparatorWrapper implements Comparator<BaseRow>, Serializable {

		private static final long serialVersionUID = 4386377835781068140L;

		private transient Comparator<BaseRow> comparator;
		private GeneratedRecordComparator generatedRecordComparator;

		private ComparatorWrapper(GeneratedRecordComparator generatedRecordComparator) {
			this.generatedRecordComparator = generatedRecordComparator;
		}

		@Override
		public int compare(BaseRow o1, BaseRow o2) {
			if (comparator == null) {
				comparator = generatedRecordComparator.newInstance(Thread.currentThread().getContextClassLoader());
			}
			return comparator.compare(o1, o2);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ComparatorWrapper) {
				ComparatorWrapper o = (ComparatorWrapper) obj;
				GeneratedRecordComparator oGeneratedComparator = o.generatedRecordComparator;
				return generatedRecordComparator.getClassName().equals(oGeneratedComparator.getClassName()) &&
						generatedRecordComparator.getCode().equals(oGeneratedComparator.getCode()) &&
						Arrays.equals(generatedRecordComparator.getReferences(), oGeneratedComparator.getReferences());
			} else {
				return false;
			}
		}
	}
}
