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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class for Rank Function.
 */
public abstract class AbstractRankFunction extends KeyedProcessFunctionWithCleanupState<BaseRow, BaseRow, BaseRow> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRankFunction.class);

	// we set default topn size to 100
	private static final long DEFAULT_TOPN_SIZE = 100;

	private final RankRange rankRange;
	private final GeneratedRecordEqualiser generatedEqualiser;
	private final boolean generateRetraction;
	protected final boolean isRowNumberAppend;
	protected final RankType rankType;
	protected final BaseRowTypeInfo inputRowType;
	protected final BaseRowTypeInfo outputRowType;
	protected final GeneratedRecordComparator generatedRecordComparator;
	protected final KeySelector<BaseRow, BaseRow> sortKeySelector;

	protected KeyContext keyContext;
	protected boolean isConstantRankEnd;
	protected long rankEnd = -1;
	protected long rankStart = -1;
	protected RecordEqualiser equaliser;
	private int rankEndIndex;
	private ValueState<Long> rankEndState;
	private Counter invalidCounter;
	private JoinedRow outputRow;

	// metrics
	protected long hitCount = 0L;
	protected long requestCount = 0L;

	public AbstractRankFunction(
			long minRetentionTime, long maxRetentionTime, BaseRowTypeInfo inputRowType, BaseRowTypeInfo outputRowType,
			GeneratedRecordComparator generatedRecordComparator, KeySelector<BaseRow, BaseRow> sortKeySelector,
			RankType rankType, RankRange rankRange, GeneratedRecordEqualiser generatedEqualiser, boolean generateRetraction) {
		super(minRetentionTime, maxRetentionTime);
		this.rankRange = rankRange;
		this.generatedEqualiser = generatedEqualiser;
		this.generateRetraction = generateRetraction;
		this.rankType = rankType;
		// TODO support RANK and DENSE_RANK
		switch (rankType) {
			case ROW_NUMBER:
				break;
			case RANK:
				LOG.error("RANK() on streaming table is not supported currently");
				throw new UnsupportedOperationException("RANK() on streaming table is not supported currently");
			case DENSE_RANK:
				LOG.error("DENSE_RANK() on streaming table is not supported currently");
				throw new UnsupportedOperationException("DENSE_RANK() on streaming table is not supported currently");
			default:
				LOG.error("Streaming tables do not support {}", rankType.name());
				throw new UnsupportedOperationException("Streaming tables do not support " + rankType.toString());
		}
		this.inputRowType = inputRowType;
		this.outputRowType = outputRowType;
		this.isRowNumberAppend = inputRowType.getArity() + 1 == outputRowType.getArity();
		this.generatedRecordComparator = generatedRecordComparator;
		this.sortKeySelector = sortKeySelector;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		initCleanupTimeState("RankFunctionCleanupTime");
		outputRow = new JoinedRow();

		// variable rank limit
		if (rankRange instanceof ConstantRankRange) {
			ConstantRankRange constantRankRange = (ConstantRankRange) rankRange;
			isConstantRankEnd = true;
			rankEnd = constantRankRange.getRankEnd();
			rankStart = constantRankRange.getRankStart();
		} else if (rankRange instanceof VariableRankRange) {
			VariableRankRange variableRankRange = (VariableRankRange) rankRange;
			isConstantRankEnd = false;
			rankEndIndex = variableRankRange.getRankEndIndex();
			ValueStateDescriptor<Long> rankStateDesc = new ValueStateDescriptor("rankEnd", Types.LONG);
			rankEndState = getRuntimeContext().getState(rankStateDesc);
		}
		equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
		invalidCounter = getRuntimeContext().getMetricGroup().counter("topn.invalidTopSize");
	}

	protected long getDefaultTopSize() {
		return isConstantRankEnd ? rankEnd : DEFAULT_TOPN_SIZE;
	}

	protected long initRankEnd(BaseRow row) throws Exception {
		if (isConstantRankEnd) {
			return rankEnd;
		} else {
			Long rankEndValue = rankEndState.value();
			long curRankEnd = row.getLong(rankEndIndex);
			if (rankEndValue == null) {
				rankEnd = curRankEnd;
				rankEndState.update(rankEnd);
				return rankEnd;
			} else {
				rankEnd = rankEndValue;
				if (rankEnd != curRankEnd) {
					// increment the invalid counter when the current rank end
					// not equal to previous rank end
					invalidCounter.inc();
				}
				return rankEnd;
			}
		}
	}

	protected <K> Tuple2<Integer, Integer> rowNumber(K sortKey, BaseRow rowKey, SortedMap<K> sortedMap) {
		Iterator<Map.Entry<K, Collection<BaseRow>>> iterator = sortedMap.entrySet().iterator();
		int curRank = 1;
		while (iterator.hasNext()) {
			Map.Entry<K, Collection<BaseRow>> entry = iterator.next();
			K curKey = entry.getKey();
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
		LOG.error("Failed to find the sortKey: {}, rowkey: {} in SortedMap. " +
				"This should never happen", sortKey, rowKey);
		throw new RuntimeException(
				"Failed to find the sortKey, rowkey in SortedMap. This should never happen");
	}

	/**
	 * return true if record should be put into sort map.
	 */
	protected <K> boolean checkSortKeyInBufferRange(K sortKey, SortedMap<K> sortedMap, Comparator<K> sortKeyComparator) {
		Map.Entry<K, Collection<BaseRow>> worstEntry = sortedMap.lastEntry();
		if (worstEntry == null) {
			// sort map is empty
			return true;
		} else {
			K worstKey = worstEntry.getKey();
			int compare = sortKeyComparator.compare(sortKey, worstKey);
			if (compare < 0) {
				return true;
			} else if (sortedMap.getCurrentTopNum() < getMaxSortMapSize()) {
				return true;
			} else {
				return false;
			}
		}
	}

	protected void registerMetric(long heapSize) {
		getRuntimeContext().getMetricGroup().<Double, Gauge<Double>>gauge(
				"topn.cache.hitRate",
				new Gauge<Double>() {

					@Override
					public Double getValue() {
						return requestCount == 0 ? 1.0 :
								Long.valueOf(hitCount).doubleValue() / requestCount;
					}
				});

		getRuntimeContext().getMetricGroup().<Long, Gauge<Long>>gauge(
				"topn.cache.size",
				new Gauge<Long>() {

					@Override
					public Long getValue() {
						return heapSize;
					}
				});
	}

	protected void collect(Collector<BaseRow> out, BaseRow inputRow) {
		BaseRowUtil.setAccumulate(inputRow);
		out.collect(inputRow);
	}

	/**
	 * This is similar to [[retract()]] but always send retraction message regardless of
	 * generateRetraction is true or not.
	 */
	protected void delete(Collector<BaseRow> out, BaseRow inputRow) {
		BaseRowUtil.setRetract(inputRow);
		out.collect(inputRow);
	}

	/**
	 * This is with-row-number version of above delete() method.
	 */
	protected void delete(Collector<BaseRow> out, BaseRow inputRow, long rank) {
		if (isInRankRange(rank)) {
			out.collect(createOutputRow(inputRow, rank, BaseRowUtil.RETRACT_MSG));
		}
	}

	protected void collect(Collector<BaseRow> out, BaseRow inputRow, long rank) {
		if (isInRankRange(rank)) {
			out.collect(createOutputRow(inputRow, rank, BaseRowUtil.ACCUMULATE_MSG));
		}
	}

	protected void retract(Collector<BaseRow> out, BaseRow inputRow, long rank) {
		if (generateRetraction && isInRankRange(rank)) {
			out.collect(createOutputRow(inputRow, rank, BaseRowUtil.RETRACT_MSG));
		}
	}

	protected boolean isInRankEnd(long rank) {
		return rank <= rankEnd;
	}

	protected boolean isInRankRange(long rank) {
		return rank <= rankEnd && rank >= rankStart;
	}

	protected boolean hasOffset() {
		// rank start is 1-based
		return rankStart > 1;
	}

	protected BaseRow createOutputRow(BaseRow inputRow, long rank, byte header) {
		if (isRowNumberAppend) {
			GenericRow rankRow = new GenericRow(1);
			rankRow.setField(0, rank);

			outputRow.replace(inputRow, rankRow);
			outputRow.setHeader(header);
			return outputRow;
		} else {
			inputRow.setHeader(header);
			return inputRow;
		}
	}

	/**
	 * get sorted map size limit
	 * Implementations may vary depending on each rank who has in-memory sort map.
	 * @return
	 */
	protected abstract long getMaxSortMapSize();

	@Override
	public void processElement(
			BaseRow input, Context ctx, Collector<BaseRow> out) throws Exception {

	}

	/**
	 * Set keyContext to RankFunction.
	 *
	 * @param keyContext keyContext of current function.
	 */
	public void setKeyContext(KeyContext keyContext) {
		this.keyContext = keyContext;
	}

}
