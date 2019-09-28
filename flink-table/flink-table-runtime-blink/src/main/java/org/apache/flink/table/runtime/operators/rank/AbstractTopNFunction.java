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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.BaseRowKeySelector;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;

/**
 * Base class for TopN Function.
 */
public abstract class AbstractTopNFunction extends KeyedProcessFunctionWithCleanupState<BaseRow, BaseRow, BaseRow> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractTopNFunction.class);

	private static final String RANK_UNSUPPORTED_MSG = "RANK() on streaming table is not supported currently";

	private static final String DENSE_RANK_UNSUPPORTED_MSG = "DENSE_RANK() on streaming table is not supported currently";

	private static final String WITHOUT_RANK_END_UNSUPPORTED_MSG = "Rank end is not specified. Currently rank only support TopN, which means the rank end must be specified.";

	// we set default topN size to 100
	private static final long DEFAULT_TOPN_SIZE = 100;

	// The util to compare two sortKey equals to each other.
	private GeneratedRecordComparator generatedSortKeyComparator;
	protected Comparator<BaseRow> sortKeyComparator;

	private final boolean generateRetraction;
	protected final boolean outputRankNumber;
	protected final BaseRowTypeInfo inputRowType;
	protected final KeySelector<BaseRow, BaseRow> sortKeySelector;

	protected KeyContext keyContext;
	private final boolean isConstantRankEnd;
	private final long rankStart;
	private final int rankEndIndex;
	protected long rankEnd;
	private transient Function<BaseRow, Long> rankEndFetcher;

	private ValueState<Long> rankEndState;
	private Counter invalidCounter;
	private JoinedRow outputRow;

	// metrics
	protected long hitCount = 0L;
	protected long requestCount = 0L;

	AbstractTopNFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo inputRowType,
			GeneratedRecordComparator generatedSortKeyComparator,
			BaseRowKeySelector sortKeySelector,
			RankType rankType,
			RankRange rankRange,
			boolean generateRetraction,
			boolean outputRankNumber) {
		super(minRetentionTime, maxRetentionTime);
		// TODO support RANK and DENSE_RANK
		switch (rankType) {
			case ROW_NUMBER:
				break;
			case RANK:
				LOG.error(RANK_UNSUPPORTED_MSG);
				throw new UnsupportedOperationException(RANK_UNSUPPORTED_MSG);
			case DENSE_RANK:
				LOG.error(DENSE_RANK_UNSUPPORTED_MSG);
				throw new UnsupportedOperationException(DENSE_RANK_UNSUPPORTED_MSG);
			default:
				LOG.error("Streaming tables do not support {}", rankType.name());
				throw new UnsupportedOperationException("Streaming tables do not support " + rankType.toString());
		}

		if (rankRange instanceof ConstantRankRange) {
			ConstantRankRange constantRankRange = (ConstantRankRange) rankRange;
			isConstantRankEnd = true;
			rankStart = constantRankRange.getRankStart();
			rankEnd = constantRankRange.getRankEnd();
			rankEndIndex = -1;
		} else if (rankRange instanceof VariableRankRange) {
			VariableRankRange variableRankRange = (VariableRankRange) rankRange;
			rankEndIndex = variableRankRange.getRankEndIndex();
			isConstantRankEnd = false;
			rankStart = -1;
			rankEnd = -1;

		} else {
			LOG.error(WITHOUT_RANK_END_UNSUPPORTED_MSG);
			throw new UnsupportedOperationException(WITHOUT_RANK_END_UNSUPPORTED_MSG);
		}
		this.generatedSortKeyComparator = generatedSortKeyComparator;
		this.generateRetraction = generateRetraction;
		this.inputRowType = inputRowType;
		this.outputRankNumber = outputRankNumber;
		this.sortKeySelector = sortKeySelector;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		initCleanupTimeState("RankFunctionCleanupTime");
		outputRow = new JoinedRow();

		if (!isConstantRankEnd) {
			ValueStateDescriptor<Long> rankStateDesc = new ValueStateDescriptor<>("rankEnd", Types.LONG);
			rankEndState = getRuntimeContext().getState(rankStateDesc);
		}
		// compile comparator
		sortKeyComparator = generatedSortKeyComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());
		generatedSortKeyComparator = null;
		invalidCounter = getRuntimeContext().getMetricGroup().counter("topn.invalidTopSize");

		// initialize rankEndFetcher
		if (!isConstantRankEnd) {
			LogicalType rankEndIdxType = inputRowType.getLogicalTypes()[rankEndIndex];
			switch (rankEndIdxType.getTypeRoot()) {
				case BIGINT:
					rankEndFetcher = (BaseRow row) -> row.getLong(rankEndIndex);
					break;
				case INTEGER:
					rankEndFetcher = (BaseRow row) -> (long) row.getInt(rankEndIndex);
					break;
				case SMALLINT:
					rankEndFetcher = (BaseRow row) -> (long) row.getShort(rankEndIndex);
					break;
				default:
					LOG.error("variable rank index column must be long, short or int type, while input type is {}",
							rankEndIdxType.getClass().getName());
					throw new UnsupportedOperationException(
							"variable rank index column must be long type, while input type is " +
									rankEndIdxType.getClass().getName());
			}
		}
	}

	/**
	 * Gets default topN size.
	 *
	 * @return default topN size
	 */
	protected long getDefaultTopNSize() {
		return isConstantRankEnd ? rankEnd : DEFAULT_TOPN_SIZE;
	}

	/**
	 * Initialize rank end.
	 *
	 * @param row input record
	 * @return rank end
	 * @throws Exception
	 */
	protected long initRankEnd(BaseRow row) throws Exception {
		if (isConstantRankEnd) {
			return rankEnd;
		} else {
			Long rankEndValue = rankEndState.value();
			long curRankEnd = rankEndFetcher.apply(row);
			if (rankEndValue == null) {
				rankEnd = curRankEnd;
				rankEndState.update(rankEnd);
				return rankEnd;
			} else {
				rankEnd = rankEndValue;
				if (rankEnd != curRankEnd) {
					// increment the invalid counter when the current rank end not equal to previous rank end
					invalidCounter.inc();
				}
				return rankEnd;
			}
		}
	}

	/**
	 * Checks whether the record should be put into the buffer.
	 *
	 * @param sortKey sortKey to test
	 * @param buffer buffer to add
	 * @return true if the record should be put into the buffer.
	 */
	protected boolean checkSortKeyInBufferRange(BaseRow sortKey, TopNBuffer buffer) {
		Comparator<BaseRow> comparator = buffer.getSortKeyComparator();
		Map.Entry<BaseRow, Collection<BaseRow>> worstEntry = buffer.lastEntry();
		if (worstEntry == null) {
			// return true if the buffer is empty.
			return true;
		} else {
			BaseRow worstKey = worstEntry.getKey();
			int compare = comparator.compare(sortKey, worstKey);
			if (compare < 0) {
				return true;
			} else {
				return buffer.getCurrentTopNum() < getDefaultTopNSize();
			}
		}
	}

	protected void registerMetric(long heapSize) {
		getRuntimeContext().getMetricGroup().<Double, Gauge<Double>>gauge(
				"topn.cache.hitRate",
				() -> requestCount == 0 ? 1.0 : Long.valueOf(hitCount).doubleValue() / requestCount);

		getRuntimeContext().getMetricGroup().<Long, Gauge<Long>>gauge(
				"topn.cache.size", () -> heapSize);
	}

	protected void collect(Collector<BaseRow> out, BaseRow inputRow) {
		BaseRowUtil.setAccumulate(inputRow);
		out.collect(inputRow);
	}

	/**
	 * This is similar to [[retract()]] but always send retraction message regardless of generateRetraction is true or
	 * not.
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

	private BaseRow createOutputRow(BaseRow inputRow, long rank, byte header) {
		if (outputRankNumber) {
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
	 * Sets keyContext to RankFunction.
	 *
	 * @param keyContext keyContext of current function.
	 */
	public void setKeyContext(KeyContext keyContext) {
		this.keyContext = keyContext;
	}

}
