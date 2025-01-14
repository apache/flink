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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;

/**
 * Base class for TopN Function.
 *
 * <p>TODO: move util methods and metrics to AbstractTopNHelper after using AbstractTopNHelper in
 * all TopN functions.
 */
public abstract class AbstractTopNFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTopNFunction.class);

    private static final String RANK_UNSUPPORTED_MSG =
            "RANK() on streaming table is not supported currently";

    private static final String DENSE_RANK_UNSUPPORTED_MSG =
            "DENSE_RANK() on streaming table is not supported currently";

    private static final String WITHOUT_RANK_END_UNSUPPORTED_MSG =
            "Rank end is not specified. Currently rank only support TopN, which means the rank end must be specified.";

    // we set default topN size to 100
    private static final long DEFAULT_TOPN_SIZE = 100;

    protected final StateTtlConfig ttlConfig;

    private final boolean generateUpdateBefore;

    protected final boolean outputRankNumber;

    protected final InternalTypeInfo<RowData> inputRowType;

    protected final KeySelector<RowData, RowData> sortKeySelector;

    protected final boolean isConstantRankEnd;

    protected final long rankStart;

    // constant rank end
    // if rank end is variable, this var is null
    @Nullable protected final Long constantRankEnd;

    // variable rank end index
    private final int rankEndIndex;

    // The util to compare two sortKey equals to each other.
    private GeneratedRecordComparator generatedSortKeyComparator;

    protected Comparator<RowData> sortKeyComparator;

    protected KeyContext keyContext;

    // variable rank end fetcher
    protected transient Function<RowData, Long> rankEndFetcher;

    protected Counter invalidCounter;
    private JoinedRowData outputRow;

    // metrics
    protected long hitCount = 0L;
    protected long requestCount = 0L;

    protected AbstractTopNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            GeneratedRecordComparator generatedSortKeyComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber) {
        this.ttlConfig = ttlConfig;
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
                throw new UnsupportedOperationException(
                        "Streaming tables do not support " + rankType.toString());
        }

        if (rankRange instanceof ConstantRankRange) {
            ConstantRankRange constantRankRange = (ConstantRankRange) rankRange;
            isConstantRankEnd = true;
            rankStart = constantRankRange.getRankStart();
            rankEndIndex = -1;
            constantRankEnd = constantRankRange.getRankEnd();
        } else if (rankRange instanceof VariableRankRange) {
            VariableRankRange variableRankRange = (VariableRankRange) rankRange;
            rankEndIndex = variableRankRange.getRankEndIndex();
            isConstantRankEnd = false;
            rankStart = -1;
            constantRankEnd = null;
        } else {
            LOG.error(WITHOUT_RANK_END_UNSUPPORTED_MSG);
            throw new UnsupportedOperationException(WITHOUT_RANK_END_UNSUPPORTED_MSG);
        }
        this.generatedSortKeyComparator = generatedSortKeyComparator;
        this.generateUpdateBefore = generateUpdateBefore;
        this.inputRowType = inputRowType;
        this.outputRankNumber = outputRankNumber;
        this.sortKeySelector = sortKeySelector;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        outputRow = new JoinedRowData();

        // compile comparator
        sortKeyComparator =
                generatedSortKeyComparator.newInstance(
                        getRuntimeContext().getUserCodeClassLoader());
        generatedSortKeyComparator = null;
        invalidCounter = getRuntimeContext().getMetricGroup().counter("topn.invalidTopSize");

        // initialize rankEndFetcher
        if (!isConstantRankEnd) {
            LogicalType rankEndIdxType = inputRowType.toRowFieldTypes()[rankEndIndex];
            switch (rankEndIdxType.getTypeRoot()) {
                case BIGINT:
                    rankEndFetcher = (RowData row) -> row.getLong(rankEndIndex);
                    break;
                case INTEGER:
                    rankEndFetcher = (RowData row) -> (long) row.getInt(rankEndIndex);
                    break;
                case SMALLINT:
                    rankEndFetcher = (RowData row) -> (long) row.getShort(rankEndIndex);
                    break;
                default:
                    LOG.error(
                            "variable rank index column must be long, short or int type, while input type is {}",
                            rankEndIdxType.getClass().getName());
                    throw new UnsupportedOperationException(
                            "variable rank index column must be long type, while input type is "
                                    + rankEndIdxType.getClass().getName());
            }
        }
    }

    /**
     * Gets default topN size.
     *
     * @return default topN size
     */
    protected long getDefaultTopNSize() {
        return isConstantRankEnd ? Objects.requireNonNull(constantRankEnd) : DEFAULT_TOPN_SIZE;
    }

    /**
     * Checks whether the record should be put into the buffer.
     *
     * @param sortKey sortKey to test
     * @param buffer buffer to add
     * @return true if the record should be put into the buffer.
     */
    protected boolean checkSortKeyInBufferRange(RowData sortKey, TopNBuffer buffer) {
        return buffer.checkSortKeyInBufferRange(sortKey, getDefaultTopNSize());
    }

    protected void registerMetric(long heapSize) {
        registerMetric(heapSize, requestCount, hitCount);
    }

    protected void registerMetric(long heapSize, long requestCount, long hitCount) {
        getRuntimeContext()
                .getMetricGroup()
                .<Double, Gauge<Double>>gauge(
                        "topn.cache.hitRate",
                        () ->
                                requestCount == 0
                                        ? 1.0
                                        : Long.valueOf(hitCount).doubleValue() / requestCount);

        getRuntimeContext()
                .getMetricGroup()
                .<Long, Gauge<Long>>gauge("topn.cache.size", () -> heapSize);
    }

    protected void collectInsert(
            Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
        if (isInRankRange(rank, rankEnd)) {
            out.collect(createOutputRow(inputRow, rank, RowKind.INSERT));
        }
    }

    protected void collectInsert(Collector<RowData> out, RowData inputRow) {
        inputRow.setRowKind(RowKind.INSERT);
        out.collect(inputRow);
    }

    protected void collectDelete(
            Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
        if (isInRankRange(rank, rankEnd)) {
            out.collect(createOutputRow(inputRow, rank, RowKind.DELETE));
        }
    }

    protected void collectDelete(Collector<RowData> out, RowData inputRow) {
        inputRow.setRowKind(RowKind.DELETE);
        out.collect(inputRow);
    }

    protected void collectUpdateAfter(
            Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
        if (isInRankRange(rank, rankEnd)) {
            out.collect(createOutputRow(inputRow, rank, RowKind.UPDATE_AFTER));
        }
    }

    protected void collectUpdateAfter(Collector<RowData> out, RowData inputRow) {
        inputRow.setRowKind(RowKind.UPDATE_AFTER);
        out.collect(inputRow);
    }

    protected void collectUpdateBefore(
            Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
        if (generateUpdateBefore && isInRankRange(rank, rankEnd)) {
            out.collect(createOutputRow(inputRow, rank, RowKind.UPDATE_BEFORE));
        }
    }

    protected void collectUpdateBefore(Collector<RowData> out, RowData inputRow) {
        if (generateUpdateBefore) {
            inputRow.setRowKind(RowKind.UPDATE_BEFORE);
            out.collect(inputRow);
        }
    }

    protected boolean isInRankRange(long rank, long rankEnd) {
        return rank <= rankEnd && rank >= rankStart;
    }

    protected boolean hasOffset() {
        // rank start is 1-based
        return rankStart > 1;
    }

    private RowData createOutputRow(RowData inputRow, long rank, RowKind rowKind) {
        if (outputRankNumber) {
            GenericRowData rankRow = new GenericRowData(1);
            rankRow.setField(0, rank);

            outputRow.replace(inputRow, rankRow);
            outputRow.setRowKind(rowKind);
            return outputRow;
        } else {
            inputRow.setRowKind(rowKind);
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

    /** An abstract helper to do the logic Top-n used for all top-n functions. */
    public abstract static class AbstractTopNHelper {

        protected final AbstractTopNFunction topNFunction;

        protected final StateTtlConfig ttlConfig;

        protected final KeySelector<RowData, RowData> sortKeySelector;

        protected final Comparator<RowData> sortKeyComparator;

        protected final boolean outputRankNumber;

        protected final KeyContext keyContext;

        // metrics used for cache
        private long hitCount = 0L;
        private long requestCount = 0L;

        public AbstractTopNHelper(AbstractTopNFunction topNFunction) {
            this.topNFunction = topNFunction;
            this.ttlConfig = topNFunction.ttlConfig;
            this.sortKeySelector = topNFunction.sortKeySelector;
            this.sortKeyComparator = topNFunction.sortKeyComparator;
            this.outputRankNumber = topNFunction.outputRankNumber;
            this.keyContext = topNFunction.keyContext;
        }

        protected void collectInsert(
                Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
            topNFunction.collectInsert(out, inputRow, rank, rankEnd);
        }

        protected void collectInsert(Collector<RowData> out, RowData inputRow) {
            topNFunction.collectInsert(out, inputRow);
        }

        protected void collectDelete(
                Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
            topNFunction.collectDelete(out, inputRow, rank, rankEnd);
        }

        protected void collectDelete(Collector<RowData> out, RowData inputRow) {
            topNFunction.collectDelete(out, inputRow);
        }

        protected void collectUpdateAfter(
                Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
            topNFunction.collectUpdateAfter(out, inputRow, rank, rankEnd);
        }

        protected void collectUpdateAfter(Collector<RowData> out, RowData inputRow) {
            topNFunction.collectUpdateAfter(out, inputRow);
        }

        protected void collectUpdateBefore(
                Collector<RowData> out, RowData inputRow, long rank, long rankEnd) {
            topNFunction.collectUpdateBefore(out, inputRow, rank, rankEnd);
        }

        protected void collectUpdateBefore(Collector<RowData> out, RowData inputRow) {
            topNFunction.collectUpdateBefore(out, inputRow);
        }

        protected boolean isInRankEnd(long rank, long rankEnd) {
            return rank <= rankEnd;
        }

        public void accRequestCount() {
            requestCount++;
        }

        public void accHitCount() {
            hitCount++;
        }

        protected void registerMetric(long heapSize) {
            topNFunction.registerMetric(heapSize, requestCount, hitCount);
        }
    }
}
