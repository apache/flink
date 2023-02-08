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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.flink.types.RowKind;
import org.apache.flink.types.Row;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.streaming.api.watermark.Watermark;



import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.table.runtime.util.RowDataStringSerializer;

/**
 * Abstract implementation for streaming unbounded Join operator which defines some member fields
 * can be shared between different implementations.
 */
public abstract class AbstractStreamingJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = -376944622236540545L;

    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    private final GeneratedJoinCondition generatedJoinCondition;
    protected final InternalTypeInfo<RowData> leftType;
    protected final InternalTypeInfo<RowData> rightType;

    protected final JoinInputSideSpec leftInputSideSpec;
    protected final JoinInputSideSpec rightInputSideSpec;

    private final boolean[] filterNullKeys;

    protected final long stateRetentionTime;

    protected transient JoinConditionWithNullFilters joinCondition;
    protected transient TimestampedCollector<RowData> collector;

    protected boolean isBatchBackfillEnabled;
    private boolean isStreamMode = true;

    public AbstractStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long stateRetentionTime,
            boolean isBatchBackfillEnabled) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftInputSideSpec = leftInputSideSpec;
        this.rightInputSideSpec = rightInputSideSpec;
        this.stateRetentionTime = stateRetentionTime;
        this.filterNullKeys = filterNullKeys;
        this.isBatchBackfillEnabled = isBatchBackfillEnabled;
        this.isStreamMode = true;
    }

    @Override
    public void open() throws Exception {
        super.open();
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(new Configuration());

        this.collector = new TimestampedCollector<>(output);

        if (!isHybridStreamBatchCapable()) {
            LOG.info("{} not stream batch capable", getPrintableName());
        } else if (isBatchBackfillEnabled) {
            this.isStreamMode = false;
            LOG.info("Initializing batch capable {} in BATCH mode", getPrintableName());
        } else {
            this.isStreamMode = true;
            LOG.info("Initializing batch capable {} in STREAMING mode", getPrintableName());
        }
    }

    protected String getPrintableName() {
        return getRuntimeContext().getJobId() + " " + getRuntimeContext().getTaskName();
    }

    protected abstract boolean isHybridStreamBatchCapable();

    protected boolean isBatchMode() {
        return !isStreamMode;
    }

    protected void setStreamMode(boolean mode) {
        isStreamMode = mode;
    }

    protected void emitStateAndSwitchToStreaming() throws Exception {
        throw new Exception(getPrintableName() + ": programming error does not support batch mode");
    }

    public void processWatermark(Watermark mark) throws Exception {
        if (isBatchMode()) {
            // we are in batch mode, do not re-emit watermark until we flip
            if (mark.getTimestamp() == Watermark.MAX_WATERMARK.getTimestamp()) {
                // We've reached the end of the stream, emit and forward watermark
                emitStateAndSwitchToStreaming();
                super.processWatermark(mark);
            }
        } else {
            // We are in streaming mode, default to standard watermark processing code
            super.processWatermark(mark);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    /**
     * The {@link AssociatedRecords} is the records associated to the input row. It is a wrapper of
     * {@code List<OuterRecord>} which provides two helpful methods {@link #getRecords()} and {@link
     * #getOuterRecords()}. See the method Javadoc for more details.
     */
    protected static final class AssociatedRecords {
        private final static int EXPENSIVE_rowsFetched_THRESHOLD = 1000;
        private final static int MAX_ASSOCIATED_ROWS_CACHE_SIZE = 10000;

        private final List<OuterRecord> records;
        private final int numRecords;

        private JoinCondition condition;
        private JoinRecordStateView otherSideStateView;
        private RowData input;
        private boolean inputIsLeft;

        private AssociatedRecords(List<OuterRecord> records) {
            this.records = records;
            this.numRecords = records.size();
        }

        /***
         *  A 'streaming' implementation of AssociatedRecords, which doesn't store all records in memory. It reduces
         *  the memory overhead at the cost of performance.
         */
        private AssociatedRecords(
                JoinCondition condition,
                JoinRecordStateView otherSideStateView,
                RowData input,
                boolean inputIsLeft,
                int numRecords
                ) {
            this.condition = condition;
            this.otherSideStateView = otherSideStateView;
            this.input = input;
            this.inputIsLeft = inputIsLeft;
            this.numRecords = numRecords;
            this.records = null;
        }

        public boolean isEmpty() {
            return numRecords == 0;
        }

        public int size() {
            return numRecords;
        }

        /**
         * Gets the iterable of records. This is usually be called when the {@link
         * AssociatedRecords} is from inner side.
         */
        public Iterable<RowData> getRecords() throws Exception {
            if (records != null) {
                // use the cached associations list
                return new RecordsIterable(records);
            } else {
                // XXX(sergei): for now we only support inner joins
                // recompute the associations
                final Iterator<RowData> iterator = otherSideStateView.getRecords().iterator();

                return new Iterable<RowData>() {

                    public Iterator<RowData> iterator() {

                        return new Iterator<RowData>() {
                            private RowData nextRecord = null;

                            public boolean hasNext() {
                                if (nextRecord != null) {
                                    return true;
                                }
                                while (iterator.hasNext()) {
                                    RowData record = iterator.next();
                                    boolean matched =
                                        inputIsLeft
                                            ? condition.apply(input, record)
                                            : condition.apply(record, input);
                                    if (matched) {
                                        nextRecord = record;
                                        return true;
                                    }
                                }
                                return false;
                            }

                            public RowData next() {
                                if (hasNext()) {
                                    RowData record = nextRecord;
                                    nextRecord = null;
                                    return record;
                                } else {
                                    throw new java.util.NoSuchElementException();
                                }
                            }
                        };
                    }
                };

            }
        }

        /**
         * Gets the iterable of {@link OuterRecord} which composites record and numOfAssociations.
         * This is usually be called when the {@link AssociatedRecords} is from outer side.
         */
        public Iterable<OuterRecord> getOuterRecords() {
            return records;
        }

        /**
         * Creates an {@link AssociatedRecords} which represents the records associated to the input
         * row.
         */

	public static AssociatedRecords of(
                RowData input,
                boolean inputIsLeft,
                JoinRecordStateView otherSideStateView,
                JoinCondition condition)
                throws Exception {
	    String operator_name = " ";
	    return AssociatedRecords.of(input, inputIsLeft, null, null, operator_name, otherSideStateView, condition);
	}

        public static AssociatedRecords of(
                RowData input,
                boolean inputIsLeft,
        		InternalTypeInfo<RowData> leftType,
	        	InternalTypeInfo<RowData> rightType,
		        String operator_name,
                JoinRecordStateView otherSideStateView,
                JoinCondition condition)
                throws Exception {
            List<OuterRecord> associations = new ArrayList<>();
            int rowsFetched = 0;
            int rowsMatched = 0;
            boolean cacheDisabled = false;

            if (otherSideStateView instanceof OuterJoinRecordStateView) {
                OuterJoinRecordStateView outerStateView =
                        (OuterJoinRecordStateView) otherSideStateView;
                Iterable<Tuple2<RowData, Integer>> records =
                        outerStateView.getRecordsAndNumOfAssociations();
                for (Tuple2<RowData, Integer> record : records) {
                    boolean matched =
                            inputIsLeft
                                    ? condition.apply(input, record.f0)
                                    : condition.apply(record.f0, input);
		            rowsFetched = rowsFetched + 1;

                    if (matched) {
			            rowsMatched = rowsMatched + 1;
                        associations.add(new OuterRecord(record.f0, record.f1));
                    }
                }
            } else {
                Iterable<RowData> records = otherSideStateView.getRecords();
                for (RowData record : records) {
                    boolean matched =
                            inputIsLeft
                                    ? condition.apply(input, record)
                                    : condition.apply(record, input);
		            rowsFetched = rowsFetched + 1;

                    if (matched) {
			            rowsMatched = rowsMatched + 1;
                        // use -1 as the default number of associations
                        if (associations != null) {
                            associations.add(new OuterRecord(record, -1));
                            if (rowsMatched > MAX_ASSOCIATED_ROWS_CACHE_SIZE) {
                                RowDataStringSerializer rowStringSerializer = new RowDataStringSerializer(
                                    inputIsLeft ? leftType : rightType);
                                LOG.info(operator_name + ": EXPENSIVE bypassing associated row cache for: {}",
                                    rowStringSerializer.asString(input));
                                associations = null;
                                cacheDisabled = true;
                            }
                        }
                    }
                }
            }


		    if ((rowsFetched > EXPENSIVE_rowsFetched_THRESHOLD || rowsFetched - rowsMatched > 500)
                 && leftType != null && rightType != null) {
                RowDataStringSerializer rowStringSerializer = new RowDataStringSerializer(inputIsLeft ? leftType : rightType);
		        LOG.info(operator_name + ": EXPENSIVE Inner Join fetched: " + rowsFetched + ", matched " + rowsMatched + " (association cache disabled: " + cacheDisabled + ") for " + (inputIsLeft ? " left input: " : "right input: ") + rowStringSerializer.asString(input));
            }

            if (associations == null) {
                return new AssociatedRecords(condition, otherSideStateView, input, inputIsLeft, rowsMatched);
            } else {
                return new AssociatedRecords(associations);
            }
        }
    }

    /** A lazy Iterable which transform {@code List<OuterReocord>} to {@code Iterable<RowData>}. */
    private static final class RecordsIterable implements IterableIterator<RowData> {
        private final List<OuterRecord> records;
        private int index = 0;

        private RecordsIterable(List<OuterRecord> records) {
            this.records = records;
        }

        @Override
        public Iterator<RowData> iterator() {
            index = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return index < records.size();
        }

        @Override
        public RowData next() {
            RowData row = records.get(index).record;
            index++;
            return row;
        }
    }

    /**
     * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The {@code
     * numOfAssociations} represents the number of associated records in the other side. It is used
     * when the record is from outer side (e.g. left side in LEFT OUTER JOIN). When the {@code
     * numOfAssociations} is ZERO, we need to send a null padding row. This is useful to avoid
     * recompute the associated numbers every time.
     *
     * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the {@code
     * numOfAssociations} will always be {@code -1}.
     */
    protected static final class OuterRecord {
        public final RowData record;
        public final int numOfAssociations;

        private OuterRecord(RowData record, int numOfAssociations) {
            this.record = record;
            this.numOfAssociations = numOfAssociations;
        }
    }
}
